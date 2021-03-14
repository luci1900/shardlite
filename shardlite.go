package shardlite

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

var Debug bool

type Storer interface {
	Upload(kind string, id string, file io.ReadSeeker) error
	Download(kind string, id string) (io.ReadCloser, error)
}

type Locker interface {
	TryLock() error
	TryUnlock() error
}

type LockerMaker interface {
	MakeLock(kind string, id string, url string) Locker
}

type ErrAlreadyActive struct {
	Url string
}

func (e *ErrAlreadyActive) Error() string {
	return fmt.Sprintf("already active: %s", e.Url)
}

type Config struct {
	Name          string
	Url           string
	DbPath        string
	MigrateCb     func(*sql.DB) error
	ActivationTtl time.Duration
	SaveInterval  time.Duration
	Storage       Storer
	Leaser        LockerMaker
}

type Shard struct {
	id           string
	active       bool
	db           *sql.DB
	lastUsed     time.Time
	lock         *sync.Mutex
	deactivateCh chan string
	stopCh       chan bool
	lease        Locker
	config       *Config
}

func newShard(
	id string, deactivateCh chan string, lease Locker,
	config *Config,
) *Shard {
	return &Shard{
		id:           id,
		active:       false,
		db:           nil,
		lastUsed:     time.Now(),
		lock:         &sync.Mutex{},
		deactivateCh: deactivateCh,
		stopCh:       make(chan bool, 1),
		lease:        lease,
		config:       config,
	}
}

func (s *Shard) dbPath() string {
	return path.Join(
		s.config.DbPath,
		s.config.Name,
		fmt.Sprintf("%s.db", s.id),
	)
}

func (s *Shard) connect() (*sql.DB, error) {
	if Debug {
		log.Printf("Db %v", s.dbPath())
	}

	db, err := sql.Open("sqlite3", s.dbPath())
	if err != nil {
		return nil, err
	}
	err = db.Ping()
	if err != nil {
		return nil, err
	} else {
		return db, nil
	}
}

func (s *Shard) load() error {
	if Debug {
		log.Printf("Loading shard %v", s.id)
	}

	in, err := s.config.Storage.Download(s.config.Name, s.id)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		} else {
			return err
		}
	}
	defer in.Close()
	out, err := os.Create(s.dbPath())
	if err != nil {
		return err
	}
	defer out.Close()

	if _, err := io.Copy(out, in); err != nil {
		os.Remove(s.dbPath())
		return err
	}
	return nil
}

func (s *Shard) save() error {
	if Debug {
		log.Printf("Saving shard %v", s.id)
	}

	_, err := s.db.Exec("BEGIN EXCLUSIVE TRANSACTION")
	if err != nil {
		return err
	}
	defer s.db.Exec("END TRANSACTION")

	in, err := os.Open(s.dbPath())
	if err != nil {
		return err
	}
	defer in.Close()

	return s.config.Storage.Upload(s.config.Name, s.id, in)
}

func (s *Shard) Activate() (*sql.DB, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if !s.active {
		if Debug {
			log.Printf("Activating shard %v", s.id)
		}

		if err := s.lease.TryLock(); err != nil {
			return nil, err
		}

		if err := s.load(); err != nil {
			return nil, err
		}
		db, err := s.connect()
		if err != nil {
			return nil, err
		}
		if err := s.config.MigrateCb(db); err != nil {
			return nil, err
		}
		s.db = db
		s.active = true

		go func() {
			for {
				select {
				case <-time.After(s.config.SaveInterval):
					if time.Since(s.lastUsed) >= s.config.ActivationTtl {
						s.Deactivate()
					} else {
						s.Save()
					}
				case <-time.After(s.config.ActivationTtl):
					if time.Since(s.lastUsed) >= s.config.ActivationTtl {
						s.Deactivate()
					}
				case <-s.stopCh:
					return
				}
			}
		}()
	}

	s.lastUsed = time.Now()
	return s.db, nil
}

func (s *Shard) Save() {
	s.lock.Lock()
	defer s.lock.Unlock()

	if !s.active {
		return
	}

	if err := s.save(); err != nil {
		log.Printf("failed to save: %v", err)
	}
}

func (s *Shard) Deactivate() {
	s.lock.Lock()
	defer s.lock.Unlock()

	if !s.active {
		return
	}

	if Debug {
		log.Printf("Deactivating shard %v", s.id)
	}

	if err := s.save(); err != nil {
		log.Print(err)
	} else {
		if err := s.db.Close(); err != nil {
			log.Print(err)
		}
		if err := os.Remove(s.dbPath()); err != nil {
			log.Print(err)
		}
	}

	if err := s.lease.TryUnlock(); err != nil {
		log.Printf("failed to unlock: %v", err)
	}

	s.active = false
	s.db = nil
	s.deactivateCh <- s.id
}

type Pile struct {
	started      bool
	shards       map[string]*Shard
	lock         *sync.Mutex
	stopCh       chan bool
	deactivateCh chan string
	config       *Config
}

func NewPile(config *Config) *Pile {
	return &Pile{
		started:      false,
		shards:       make(map[string]*Shard),
		lock:         &sync.Mutex{},
		stopCh:       make(chan bool, 1),
		deactivateCh: make(chan string),
		config:       config,
	}
}

func (p *Pile) Start() error {
	p.lock.Lock()
	defer p.lock.Unlock()

	if p.started {
		return nil
	}

	dbDir := path.Join(p.config.DbPath, p.config.Name)
	if err := os.MkdirAll(dbDir, os.ModePerm); err != nil {
		return err
	}

	go func() {
		for {
			select {
			case shardId := <-p.deactivateCh:
				p.removeShard(shardId)
			case <-p.stopCh:
				return
			}
		}
	}()

	p.started = true
	return nil
}

func (p *Pile) Stop() {
	p.lock.Lock()
	defer p.lock.Unlock()

	if !p.started {
		return
	}

	p.stopCh <- true
	if Debug {
		log.Println("Shutting down")
	}

	wg := &sync.WaitGroup{}

	for _, shard := range p.shards {
		wg.Add(1)
		go func(shard *Shard) {
			shard.Deactivate()
			wg.Done()
		}(shard)
	}

	wg.Wait()
	p.started = false
}

func (p *Pile) Shard(id string) *Shard {
	p.lock.Lock()
	defer p.lock.Unlock()

	shard, ok := p.shards[id]
	if !ok {
		shard = newShard(
			id, p.deactivateCh,
			p.config.Leaser.MakeLock(p.config.Name, id, p.config.Url),
			p.config,
		)
		p.shards[id] = shard
	}

	return shard
}

func (p *Pile) removeShard(id string) {
	p.lock.Lock()
	defer p.lock.Unlock()

	delete(p.shards, id)
}
