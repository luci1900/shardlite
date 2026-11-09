package blob

import "log/slog"

// ClockRun is how many samples or misses in a row the clock check waits for.
const ClockRun = clockRun

// ClockCheck is the clock check, for tests to feed samples directly.
type ClockCheck = clockCheck

// NewClockCheck returns a check that logs to log.
func NewClockCheck(log *slog.Logger) *ClockCheck { return newClockCheck(log) }

// Observe gives the check a sample or the failure to read one.
func (c *clockCheck) Observe(s ClockSample, err error) { c.observe(s, err) }
