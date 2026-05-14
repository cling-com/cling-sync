package lib

import "time"

func NewTimestampFromTime(t time.Time) Timestamp {
	return Timestamp{Sec: t.Unix(), Nsec: uint32(t.Nanosecond())} //nolint:gosec
}

func NewTimestampNow() Timestamp {
	return NewTimestampFromTime(time.Now())
}

func (t *Timestamp) Time() time.Time {
	return time.Unix(t.Sec, int64(t.Nsec))
}
