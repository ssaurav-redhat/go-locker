package model

import "time"

type Lock struct {
	Uuid   string
	Expiry int64
}

// IsExpired will check if the lock is expired
func (l *Lock) IsExpired() bool {
	return time.Now().Unix() > l.Expiry
}

//
