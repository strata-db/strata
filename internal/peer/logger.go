package peer

import "log"

// peerLogger is the minimal leveled logging interface used by Server and Client.
type peerLogger interface {
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

// stdlibPeerLogger is the fallback used when no logger is provided (e.g. in
// package-internal tests).  DEBUG messages are discarded.
type stdlibPeerLogger struct{}

func (stdlibPeerLogger) Debugf(string, ...interface{}) {}
func (stdlibPeerLogger) Infof(format string, args ...interface{}) {
	log.Printf("[INFO]  "+format, args...)
}
func (stdlibPeerLogger) Warnf(format string, args ...interface{}) {
	log.Printf("[WARN]  "+format, args...)
}
func (stdlibPeerLogger) Errorf(format string, args ...interface{}) {
	log.Printf("[ERROR] "+format, args...)
}
