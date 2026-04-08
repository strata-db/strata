package t4

import "github.com/sirupsen/logrus"

// Logger is the logging interface used by t4.  Any leveled logger can be
// adapted to satisfy it with a small wrapper; *logrus.Logger already does.
//
// The interface is intentionally narrow: only formatted, leveled methods are
// required.  Structured fields and writer-level operations are not part of
// the contract so that callers are not forced into logrus-specific types.
type Logger interface {
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

// NoopLogger discards all log output.  Useful in tests or when the
// embedding application manages its own logging at a higher layer.
var NoopLogger Logger = noopLogger{}

type noopLogger struct{}

func (noopLogger) Debugf(string, ...interface{}) {}
func (noopLogger) Infof(string, ...interface{})  {}
func (noopLogger) Warnf(string, ...interface{})  {}
func (noopLogger) Errorf(string, ...interface{}) {}

// defaultLogger returns a Logger backed by the global logrus standard logger.
func defaultLogger() Logger { return logrus.StandardLogger() }

// pebbleLogger adapts a Logger to the pebble.Logger interface.
// Pebble's Infof calls (WAL recovery, compaction progress, etc.) are mapped
// to Debugf so they are hidden at the default INFO level, avoiding the
// noisy "[JOB N] WAL file …" lines seen in embedded mode.
type pebbleLogger struct{ log Logger }

func (l *pebbleLogger) Infof(format string, args ...interface{}) {
	l.log.Debugf(format, args...)
}
func (l *pebbleLogger) Fatalf(format string, args ...interface{}) {
	l.log.Errorf(format, args...)
}
