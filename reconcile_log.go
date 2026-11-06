package main

// The operator writes one line on stderr for each operation a person
// caused: a check created, updated, or deleted in Healthchecks, a
// finalizer added or removed, and an error with its source's own text.
// Probes, pings, and steady passes write nothing.

import (
	"fmt"
	"io"
	"sync"
)

// lineLog writes whole lines from the pass, the probe schedule, and the
// heartbeat, which run on different goroutines.
type lineLog struct {
	mutex sync.Mutex
	out   io.Writer
	// faults holds the failure that started for each subject and topic,
	// so a failure that repeats on every pass writes one line. A topic
	// names one kind of work on the subject, such as a ping, so a
	// failure of one does not hide a failure of another.
	faults map[string]map[string]string
}

// The topics of a subject's failures. A reconcile's own failure has
// the empty topic.
const (
	topicReconcile = ""
	topicStatus    = "status"
	// topicOptional is an optional child's write (children.go).
	topicOptional = "optional child"
	// topicPoll is a poll that no instance manager answered
	// (reconcile_poll.go).
	topicPoll = "poll"
)

func newLineLog(out io.Writer) *lineLog {
	return &lineLog{out: out, faults: map[string]map[string]string{}}
}

// printf writes one line.
func (l *lineLog) printf(format string, args ...any) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	_, _ = fmt.Fprintf(l.out, format+"\n", args...)
}

// fault writes "subject: message" when a failure of the subject and
// topic starts. A failure that lasts writes nothing more, whatever the
// text of the errors that follow, because an error's text can change
// on every attempt.
func (l *lineLog) fault(subject, topic, message string) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if _, found := l.faults[subject][topic]; found {
		return
	}
	if l.faults[subject] == nil {
		l.faults[subject] = map[string]string{}
	}
	l.faults[subject][topic] = message
	_, _ = fmt.Fprintf(l.out, "%s: %s\n", subject, message)
}

// clear ends the failure of a subject and topic, and writes one line
// that says it ended.
func (l *lineLog) clear(subject, topic string) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if _, found := l.faults[subject][topic]; !found {
		return
	}
	delete(l.faults[subject], topic)
	what := "recovered"
	if topic != topicReconcile {
		what = topic + " recovered"
	}
	_, _ = fmt.Fprintf(l.out, "%s: %s\n", subject, what)
}

// forget forgets every failure of a subject that is gone.
func (l *lineLog) forget(subject string) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	delete(l.faults, subject)
}
