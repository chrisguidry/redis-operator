package main

// The series of repeats of one Event. A repeat within the window
// patches the Event the recorder wrote, in place of a new Event, as
// client-go's recorder does.

import (
	"container/list"
	"time"
)

// eventSeriesWindow is how long after the last Event of a series a
// repeat still patches that Event. A repeat after a longer gap is a
// new occurrence, and a new Event shows when it began. client-go's
// recorder uses the same window.
const eventSeriesWindow = 10 * time.Minute

// eventSeriesLimit bounds the series the recorder remembers. With more
// open series than this it forgets the one used longest ago, whose
// next repeat starts a new Event. client-go's recorder uses the same
// limit.
const eventSeriesLimit = 4096

// seriesEntry is the Event a series patches: its name, the count it
// holds, and the time of its last repeat.
type seriesEntry struct {
	name  string
	count int32
	last  time.Time
}

type eventSeries struct {
	order *list.List
	byKey map[string]*list.Element
}

type seriesItem struct {
	key   string
	entry seriesEntry
}

func newEventSeries() *eventSeries {
	return &eventSeries{order: list.New(), byKey: map[string]*list.Element{}}
}

// eventKey is the key of an Event's series: the object, the type, the
// reason, and the message. The UID tells apart two objects of one name,
// one deleted and one created after it.
func eventKey(event kubeEvent) string {
	object := event.InvolvedObject
	return object.Kind + "\x00" + object.Namespace + "\x00" + object.Name + "\x00" + object.UID + "\x00" +
		event.Type + "\x00" + event.Reason + "\x00" + event.Message
}

func (s *eventSeries) get(key string) (seriesEntry, bool) {
	element, ok := s.byKey[key]
	if !ok {
		return seriesEntry{}, false
	}
	s.order.MoveToFront(element)
	return element.Value.(*seriesItem).entry, true
}

func (s *eventSeries) put(key string, entry seriesEntry) {
	if element, ok := s.byKey[key]; ok {
		element.Value.(*seriesItem).entry = entry
		s.order.MoveToFront(element)
		return
	}
	s.byKey[key] = s.order.PushFront(&seriesItem{key: key, entry: entry})
	if s.order.Len() > eventSeriesLimit {
		oldest := s.order.Back()
		s.order.Remove(oldest)
		delete(s.byKey, oldest.Value.(*seriesItem).key)
	}
}

func (s *eventSeries) remove(key string) {
	if element, ok := s.byKey[key]; ok {
		s.order.Remove(element)
		delete(s.byKey, key)
	}
}
