package main

// A five-field cron schedule: minute, hour, day of month, month, and
// day of week, in UTC. Each field takes *, a number, a range a-b, a
// list a,b, and a step */n or a-b/n. As in cron, when both the day of
// month and the day of week are restricted, a day that matches either
// one matches.

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type cronSchedule struct {
	fields     [5]map[int]bool
	anyDay     bool
	anyWeekday bool
}

var cronBounds = [5][2]int{{0, 59}, {0, 23}, {1, 31}, {1, 12}, {0, 6}}

func parseCron(text string) (cronSchedule, error) {
	parts := strings.Fields(text)
	if len(parts) != 5 {
		return cronSchedule{}, fmt.Errorf("schedule %q has %d fields; it needs five: minute hour day-of-month month day-of-week", text, len(parts))
	}
	var schedule cronSchedule
	for i, part := range parts {
		values, err := parseCronField(part, cronBounds[i][0], cronBounds[i][1])
		if err != nil {
			return cronSchedule{}, fmt.Errorf("schedule %q: %w", text, err)
		}
		schedule.fields[i] = values
	}
	schedule.anyDay = parts[2] == "*"
	schedule.anyWeekday = parts[4] == "*"
	return schedule, nil
}

func parseCronField(field string, low, high int) (map[int]bool, error) {
	values := map[int]bool{}
	for term := range strings.SplitSeq(field, ",") {
		span, stepText, stepped := strings.Cut(term, "/")
		step := 1
		if stepped {
			n, err := strconv.Atoi(stepText)
			if err != nil || n < 1 {
				return nil, fmt.Errorf("%q has a step that is not a positive number", term)
			}
			step = n
		}
		from, to := low, high
		if span != "*" {
			first, last, ranged := strings.Cut(span, "-")
			a, err := strconv.Atoi(first)
			if err != nil {
				return nil, fmt.Errorf("%q is not a number, a range, or *", term)
			}
			from, to = a, a
			if ranged {
				if to, err = strconv.Atoi(last); err != nil {
					return nil, fmt.Errorf("%q is not a number, a range, or *", term)
				}
			} else if stepped {
				to = high
			}
		}
		if from < low || to > high || from > to {
			return nil, fmt.Errorf("%q is outside %d-%d", term, low, high)
		}
		for v := from; v <= to; v += step {
			values[v] = true
		}
	}
	return values, nil
}

// next is the first minute after at that the schedule matches.
func (s cronSchedule) next(at time.Time) time.Time {
	t := at.UTC().Truncate(time.Minute).Add(time.Minute)
	// A schedule matches within a few years, or never, as for the 31st
	// of February.
	for limit := t.AddDate(5, 0, 0); t.Before(limit); t = t.Add(time.Minute) {
		if !s.fields[3][int(t.Month())] {
			t = time.Date(t.Year(), t.Month()+1, 1, 0, 0, 0, 0, time.UTC).Add(-time.Minute)
			continue
		}
		if !s.dayMatches(t) {
			t = time.Date(t.Year(), t.Month(), t.Day()+1, 0, 0, 0, 0, time.UTC).Add(-time.Minute)
			continue
		}
		if !s.fields[1][t.Hour()] {
			t = t.Truncate(time.Hour).Add(time.Hour - time.Minute)
			continue
		}
		if s.fields[0][t.Minute()] {
			return t
		}
	}
	return time.Time{}
}

func (s cronSchedule) dayMatches(t time.Time) bool {
	day, weekday := s.fields[2][t.Day()], s.fields[4][int(t.Weekday())]
	switch {
	case s.anyDay && s.anyWeekday:
		return true
	case s.anyDay:
		return weekday
	case s.anyWeekday:
		return day
	}
	return day || weekday
}
