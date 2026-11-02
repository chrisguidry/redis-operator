package main

import (
	"testing"
	"time"
)

func TestTheNextRunOfASchedule(t *testing.T) {
	at := time.Date(2026, 11, 2, 3, 30, 0, 0, time.UTC)
	cases := []struct {
		schedule string
		want     time.Time
	}{
		{"0 3 * * *", time.Date(2026, 11, 3, 3, 0, 0, 0, time.UTC)},
		{"*/15 * * * *", time.Date(2026, 11, 2, 3, 45, 0, 0, time.UTC)},
		{"0 0 1 * *", time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC)},
		{"30 4,16 * * 1-5", time.Date(2026, 11, 2, 4, 30, 0, 0, time.UTC)},
		{"0 12 * * 0", time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)},
		{"31 3 * * *", time.Date(2026, 11, 2, 3, 31, 0, 0, time.UTC)},
	}
	for _, c := range cases {
		t.Run(c.schedule, func(t *testing.T) {
			schedule, err := parseCron(c.schedule)
			mustSucceed(t, err)

			mustMatch(t, schedule.next(at), c.want)
		})
	}
}

func TestABadScheduleIsRefused(t *testing.T) {
	for _, schedule := range []string{"", "* * * *", "61 * * * *", "* * * * 8", "a * * * *", "*/0 * * * *", "5-2 * * * *"} {
		t.Run(schedule, func(t *testing.T) {
			_, err := parseCron(schedule)

			mustMatch(t, err != nil, true)
		})
	}
}
