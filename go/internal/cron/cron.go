// Package cron provides a lightweight 5-field cron expression parser and runner.
// Supports: minute hour day-of-month month day-of-week
// Features: *, */N, N-M, N,M,O, named months/days (JAN-DEC, MON-SUN)
package cron

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Schedule represents a parsed cron expression.
type Schedule struct {
	Minutes    []bool // [0..59]
	Hours      []bool // [0..23]
	DaysOfMonth []bool // [1..31] (index 0 unused)
	Months     []bool // [1..12] (index 0 unused)
	DaysOfWeek []bool // [0..6] Sunday=0
}

var monthNames = map[string]int{
	"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
	"JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}

var dayNames = map[string]int{
	"SUN": 0, "MON": 1, "TUE": 2, "WED": 3, "THU": 4, "FRI": 5, "SAT": 6,
}

// Parse parses a 5-field cron expression.
func Parse(expr string) (*Schedule, error) {
	fields := strings.Fields(expr)
	if len(fields) != 5 {
		return nil, fmt.Errorf("cron: expected 5 fields, got %d in %q", len(fields), expr)
	}

	minutes, err := parseField(fields[0], 0, 59, nil)
	if err != nil {
		return nil, fmt.Errorf("cron: minute: %w", err)
	}
	hours, err := parseField(fields[1], 0, 23, nil)
	if err != nil {
		return nil, fmt.Errorf("cron: hour: %w", err)
	}
	dom, err := parseField(fields[2], 1, 31, nil)
	if err != nil {
		return nil, fmt.Errorf("cron: day-of-month: %w", err)
	}
	months, err := parseField(fields[3], 1, 12, monthNames)
	if err != nil {
		return nil, fmt.Errorf("cron: month: %w", err)
	}
	dow, err := parseField(fields[4], 0, 6, dayNames)
	if err != nil {
		return nil, fmt.Errorf("cron: day-of-week: %w", err)
	}

	s := &Schedule{
		Minutes:     make([]bool, 60),
		Hours:       make([]bool, 24),
		DaysOfMonth: make([]bool, 32),
		Months:      make([]bool, 13),
		DaysOfWeek:  make([]bool, 7),
	}
	for _, v := range minutes {
		s.Minutes[v] = true
	}
	for _, v := range hours {
		s.Hours[v] = true
	}
	for _, v := range dom {
		s.DaysOfMonth[v] = true
	}
	for _, v := range months {
		s.Months[v] = true
	}
	for _, v := range dow {
		s.DaysOfWeek[v] = true
	}
	return s, nil
}

// Matches reports whether the schedule matches the given time (truncated to minute).
func (s *Schedule) Matches(t time.Time) bool {
	return s.Minutes[t.Minute()] &&
		s.Hours[t.Hour()] &&
		s.DaysOfMonth[t.Day()] &&
		s.Months[int(t.Month())] &&
		s.DaysOfWeek[int(t.Weekday())]
}

// Next returns the next time after t that matches the schedule.
// Searches up to 2 years ahead; returns zero time if none found.
func (s *Schedule) Next(t time.Time) time.Time {
	// Start from the next minute
	t = t.Truncate(time.Minute).Add(time.Minute)
	limit := t.Add(2 * 365 * 24 * time.Hour)

	for t.Before(limit) {
		if !s.Months[int(t.Month())] {
			// Skip to next month
			t = time.Date(t.Year(), t.Month()+1, 1, 0, 0, 0, 0, t.Location())
			continue
		}
		if !s.DaysOfMonth[t.Day()] || !s.DaysOfWeek[int(t.Weekday())] {
			// Skip to next day
			t = time.Date(t.Year(), t.Month(), t.Day()+1, 0, 0, 0, 0, t.Location())
			continue
		}
		if !s.Hours[t.Hour()] {
			// Skip to next hour
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour()+1, 0, 0, 0, t.Location())
			continue
		}
		if !s.Minutes[t.Minute()] {
			t = t.Add(time.Minute)
			continue
		}
		return t
	}
	return time.Time{}
}

// Runner manages a set of cron jobs.
type Runner struct {
	cancel context.CancelFunc
}

// Job is a single cron job with a parsed schedule and callback.
type Job struct {
	Schedule *Schedule
	Fn       func()
}

// Start launches goroutines for all jobs. Each goroutine sleeps until the next
// matching time, fires the callback, and repeats. Cancel via Stop().
func Start(ctx context.Context, jobs []Job) *Runner {
	ctx, cancel := context.WithCancel(ctx)
	r := &Runner{cancel: cancel}
	for _, job := range jobs {
		go runJob(ctx, job)
	}
	return r
}

// Stop stops all running cron jobs.
func (r *Runner) Stop() {
	if r != nil && r.cancel != nil {
		r.cancel()
	}
}

func runJob(ctx context.Context, job Job) {
	for {
		now := time.Now()
		next := job.Schedule.Next(now)
		if next.IsZero() {
			return
		}
		delay := time.Until(next)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
			job.Fn()
		}
	}
}

// parseField parses a single cron field into a list of integer values.
func parseField(field string, min, max int, names map[string]int) ([]int, error) {
	var result []int
	parts := strings.Split(field, ",")
	for _, part := range parts {
		vals, err := parsePart(strings.TrimSpace(part), min, max, names)
		if err != nil {
			return nil, err
		}
		result = append(result, vals...)
	}
	return result, nil
}

func parsePart(part string, min, max int, names map[string]int) ([]int, error) {
	// Handle */N
	if strings.HasPrefix(part, "*/") {
		stepStr := part[2:]
		step, err := strconv.Atoi(stepStr)
		if err != nil || step <= 0 {
			return nil, fmt.Errorf("invalid step %q", part)
		}
		var vals []int
		for i := min; i <= max; i += step {
			vals = append(vals, i)
		}
		return vals, nil
	}

	// Handle *
	if part == "*" {
		vals := make([]int, 0, max-min+1)
		for i := min; i <= max; i++ {
			vals = append(vals, i)
		}
		return vals, nil
	}

	// Handle N-M or N-M/S
	if strings.Contains(part, "-") {
		rangeParts := strings.SplitN(part, "/", 2)
		step := 1
		if len(rangeParts) == 2 {
			s, err := strconv.Atoi(rangeParts[1])
			if err != nil || s <= 0 {
				return nil, fmt.Errorf("invalid step in range %q", part)
			}
			step = s
		}
		bounds := strings.SplitN(rangeParts[0], "-", 2)
		lo, err := resolveValue(bounds[0], names)
		if err != nil {
			return nil, fmt.Errorf("invalid range start %q: %w", bounds[0], err)
		}
		hi, err := resolveValue(bounds[1], names)
		if err != nil {
			return nil, fmt.Errorf("invalid range end %q: %w", bounds[1], err)
		}
		if lo < min || hi > max || lo > hi {
			return nil, fmt.Errorf("range %d-%d out of bounds [%d,%d]", lo, hi, min, max)
		}
		var vals []int
		for i := lo; i <= hi; i += step {
			vals = append(vals, i)
		}
		return vals, nil
	}

	// Single value
	v, err := resolveValue(part, names)
	if err != nil {
		return nil, err
	}
	if v < min || v > max {
		return nil, fmt.Errorf("value %d out of bounds [%d,%d]", v, min, max)
	}
	return []int{v}, nil
}

func resolveValue(s string, names map[string]int) (int, error) {
	s = strings.TrimSpace(s)
	if v, ok := names[strings.ToUpper(s)]; ok {
		return v, nil
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("invalid value %q", s)
	}
	return v, nil
}
