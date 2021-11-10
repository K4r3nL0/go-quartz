package quartz

import (
	"testing"
	"time"
)

var from_epoch int64 = 1577836800000000000

func TestSimpleTrigger(t *testing.T) {
	trigger := NewSimpleTrigger(time.Second * 5)
	trigger.Description()

	demoLocation, _ := time.LoadLocation("America/Mexico_City")

	next, err := trigger.NextFireTime(from_epoch, demoLocation)
	assertEqualInt64(t, next, 1577836805000000000)
	assertEqual(t, err, nil)

	next, err = trigger.NextFireTime(next, demoLocation)
	assertEqualInt64(t, next, 1577836810000000000)
	assertEqual(t, err, nil)

	next, err = trigger.NextFireTime(next, demoLocation)
	assertEqualInt64(t, next, 1577836815000000000)
	assertEqual(t, err, nil)
}

func TestRunOnceTrigger(t *testing.T) {
	trigger := NewRunOnceTrigger(time.Second * 5)
	trigger.Description()

	demoLocation, _ := time.LoadLocation("America/Mexico_City")

	next, err := trigger.NextFireTime(from_epoch, demoLocation)
	assertEqualInt64(t, next, 1577836805000000000)
	assertEqual(t, err, nil)

	next, err = trigger.NextFireTime(next, demoLocation)
	assertEqualInt64(t, next, 0)
	assertNotEqual(t, err, nil)
}
