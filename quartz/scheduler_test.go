package quartz

import (
	"net/http"
	"testing"
	"time"
)

func TestScheduler(t *testing.T) {
	sched := NewStdScheduler()
	var jobKeys [4]int

	shellJob := NewShellJob("ls -la")
	shellJob.Description()
	jobKeys[0] = shellJob.Key()

	curlJob, err := NewCurlJob(http.MethodGet, "http://worldclockapi.com/api/json/est/now", "", nil)
	assertEqual(t, err, nil)
	curlJob.Description()
	jobKeys[1] = curlJob.Key()

	errShellJob := NewShellJob("ls -z")
	jobKeys[2] = errShellJob.Key()

	errCurlJob, err := NewCurlJob(http.MethodGet, "http://", "", nil)
	assertEqual(t, err, nil)
	jobKeys[3] = errCurlJob.Key()

	demoLocation, _ := time.LoadLocation("America/Mexico_City")
	sched.Start(demoLocation)
	sched.ScheduleJob(shellJob, NewSimpleTrigger(time.Millisecond*800), demoLocation)
	sched.ScheduleJob(curlJob, NewRunOnceTrigger(time.Millisecond), demoLocation)
	sched.ScheduleJob(errShellJob, NewRunOnceTrigger(time.Millisecond), demoLocation)
	sched.ScheduleJob(errCurlJob, NewSimpleTrigger(time.Millisecond*800), demoLocation)

	time.Sleep(time.Second)
	scheduledJobKeys := sched.GetJobKeys()
	assertEqual(t, scheduledJobKeys, []int{3059422767, 328790344})

	_, err = sched.GetScheduledJob(jobKeys[0])
	if err != nil {
		t.Fail()
	}

	err = sched.DeleteJob(shellJob.Key())
	if err != nil {
		t.Fail()
	}

	scheduledJobKeys = sched.GetJobKeys()
	assertEqual(t, scheduledJobKeys, []int{328790344})
	assertEqual(t, sched.Queue.Len(), 1)

	sched.Clear()
	sched.Stop()
	assertEqual(t, shellJob.JobStatus, OK)
	assertEqual(t, curlJob.JobStatus, OK)
	assertEqual(t, errShellJob.JobStatus, FAILURE)
	assertEqual(t, errCurlJob.JobStatus, FAILURE)
}
