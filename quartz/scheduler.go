package quartz

import (
	"container/heap"
	"errors"
	"log"
	"sync"
	"time"
)

// ScheduledJob wraps the scheduled job with the metadata.
type ScheduledJob struct {
	Job                Job
	TriggerDescription string
	NextRunTime        int64
}

// A Scheduler is the Jobs orchestrator.
// Schedulers responsible for executing Jobs when their associated Triggers fire (when their scheduled time arrives).
type Scheduler interface {
	// start the scheduler
	Start(location *time.Location)
	// schedule the job with the specified trigger
	ScheduleJob(job Job, trigger Trigger, location *time.Location) error
	// get keys of all of the scheduled jobs
	GetJobKeys() []int
	// get the scheduled job metadata
	GetScheduledJob(key int) (*ScheduledJob, error)
	// remove the job from the execution queue
	DeleteJob(key int) error
	// clear all the scheduled jobs
	Clear()
	// shutdown the scheduler
	Stop()
}

// StdScheduler implements the quartz.Scheduler interface.
type StdScheduler struct {
	sync.Mutex
	Queue     *PriorityQueue
	interrupt chan interface{}
	exit      chan interface{}
	feeder    chan *Item
}

// NewStdScheduler returns a new StdScheduler.
func NewStdScheduler() *StdScheduler {
	return &StdScheduler{
		Queue:     &PriorityQueue{},
		interrupt: make(chan interface{}),
		exit:      nil,
		feeder:    make(chan *Item)}
}

// ScheduleJob uses the specified Trigger to schedule the Job.
func (sched *StdScheduler) ScheduleJob(job Job, trigger Trigger, location *time.Location) error {
	nextRunTime, err := trigger.NextFireTime(NowNano(location), location)

	if err == nil {
		sched.feeder <- &Item{
			job,
			trigger,
			nextRunTime,
			0}
		return nil
	}

	return err
}

// Start starts the StdScheduler execution loop.
func (sched *StdScheduler) Start(location *time.Location) {
	// reset the exit channel
	sched.exit = make(chan interface{})
	// start the feed reader
	go sched.startFeedReader()
	// start scheduler execution loop
	go sched.startExecutionLoop(location)
}

// GetJobKeys returns the keys of all of the scheduled jobs.
func (sched *StdScheduler) GetJobKeys() []int {
	sched.Lock()
	defer sched.Unlock()

	keys := make([]int, 0, sched.Queue.Len())
	for _, item := range *sched.Queue {
		keys = append(keys, item.Job.Key())
	}

	return keys
}

// GetScheduledJob returns the ScheduledJob by the unique key.
func (sched *StdScheduler) GetScheduledJob(key int) (*ScheduledJob, error) {
	sched.Lock()
	defer sched.Unlock()

	for _, item := range *sched.Queue {
		if item.Job.Key() == key {
			return &ScheduledJob{
				item.Job,
				item.Trigger.Description(),
				item.priority,
			}, nil
		}
	}

	return nil, errors.New("No Job with the given Key found")
}

// DeleteJob removes the job for the specified key from the StdScheduler if present.
func (sched *StdScheduler) DeleteJob(key int) error {
	sched.Lock()
	defer sched.Unlock()

	for i, item := range *sched.Queue {
		if item.Job.Key() == key {
			sched.Queue.Remove(i)
			return nil
		}
	}

	return errors.New("No Job with the given Key found")
}

// Clear removes all of the scheduled jobs.
func (sched *StdScheduler) Clear() {
	sched.Lock()
	defer sched.Unlock()

	// reset the jobs queue
	sched.Queue = &PriorityQueue{}
}

// Stop exits the StdScheduler execution loop.
func (sched *StdScheduler) Stop() {
	log.Printf("Closing the StdScheduler.")
	close(sched.exit)
}

func (sched *StdScheduler) startExecutionLoop(location *time.Location) {
	for {
		if sched.queueLen() == 0 {
			select {
			case <-sched.interrupt:
			case <-sched.exit:
				return
			}
		} else {
			tick := sched.calculateNextTick(location)
			select {
			case <-tick:
				sched.executeAndReschedule(location)
			case <-sched.interrupt:
				continue
			case <-sched.exit:
				log.Printf("Exit the execution loop.")
				return
			}
		}
	}
}

func (sched *StdScheduler) queueLen() int {
	sched.Lock()
	defer sched.Unlock()

	return sched.Queue.Len()
}

func (sched *StdScheduler) calculateNextTick(location *time.Location) <-chan time.Time {
	sched.Lock()
	ts := sched.Queue.Head().priority
	sched.Unlock()

	return time.After(time.Duration(parkTime(ts, location)))
}

func (sched *StdScheduler) executeAndReschedule(location *time.Location) {
	// return if the job queue is empty
	if sched.queueLen() == 0 {
		return
	}

	// fetch an item
	sched.Lock()
	item := heap.Pop(sched.Queue).(*Item)
	sched.Unlock()

	// execute the Job
	if !isOutdated(item.priority, location) {
		go item.Job.Execute()
	}

	// reschedule the Job
	nextRunTime, err := item.Trigger.NextFireTime(item.priority, location)
	if err == nil {
		item.priority = nextRunTime
		sched.feeder <- item
	}
}

func (sched *StdScheduler) startFeedReader() {
	for {
		select {
		case item := <-sched.feeder:
			sched.Lock()
			heap.Push(sched.Queue, item)
			sched.reset()
			sched.Unlock()
		case <-sched.exit:
			log.Printf("Exit the feed reader.")
			return
		}
	}
}

func (sched *StdScheduler) reset() {
	select {
	case sched.interrupt <- struct{}{}:
	default:
	}
}

func parkTime(ts int64, location *time.Location) int64 {
	now := NowNano(location)
	if ts > now {
		return ts - now
	}
	return 0
}
