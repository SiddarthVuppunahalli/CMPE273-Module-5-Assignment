package main

import (
	"sync"
	"testing"
)

func TestProcessOnceAllowsOneWorkerPerEvent(t *testing.T) {
	const workers = 20
	stateDir := t.TempDir()
	start := make(chan struct{})
	results := make(chan bool, workers)
	errors := make(chan error, workers)
	var group sync.WaitGroup

	for i := 0; i < workers; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			processed, err := processOnce(stateDir, "same-event", 0)
			if err != nil {
				errors <- err
				return
			}
			results <- processed
		}()
	}
	close(start)
	group.Wait()
	close(results)
	close(errors)

	for err := range errors {
		t.Errorf("processOnce failed: %v", err)
	}
	winners := 0
	for processed := range results {
		if processed {
			winners++
		}
	}
	if winners != 1 {
		t.Fatalf("processed %d times, want exactly 1", winners)
	}
}
