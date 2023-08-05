package mongostorage

import (
	"log"
	"math/rand"
	"time"
)


// calculateExponentialBackoff is a function that takes an integer 'attempt' representing the current attempt number for a retry operation.
// It calculates the backoff duration to be used before the next retry based on exponential backoff with jitter.
// function written by AI.
func calculateExponentialBackoff(attempt int) time.Duration {
	maxbackoff := time.Duration(30000)
	backoffBase := 100 * time.Millisecond // Base backoff duration (adjust as needed)
	backoffFactor := 2                    // Backoff factor (adjust as needed)

	// Calculate the backoff duration with exponential increase
	backoffDuration := time.Duration(backoffFactor<<uint(attempt-1)) * backoffBase
	if backoffDuration > maxbackoff {
		backoffDuration = maxbackoff
	}
	// Add some jitter to prevent all clients from retrying simultaneously
	jitter := time.Duration(rand.Int63n(int64(backoffBase / 2)))
	return backoffDuration + jitter
} // end func calculateExponentialBackoff

// MongoWorker_UpDN_Random periodically sends random up/down (true/false) signals to the worker channels.
// The purpose of this function is to simulate random up/down requests to control the worker
// function not written by AI.
// ./mongodbtest -randomUpDN -test-num 0
func MongoWorker_UpDN_Random() {
	isleep := 1
	log.Print("Start mongostorage.MongoWorker_UpDN_Random")
	for {
		arandA := rand.Intn(2)
		arandB := rand.Intn(4)
		time.Sleep(time.Second * time.Duration(isleep))
		sendbool := false
		switch arandA {
		case 1:
			sendbool = true
		default:
		}
		switch arandB {
		case 0:
			//wType = "reader"
			log.Printf("~~ UpDN_Random sending %t to UpDn_Reader_Worker_chan", sendbool)
			UpDn_Reader_Worker_chan <- sendbool
		case 1:
			//wType = "delete"
			log.Printf("~~ UpDN_Random sending %t to UpDn_Delete_Worker_chan", sendbool)
			UpDn_Delete_Worker_chan <- sendbool
		case 2:
			//wType = "insert"
			log.Printf("~~ UpDN_Random sending %t to UpDn_Insert_Worker_chan", sendbool)
			UpDn_Insert_Worker_chan <- sendbool
		case 3:
			//wType = "StopAll"
			log.Printf("~~ UpDN_Random sending %t to UpDn_StopAll_Worker_chan", sendbool)
			UpDn_StopAll_Worker_chan <- sendbool
		default:
		}
	}
} // end func MongoWorker_UpDN_Random

// The iStop_Worker function is responsible for stopping a specific type of worker (reader, delete, or insert)
// The function uses channels to communicate with the worker goroutines and control their termination.
// The function uses a switch statement to identify the type of worker to stop.
// For each type, it reads the current maximum worker ID from the corresponding channel.
// After retrieving the ID, it immediately writes it back to the channel, effectively "parking" the value again, so other parts of the code can still access the ID.
// function not written by AI.
func iStop_Worker(wType string) int {
	var maxwid int
	switch wType {
	case "reader":
		maxwid = <-stop_reader_worker_chan // read value
		stop_reader_worker_chan <- maxwid  // park value again
	case "delete":
		maxwid = <-stop_delete_worker_chan // read value
		stop_delete_worker_chan <- maxwid  // park value again
	case "insert":
		maxwid = <-stop_insert_worker_chan // read value
		stop_insert_worker_chan <- maxwid  // park value again
	default:
		log.Printf("Error iStop_Worker unknown Wtype=%s", wType)
	} // end switch wType
	return maxwid
} // fund end iStop_Worker

// updn_Set internally updates the worker count of the specified type and starts worker goroutines accordingly.
// function not written by AI.
func updn_Set(wType string, maxwid int, cfg *MongoStorageConfig) {
	var oldval int
	switch wType {
	case "reader":
		oldval = <-stop_reader_worker_chan // read old value
		stop_reader_worker_chan <- maxwid  // set new value
	case "delete":
		oldval = <-stop_delete_worker_chan // read old value
		stop_delete_worker_chan <- maxwid  // set new value
	case "insert":
		oldval = <-stop_insert_worker_chan // read old value
		stop_insert_worker_chan <- maxwid  // set new value
	default:
		log.Printf("Error updn_Set unknown Wtype=%s", wType)
	} // end switch wType

	if maxwid > oldval {
		// start new worker routines for wType
		switch wType {
		case "reader":
			for i := oldval + 1; i <= maxwid; i++ {
				go mongoWorker_Reader(i, &READER, cfg)
			}
		case "delete":
			for i := oldval + 1; i <= maxwid; i++ {
				go mongoWorker_Delete(i, &DELETE, cfg)
			}
		case "insert":
			for i := oldval + 1; i <= maxwid; i++ {
				go mongoWorker_Insert(i, &INSERT, cfg)
			}
		default:
			log.Printf("Error updn_Set unknown Wtype=%s", wType)
		} // end switch wType
	}
	log.Printf("$$ updn_Set wType=%s oldval=%d maxwid=%d", wType, oldval, maxwid)
} // end func updn_Set

// mongoWorker_UpDn_Scaler runs in the background and listens on channels for up/down requests to start/stop workers.
// Explanation:
// mongoWorker_UpDn_Scaler is responsible for managing the scaling of worker goroutines based on up/down requests.
// It listens to UpDn_*_Worker_chan channels to receive requests for starting or stopping specific types of workers.
// The function uses updn_Set to update the worker counts accordingly, which effectively starts or stops worker goroutines.
// This mechanism allows the application to dynamically adjust the number of worker goroutines based on the workload
// or other factors.
// Note: The function runs in the background and continues to process requests as they arrive.
//
// function not written by AI.
func mongoWorker_UpDn_Scaler(cfg *MongoStorageConfig) { // <-- needs load inital values
	// load initial values into channels
	stop_reader_worker_chan <- cfg.GetWorker
	stop_delete_worker_chan <- cfg.DelWorker
	stop_insert_worker_chan <- cfg.InsWorker
	atimeout := time.Duration(time.Second * 5)

	// The anonymous goroutine periodically checks the UpDn_StopAll_Worker_chan channel for messages.
	// If a "true" value is received, it sends "false" to all UpDn_*_Worker channels
	//   (UpDn_Reader_Worker_chan, UpDn_Delete_Worker_chan, and UpDn_Insert_Worker_chan)
	// to signal the worker goroutines to stop gracefully.
	// If a "false" value is received, no action is taken.
	// The goroutine also logs a message every N seconds to indicate that it is alive and running.
	// This functionality allows dynamically scaling the number of worker goroutines based on received up/down requests
	// while keeping track of their status and allowing controlled termination.
	go func(getWorker int, delWorker int, insWorker int) {
		timeout := time.After(atimeout)
		for {
			select {
			case <-timeout:
				timeout = time.After(atimeout)
				//log.Printf("UpDn_StopAll_Worker_chan alive")

			case retbool := <-UpDn_StopAll_Worker_chan:
				switch retbool {
				case true:
					// pass a false to all UpDn_*_Worker channels to stop them
					for i := 1; i <= getWorker; i++ {
						UpDn_Reader_Worker_chan <- false
					}
					for i := 1; i <= delWorker; i++ {
						UpDn_Delete_Worker_chan <- false
					}
					for i := 1; i <= insWorker; i++ {
						UpDn_Insert_Worker_chan <- false
					}
				case false:
					// pass here, will not do anything
					// to stop all workers
					//   pass a single true to UpDn_StopAll_Worker_chan
				} // end switch retbool
			} // end select
		} // end for
	}(cfg.GetWorker, cfg.DelWorker, cfg.InsWorker)
	time.Sleep(time.Second / 1000)

	// The anonymous goroutine continuously listens to the UpDn_Reader_Worker_chan channel for messages.
	// If a "true" value is received, it increments the GetWorker variable to increase the number of reader worker goroutines.
	// If a "false" value is received, it decrements GetWorker to decrease the number of reader worker goroutines.
	// After processing the message, the goroutine calls the updn_Set function to update the reader worker count with the new value (GetWorker).
	// The goroutine also logs a message every N seconds to indicate that it is alive and running.
	// The time.Sleep function is used to slightly delay the execution to avoid consuming excessive resources.
	// This functionality allows dynamically scaling the number of reader worker goroutines based on received up/down requests
	// and keeping track of their status while ensuring controlled termination and worker count adjustment.
	go func(getWorker int, wType string) {
		timeout := time.After(atimeout)
		for {
			select {
			case <-timeout:
				timeout = time.After(atimeout)
				//log.Printf("UpDn_Reader_Worker_chan alive")

			case retbool := <-UpDn_Reader_Worker_chan:
				switch retbool {
				case true:
					getWorker++
				case false:
					if getWorker > 0 {
						getWorker--
					}
				} // end switch retbool
				updn_Set(wType, getWorker, cfg)
			} // end select
		} // end for
	}(cfg.GetWorker, "reader")
	time.Sleep(time.Second / 1000)

	// The anonymous goroutine continuously listens to the UpDn_Delete_Worker_chan channel for messages.
	// If a "true" value is received, it increments the DelWorker variable to increase the number of delete worker goroutines.
	// If a "false" value is received, it decrements DelWorker to decrease the number of delete worker goroutines.
	// After processing the message, the goroutine calls the updn_Set function to update the delete worker count with the new value (DelWorker).
	// The goroutine also logs a message every N seconds to indicate that it is alive and running.
	// The time.Sleep function is used to slightly delay the execution to avoid consuming excessive resources.
	//This functionality allows dynamically scaling the number of delete worker goroutines based on received up/down requests
	// and keeping track of their status while ensuring controlled termination and worker count adjustment.
	go func(delWorker int, wType string) {
		timeout := time.After(atimeout)
		for {
			select {
			case <-timeout:
				timeout = time.After(atimeout)
				//log.Printf("UpDn_Delete_Worker_chan alive")

			case retbool := <-UpDn_Delete_Worker_chan:
				switch retbool {
				case true:
					delWorker++
				case false:
					if delWorker > 0 {
						delWorker--
					}
				} // end switch retbool
				updn_Set(wType, delWorker, cfg)
			} // end select
		} // end for
	}(cfg.DelWorker, "delete")
	time.Sleep(time.Second / 1000)

	// The anonymous goroutine continuously listens to the UpDn_Insert_Worker_chan channel for messages.
	// If a "true" value is received, it increments the InsWorker variable to increase the number of insert worker goroutines.
	// If a "false" value is received, it decrements InsWorker to decrease the number of insert worker goroutines.
	// After processing the message, the goroutine calls the updn_Set function to update the insert worker count with the new value (InsWorker).
	// The goroutine also logs a message every N seconds to indicate that it is alive and running.
	// The time.Sleep function is used to slightly delay the execution to avoid consuming excessive resources.
	// This functionality allows dynamically scaling the number of insert worker goroutines based on received up/down requests
	// and keeping track of their status while ensuring controlled termination and worker count adjustment.
	go func(insWorker int, wType string) {
		timeout := time.After(atimeout)
		for {
			select {
			case <-timeout:
				timeout = time.After(atimeout)
				//log.Printf("UpDn_Insert_Worker_chan alive")

			case retbool := <-UpDn_Insert_Worker_chan:
				switch retbool {
				case true:
					insWorker++
				case false:
					if insWorker > 0 {
						insWorker--
					}
				} // end switch retbool
				updn_Set(wType, insWorker, cfg)
			} // end select
		} // end for
	}(cfg.InsWorker, "insert")
	time.Sleep(time.Second / 1000)

} // end func mongoWorker_UpDn_Scaler

func updateWorkerStatus(wType *string, status update) {
	//log.Printf("## updateWorkerStatus wType=%s status='%v'", *wType, status)
	worker_status_chan <- workerstatus{wType: *wType, status: status}
}

func workerStatus() {
	// worker status counter map
	// prevents workers from calling sync.Mutex
	// workers send updates into worker_status_chan
	counter := make(map[string]map[string]uint64)
	counter["reader"] = make(map[string]uint64)
	counter["delete"] = make(map[string]uint64)
	counter["insert"] = make(map[string]uint64)
	timeout := time.After(time.Millisecond * 2500)
	for {
		select {

			case <- timeout:

				Counter.Set("Did_mongoWorker_Reader", counter["reader"]["did"])
				Counter.Set("Did_mongoWorker_Delete", counter["delete"]["did"])
				Counter.Set("Did_mongoWorker_Insert", counter["insert"]["did"])

				Counter.Set("Running_mongoWorker_Reader", counter["reader"]["run"])
				Counter.Set("Running_mongoWorker_Delete", counter["delete"]["run"])
				Counter.Set("Running_mongoWorker_Insert", counter["insert"]["run"])

				timeout = time.After(time.Millisecond * 1000)

		case nu := <-worker_status_chan:

			if nu.status.Did > 0 {
				counter[nu.wType]["did"] += uint64(nu.status.Did)
			}

			if nu.status.Bad > 0 {
				counter[nu.wType]["bad"] += uint64(nu.status.Bad)
			}

			if nu.status.Boot {
				counter[nu.wType]["run"]++
			} else if nu.status.Stop {
				if counter[nu.wType]["run"] > 0 {
					counter[nu.wType]["run"]--
				}
			}

		} // end select
	} // end for
} // end func WorkerStatus
