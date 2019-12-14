package server

import (
	log "github.com/sirupsen/logrus"
	"os"
	"runtime"
	"time"
	"sync"
)

var JuiceFileList []string
var JuiceAssignmentMap map[string][]string

var JuiceMutex sync.Mutex

func JuiceMasterManager() {
	hostname, _ := os.Hostname()
	JuiceMutex.Lock()

	LEADER_CHECK: for {
		if Membership.List[0] == hostname && len(Membership.MJQueue) != 0 && Membership.MJQueue[0].Command == "Juice" {
			log.Info("Juice request detected and the current node is the master node!")
			break
		}

		runtime.Gosched()
	}

	JuiceFileList = CallGrossFindDir(Membership.List[1])
	JuiceAssignmentMap = map[string][]string{}
	JuiceMutex.Unlock()

	for {
		// If a lower node is added back to the Membership list, this node stops being the master
		if Membership.List[0] != hostname {
			log.Info("A lower ID node has joined! Current node no longer the master!")
			goto LEADER_CHECK
		}

		detectJuiceWorkerFailures()
		if len(JuiceFileList) == 0 {
			if Membership.MJQueue[0].DeleteInput {
				log.Infof("Deleting temp folder!")
				go deleteFolder()
			}

			Membership.MJQueue = Membership.MJQueue[1:]
			Membership.MapleJuiceUTime = time.Now().UnixNano() / int64(time.Millisecond)

			log.Infof("Juice request has been completed!")			
			goto LEADER_CHECK	
		}
	}

	runtime.Gosched()
}

// Detects worker failures
func detectJuiceWorkerFailures() {
	workerCount := GetWorkerCount()
	currentWorkerMap := map[string]int{}
	for i := 0; i <= workerCount; i++ {
		currentWorkerMap[Membership.List[i]] = 0
	}

	for workerName, processedFiles := range JuiceAssignmentMap {
		if _, contains := currentWorkerMap[workerName]; !contains {
			currTime := time.Now().UnixNano() / int64(time.Millisecond)
			if Membership.Data[workerName] - currTime > WORKER_TIMEOUT {
				log.Infof("Detected that worker %s has failed! Reassigning those files", workerName)
				JuiceFileList = append(JuiceFileList, processedFiles...)
				delete(JuiceAssignmentMap, workerName)
			}
		}
	}
}

// Helper that deletes a folder
func deleteFolder() {
	for i := 0; i < len(Membership.List); i++ {
		CallDeleteFolder(Membership.List[i])
	}
}