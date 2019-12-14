package server

import (
	log "github.com/sirupsen/logrus"
	"net"
	"net/rpc"
	"net/http"
	"os"
	"runtime"
	"sync"
	"time"
)

var MapleFileList []string
var FileAssignmentMap map[string][]string
var WorkerResponses map[string]bool

var WORKER_TIMEOUT int64 = 6000
var ProcessFileMutex sync.Mutex

func MapleMasterManager() {
	ProcessFileMutex.Lock()
	go mapleJuiceRequestListener()

	// If this node becomes the lowest, ID, it becomes the master node
	hostname, _ := os.Hostname()
	LEADER_CHECK: for {
		if Membership.List[0] == hostname && len(Membership.MJQueue) != 0  && Membership.MJQueue[0].Command == "Maple" {
			log.Info("Maple request detected and the current node is the master node!")
			break
		}

		runtime.Gosched()
	}

	FileAssignmentMap = getProcessedFileMap()
	MapleFileList = findFileList()
	WorkerResponses = map[string]bool{}

	finishedProcessingFiles := false
	ProcessFileMutex.Unlock()
	log.Infof("Master got file list! Unlocking mutex.")

	for {
		// If a lower node is added back to the Membership list, this node stops being the master
		if Membership.List[0] != hostname {
			log.Info("A lower ID node has joined! Current node no longer the master!")
			goto LEADER_CHECK
		}

		detectWorkerFailures()
		if !finishedProcessingFiles && len(MapleFileList) == 0 {
			finishedProcessingFiles = true
			log.Info("Finished scheduling files to be processed!")
		}

		// If all workers have sent out their aggregate mapper outputs, tell them to move them to the SDFS
		if checkWorkerResponses() {
			// broadcastFinishRequest(filePrefix)

			Membership.MJQueue = Membership.MJQueue[1:]
			Membership.MapleJuiceUTime = time.Now().UnixNano() / int64(time.Millisecond)

			log.Infof("Maple request has been completed!")			
			goto LEADER_CHECK	
		}

		runtime.Gosched()
	}
}

// Goroutine that will listen for incoming RPC requests made to the Maple Juice main server
func mapleJuiceRequestListener() {
	server := rpc.NewServer()
	mapleJuiceRequestRPC := new(ExecuteMapleJuice)
	err := server.Register(mapleJuiceRequestRPC)
	if err != nil {
		log.Fatalf("Format of service ClientRequest isn't correct. %s", err)
	}

	oldMux := http.DefaultServeMux
    mux := http.NewServeMux()
    http.DefaultServeMux = mux
	server.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)
	http.DefaultServeMux = oldMux

	listener, _ := net.Listen("tcp", ":"+MAPLEJUICE_RPC_PORT)
	http.Serve(listener, mux)
}

// Function that will query the worker nodes to see what files they have finished processing
func getProcessedFileMap() (map[string][]string) {
	processedFiles := map[string][]string{}

	workerCount := GetWorkerCount()
	for i := 1; i <= workerCount; i++ {
		nodeName := Membership.List[i]
		fileList := CallGetProcessedFilesRPC(nodeName)
		processedFiles[nodeName] = fileList
	}

	return processedFiles
}

// Finds the files in the filesystem in the specified directory in the request
func findFileList() (fileList []string) {
	fileMap := map[string]int{}

	for _, node := range Membership.List {
		findDirResponse := CallFindDirectoryRPC(node, Membership.MJQueue[0].FileDirectory)

		for _, responseNode := range findDirResponse {
			fileMap[responseNode] = 0
		}
	}

	// Remove files in the fileMap that were already in the FileAssignmentMap
	for _, finishedFiles := range FileAssignmentMap {
		for _, file := range finishedFiles {
			if _, contains := fileMap[file]; contains {
				delete(fileMap, file)
			}
		}
	}

	for file, _ := range fileMap {
		fileList = append(fileList, file)
	}

	return fileList
}

// This will check to see if any of the worker nodes have failes and will add the files at
// The failed node to be reprocessed again
func detectWorkerFailures() {
	workerCount := GetWorkerCount()
	currentWorkerMap := map[string]int{}
	for i := 0; i <= workerCount; i++ {
		currentWorkerMap[Membership.List[i]] = 0
	}

	for workerName, processedFiles := range FileAssignmentMap {
		if _, contains := currentWorkerMap[workerName]; !contains {
			currTime := time.Now().UnixNano() / int64(time.Millisecond)
			if Membership.Data[workerName] - currTime > WORKER_TIMEOUT {
				log.Infof("Detected that worker %s has failed! Reassigning those files", workerName)
				MapleFileList = append(MapleFileList, processedFiles...)
				delete(FileAssignmentMap, workerName)
			}
		}
	}
}

// This will check if the master has recieved OKs from all workers. If it did, it will tell
// All workers to move the temp files to the sdfs and update their fileGroups
func checkWorkerResponses() (bool) {
	workerCount := GetWorkerCount()
	for i := 1; i <= workerCount; i++ {
		workerHostname := Membership.List[i]
		if _, contains := WorkerResponses[workerHostname]; !contains {
			return false
		}
	}

	return true
}

// The master will broadcast to all worker nodes to move the files to the SDFS and update the filesystem
// func broadcastFinishRequest() {
// 	log.Info("Master broadcasting finish request!")
// 	filePrefix := Membership.MJQueue[0].FilePrefix
// 	workerNodes := Membership.List
// 	// filePrefix := Membership.MJQueue[0].FilePrefix
// 	for _, woker := range workerNodes{
// 		CallUploadKeyFilesRPC(woker, filePrefix)
// 	}
// 	log.Info("done")
// }