package server

import (
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"math/rand"
	"net"
	"net/rpc"
	"net/http"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"
)

var NUM_REPLICAS int = 4
var FILE_RESHARD_TIMEOUT int64 = 8000
var FindFileResponseMutex sync.Mutex
var FileSystemMutex sync.Mutex

// Local datastore that keeps track of the other nodes that have the same files
type LocalFileSystem struct {
	Files       map[string][]string
	UpdateTimes map[string]int64
}

// Need to keep a global file list and server response map
var FileFoundResponses map[string][]string
var FileNotFoundCounts map[string]int
var LocalFiles *LocalFileSystem

// go routine that will handle requests and resharding of files from failed nodes
func FileSystemManager() {
	FileFoundResponses = map[string][]string{}
	FileNotFoundCounts =  map[string]int{}
	LocalFiles = &LocalFileSystem{
		Files:       map[string][]string{},
		UpdateTimes: map[string]int64{},
	}

	go clientRequestListener()
	go serverResponseListener()
	go fileTransferListener()

	completedRequests := map[string]int{}
	for {
		// Check if there are any requests in the membership list
		for _, request := range Membership.Pending {

			// If the request is in the complete list or the node doesnt have the file, continue
			_, isRequestFinished := completedRequests[request.ID]
			_, nodeContainsFile := LocalFiles.Files[request.FileName]
			if isRequestFinished {
				runtime.Gosched()
				continue
			}

			if !nodeContainsFile {
				fileStatusRPC("ServerCommunication.FileNotFound", request)
				completedRequests[request.ID] = 0
				runtime.Gosched()
				continue
			}

			fileStatusRPC("ServerCommunication.FileFound", request)
			if request.Type == "Delete" {
				deleteLocalFile(request.FileName)
			}

			completedRequests[request.ID] = 0
		}

		findFailedNodes()
		completedRequests = cleanCompletedRequests(completedRequests)
		runtime.Gosched()
	}
}

// Goroutine that will listen for incoming RPC requests made from any client
func clientRequestListener() {
	server := rpc.NewServer()
	clientRequestRPC := new(ClientRequest)
	err := server.Register(clientRequestRPC)
	if err != nil {
		log.Fatalf("Format of service ClientRequest isn't correct. %s", err)
	}

	oldMux := http.DefaultServeMux
    mux := http.NewServeMux()
    http.DefaultServeMux = mux
	server.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)
	http.DefaultServeMux = oldMux

	listener, _ := net.Listen("tcp", ":"+CLIENT_RPC_PORT)
	http.Serve(listener, mux)
}

// Goroutine that will listen for incoming RPC calls from other servers
func serverResponseListener() {
	server := rpc.NewServer()
	serverResponseRPC := new(ServerCommunication)
	err := server.Register(serverResponseRPC)
	if err != nil {
		log.Fatalf("Format of service ServerCommunication isn't correct. %s", err)
	}

	oldMux := http.DefaultServeMux
    mux := http.NewServeMux()
    http.DefaultServeMux = mux
	server.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)
	http.DefaultServeMux = oldMux

	listener, _ := net.Listen("tcp", ":"+SERVER_RPC_PORT)
	http.Serve(listener, mux)
}

// Goroutine that will listen for incoming RPC connection to transfer a file
func fileTransferListener() {
	server := rpc.NewServer()
	fileTransferRPC := new(FileTransfer)
	err := server.Register(fileTransferRPC)
	if err != nil {
		log.Fatalf("Format of service fileTransfer isn't correct. %s", err)
	}

	oldMux := http.DefaultServeMux
    mux := http.NewServeMux()
    http.DefaultServeMux = mux
	server.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)
	http.DefaultServeMux = oldMux

	listener, _ := net.Listen("tcp", ":"+FILE_RPC_PORT)
	http.Serve(listener, mux)
}

// Function that deletes data for a file from the server and the localFiles struct
func deleteLocalFile(fileName string) {
	delete(LocalFiles.Files, fileName)
	delete(LocalFiles.UpdateTimes, fileName)
	log.Infof("File %s deleted from the server!", fileName)

	err := os.Remove(SERVER_FOLDER_NAME + "/" + fileName)
	if err != nil {
		log.Infof("Unable to remove file %s from local node!", fileName)
		return
	}
}

// Function that will send an RPC call to the leader indicating whether the file was found. If the
// file was found on the server, only the fileMaster will respond
func fileStatusRPC(requestType string, request *Request) {
	args := &ServerRequestArgs{
		ID: request.ID,
	}

	if requestType == "ServerCommunication.FileFound" {
		hostname, _ := os.Hostname()
		fileGroup, _ := LocalFiles.Files[request.FileName]
		if fileGroup[0] != hostname {
			return
		}

		args.FileName = request.FileName
		args.HostList = fileGroup
	}

	CallServerCommunicationRPC(request.SrcHost, requestType, args)
}

// This will find all the files that need to be resharded to another node
func findFailedNodes() {
	hostname, _ := os.Hostname()
	for fileName, fileGroup := range LocalFiles.Files {

		currTime := time.Now().UnixNano() / int64(time.Millisecond)
		fileGroupAliveNodes := []string{}
		for _, node := range fileGroup {

			// We want a longer time out than failure detection to duplicate a file
			if currTime-Membership.Data[node] < FILE_RESHARD_TIMEOUT {
				fileGroupAliveNodes = append(fileGroupAliveNodes, node)
			}
		}

		// If there are less than NUM_REPLICAS of a file, and the current node is the fileMaster, reshard
		if len(fileGroupAliveNodes) < NUM_REPLICAS && fileGroupAliveNodes[0] == hostname {
			// log.Infof("Resharding file %s", fileName)
			// go reshardFiles(fileName, fileGroupAliveNodes)
			runtime.Gosched()
		} else if len(fileGroupAliveNodes) != len(fileGroup) {
			LocalFiles.Files[fileName] = fileGroupAliveNodes
		}
	}
}

// This will randomly pick nodes that doesnt have the file to reshard the file to them
func reshardFiles(fileName string, fileGroupAliveNodes []string) {
	newFileGroup := make([]string, len(fileGroupAliveNodes))
	copy(newFileGroup, fileGroupAliveNodes)
	rand.Seed(time.Now().UnixNano())

	// Disgusting random function that will loop until it finds a node not in the runningNodes list
	newGroupMembers := []string{}
	for {
		randIndex := rand.Intn(len(Membership.List))
		nodeName := Membership.List[randIndex]

		duplicate := false
		for i := 0; i < len(newFileGroup); i++ {
			if newFileGroup[i] == nodeName {
				duplicate = true
				break
			}
		}

		if duplicate {
			continue
		}

		newFileGroup = append(newFileGroup, nodeName)
		newGroupMembers = append(newGroupMembers, nodeName)
		if len(newFileGroup) == NUM_REPLICAS {
			sort.Strings(newFileGroup)
			break
		}
	}

	// Update the fileGroup for the original owners of the file
	updateFileGroupArgs := &ServerRequestArgs{
		ID:       "",
		FileName: fileName,
		HostList: newFileGroup,
	}
	for _, node := range fileGroupAliveNodes {
		CallServerCommunicationRPC(node, "ServerCommunication.UpdateFileGroup", updateFileGroupArgs)
	}

	// Send the file and the new group over to the new members of the fileGroup
	loadedFile, _ := ioutil.ReadFile(SERVER_FOLDER_NAME + "/" + fileName)
	fileTransferArgs := &FileTransferRequest{
		FileName:  fileName,
		FileGroup: newFileGroup,
		Data:      loadedFile,
	}
	for _, node := range newGroupMembers {
		CallFileTransferRPC(node, "FileTransfer.SendFile", fileTransferArgs)
	}
}

// After a request dissppears from the request buffer, remove it from the local completed requests map
func cleanCompletedRequests(completedRequests map[string]int) (map[string]int) {
	currentRequests := map[string]int{}
	for _, request := range Membership.Pending {
		currentRequests[request.ID] = 0
	}

	for requestID, _ := range completedRequests {
		if _, contains := currentRequests[requestID]; !contains {
			delete(completedRequests, requestID)
		}
	}

	return completedRequests
}