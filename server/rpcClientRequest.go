package server

import (
	log "github.com/sirupsen/logrus"
	"math/rand"
	"net/rpc"
	"os"
	"runtime"
	"sort"
	"strconv"
	"time"
)

type MapleJuiceRequestArgs struct {
	ExeName       string
	ProcessCount  int
	FilePrefix    string
	FileDirectory string
	DeleteInput   bool
}

type ClientResponseArgs struct {
	Success  bool
	HostList []string
}

// This RPC server will handle any requests made by the client to the server.
// The server will process it and add the request to the request buffer to try to find the file
// If the file is not found within a timeout, the server will respond with an empty list.
type ClientRequest int

type Request struct {
	ID       string
	Type     string
	SrcHost  string
	FileName string
}

var CLIENT_RPC_PORT string = "5000"

// Global that keeps track of how many requests have been created by this node
var requestCount int = 1000

func (t *ClientRequest) Put(requestFile string, response *ClientResponseArgs) error {
	log.Infof("Server recieved Put for file %s", requestFile)
	success, hostList := handleClientRequest("Put", requestFile)

	// We can change this to indicate if it was within the grace period
	response.Success = success

	// If the file was not found, pick four random nodes to shard the file to
	if !success {
		randomHostList := []string{}

		for {
			rand.Seed(time.Now().UnixNano())
			randIndex := rand.Intn(len(Membership.List))
			nodeName := Membership.List[randIndex]

			// If the current random pick matches one that was already picked, continue
			duplicate := false
			for i := 0; i < len(randomHostList); i++ {
				if randomHostList[i] == nodeName {
					duplicate = true
					break
				}
			}

			if duplicate {
				continue
			}

			randomHostList = append(randomHostList, nodeName)
			if len(randomHostList) == 4 || len(randomHostList) == len(Membership.List) {
				sort.Strings(randomHostList)
				response.HostList = randomHostList
				return nil
			}
		}
	}

	response.HostList = hostList
	return nil
}

func (t *ClientRequest) Get(requestFile string, response *ClientResponseArgs) error {
	log.Infof("Server recieved Get for file %s", requestFile)
	success, hostList := handleClientRequest("Get", requestFile)
	response.Success = success
	response.HostList = hostList

	return nil
}

func (t *ClientRequest) Delete(requestFile string, response *ClientResponseArgs) error {
	log.Infof("Server recieved Delete for file %s", requestFile)
	success, _ := handleClientRequest("Delete", requestFile)
	response.Success = success
	response.HostList = []string{}

	return nil
}

func (t *ClientRequest) List(requestFile string, response *ClientResponseArgs) error {
	log.Infof("Server recieved Ls for file %s", requestFile)
	success, hostList := handleClientRequest("List", requestFile)
	response.Success = success
	response.HostList = hostList

	return nil
}

func (t *ClientRequest) Maple(request *MapleJuiceRequestArgs, _ *ClientResponseArgs) error {
	mapleJuiceRequest := &MapleJuiceRequest{
		Command:       "Maple",
		ExeName:       request.ExeName,
		ProcessCount:  request.ProcessCount,
		FilePrefix:    request.FilePrefix,
		FileDirectory: request.FileDirectory,
		DeleteInput:   false,
	}

	Membership.MJQueue = append(Membership.MJQueue, mapleJuiceRequest)
	Membership.MapleJuiceUTime = time.Now().UnixNano() / int64(time.Millisecond)
	
	return nil
}

func (t *ClientRequest) Juice(request *MapleJuiceRequestArgs, _ *ClientResponseArgs) error {
	mapleJuiceRequest := &MapleJuiceRequest{
		Command:       "Juice",
		ExeName:       request.ExeName,
		ProcessCount:  request.ProcessCount,
		FilePrefix:    request.FilePrefix,
		FileDirectory: request.FileDirectory,
		DeleteInput:   request.DeleteInput,
	}

	Membership.MJQueue = append(Membership.MJQueue, mapleJuiceRequest)
	Membership.MapleJuiceUTime = time.Now().UnixNano() / int64(time.Millisecond)
	
	return nil
}

// Function that will handle adding the request to the request bus. Will wait for a response
// Or will time out and will return the response of the server.
func handleClientRequest(requestType string, requestFile string) (success bool, hostList []string) {
	// Will create a unique ID name with the curent VM name and a counter for how many client
	// Requests have been created from this node
	hostname, _ := os.Hostname()
	requestID := hostname + "[" + strconv.Itoa(requestCount) + "]"
	requestCount++

	request := &Request{
		ID:       requestID,
		Type:     requestType,
		SrcHost:  hostname,
		FileName: requestFile,
	}
	log.Infof("Adding %s request, ID: %s to pending bus", requestType, requestID)
	Membership.Pending = append(Membership.Pending, request)
	Membership.RequestUTime = time.Now().UnixNano() / int64(time.Millisecond)

	// Will check if the node has recieved a response from another server
	// Indicating that that node has the file
	FileNotFoundCounts[requestID] = 0

	for {
		// If the leader recieved a response from a server that the file was found
		if hostList, contains := FileFoundResponses[requestID]; contains {
			delete(FileFoundResponses, requestID)
			delete(FileNotFoundCounts, requestID)
			removeRequest(requestID)

			log.Infof("Found file %s in the sdfs", requestFile)
			return true, hostList
		}

		// If all servers reply with not found, this will return
		if counts, _ := FileNotFoundCounts[requestID]; counts >= len(Membership.List) {
			delete(FileFoundResponses, requestID)
			delete(FileNotFoundCounts, requestID)
			removeRequest(requestID)

			log.Infof("Could not find file %s in the sdfs!", requestFile)
			break
		}

		runtime.Gosched()
	}

	return false, []string{}
}

// Helper that will remove a pending request and update the RequestUTime
func removeRequest(requestID string) {
	for idx, request := range Membership.Pending {
		if request.ID == requestID {
			Membership.Pending = append(Membership.Pending[:idx], Membership.Pending[idx+1:]...)
			Membership.RequestUTime = time.Now().UnixNano() / int64(time.Millisecond)
			return
		}
	}
}

// This will invoke the specified requestType RPC call in the clientRequest file
func CallFileSystemRPC(hostname string, requestType string, fileName string) (response ClientResponseArgs, success bool) {
	callerHostname, _ := os.Hostname()
	log.Infof("Making %s request from %s to %s", requestType, callerHostname, hostname)

	client, err := rpc.DialHTTP("tcp", hostname+":"+CLIENT_RPC_PORT)
	if err != nil {
		log.Fatalf("Could not dial server for %s: ", requestType, err)
		return response, false
	}
	defer client.Close()

	err = client.Call(requestType, fileName, &response)
	if err != nil {
		log.Fatalf("Error in request: ", err)
		return response, false
	}

	return response, true
}

// This will invoke the specified requestType MapleJuice RPC call 
func CallMapleJuiceRPC(hostname string, requestType string, request *MapleJuiceRequest) {
	callerHostname, _ := os.Hostname()
	log.Infof("Making %s request from %s to %s", requestType, callerHostname, hostname)

	client, err := rpc.DialHTTP("tcp", hostname+":"+CLIENT_RPC_PORT)
	if err != nil {
		log.Fatalf("Could not dial server for %s: ", requestType, err)
		return
	}
	defer client.Close()

	err = client.Call(requestType, &request, nil)
	if err != nil {
		log.Fatalf("Error in request: ", err)
	}
}