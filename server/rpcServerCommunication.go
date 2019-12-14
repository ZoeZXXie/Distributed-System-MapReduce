package server

import (
	log "github.com/sirupsen/logrus"
	"net/rpc"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"time"
)

var SERVER_RPC_PORT string = "6000"
var FILE_DELIMITER string = "~"

type ServerRequestArgs struct {
	ID       string
	FileName string
	HostList []string
}

type ServerCommunication int

// When the file is found, it will call this RPC to the leader and send the fileGroup via golbal map
func (t *ServerCommunication) FileFound(serverAck *ServerRequestArgs, _ *string) error {
	FindFileResponseMutex.Lock()
	FileFoundResponses[serverAck.ID] = serverAck.HostList
	FindFileResponseMutex.Unlock()
	return nil
}

// When the file is not found on a server, the server pings the master to inform it.
func (t *ServerCommunication) FileNotFound(serverAck *ServerRequestArgs, _ *string) error {
	FindFileResponseMutex.Lock()
	FileNotFoundCounts[serverAck.ID]++
	FindFileResponseMutex.Unlock()
	return nil
}


// This call will be used to update the fileGroup when files are resharded
func (t *ServerCommunication) UpdateFileGroup(request ServerRequestArgs, _ *string) error {
	LocalFiles.Files[request.FileName] = request.HostList
	LocalFiles.UpdateTimes[request.FileName] = time.Now().UnixNano() / int64(time.Millisecond)

	return nil
}

// This call will look for any files that are in the specified directory
func (t *ServerCommunication) FindDirectory(dirName string, files *[]string) error {
	hostname, _ := os.Hostname()
	dirFiles := []string{}

	for file, fileGroup := range LocalFiles.Files {
		folderName := strings.Split(file, FILE_DELIMITER)[0]

		if folderName == dirName && fileGroup[0] == hostname {
			dirFiles = append(dirFiles, file)
		}
	}

	*files = dirFiles
	return nil
}

// This is gross and I know it
func (t *ServerCommunication) GrossFindDirectory(_ string, files *[]string) error {
	fileList := []string{}
	dirFiles, _ := ioutil.ReadDir(MAPLE_TEMP_FOLDER_NAME)
	for _, file := range dirFiles {
		fileList = append(fileList, file.Name())
	}

	*files = fileList
	return nil
}

// This call will get the list of all files processed at the worker
func (t *ServerCommunication) GetProcessedFiles(_ string, files *[]string) error {
	*files = ProcessedFiles
	return nil
}

// This call will be called for every node to delete the tempMapleOutput folder
func (t *ServerCommunication) DeleteFolder(_ string, _ *string) error {
	names, err := ioutil.ReadDir("mapleTempOutputs")
	if err != nil {
		return err
	}
	for _, entery := range names {
		os.RemoveAll(path.Join([]string{"mapleTempOutputs", entery.Name()}...))
	}
	return nil
}

// Helper that will invoke the tranfer data RPC as specified by requestType
func CallServerCommunicationRPC(hostname string, requestType string, request *ServerRequestArgs) {
	client, err := rpc.DialHTTP("tcp", hostname+":"+SERVER_RPC_PORT)
	if err != nil {
		log.Fatalf("Could not dial server for server communication: ", err)
		return
	}
	defer client.Close()

	err = client.Call(requestType, &request, nil)
	if err != nil {
		log.Fatalf("Error in request", err)
	}
}

// Helper that will invoke the FindDirectory RPC which will get all files in the specified directory
func CallFindDirectoryRPC(hostname string, dirName string) ([]string) {
	client, err := rpc.DialHTTP("tcp", hostname+":"+SERVER_RPC_PORT)
	if err != nil {
		log.Fatalf("Could not dial server for finding files in %s: ", dirName, err)
		return []string{}
	}
	defer client.Close()

	var response []string
	err = client.Call("ServerCommunication.FindDirectory", dirName, &response)
	if err != nil {
		log.Fatalf("Error in find directory", err)
		return []string{}
	}

	return response
}

// Gross function cause fml
func CallGrossFindDir(hostname string) ([]string) {
	client, err := rpc.DialHTTP("tcp", hostname+":"+SERVER_RPC_PORT)
	if err != nil {
		return []string{}
	}
	defer client.Close()

	var response []string
	err = client.Call("ServerCommunication.GrossFindDirectory", "", &response)
	if err != nil {
		log.Fatalf("Error in find directory", err)
		return []string{}
	}

	return response
}

// Helper that will call the getProcessedFiles RPC
func CallGetProcessedFilesRPC(hostname string) ([]string) {
	client, err := rpc.DialHTTP("tcp", hostname+":"+SERVER_RPC_PORT)
	if err != nil {
		log.Fatalf("Could not dial server for finding processed files %s", err)
		return []string{}
	}
	defer client.Close()

	var response []string
	err = client.Call("ServerCommunication.GetProcessedFiles", "", &response)
	if err != nil {
		log.Fatalf("Error in find processed files", err)
		return []string{}
	}

	return response
}

// Helper that will call the delete folder RPC
func CallDeleteFolder(hostname string) {
	client, err := rpc.DialHTTP("tcp", hostname+":"+SERVER_RPC_PORT)
	if err != nil {
		log.Fatalf("Could not dial server for finding processed files %s", err)
	}
	defer client.Close()

	err = client.Call("ServerCommunication.DeleteFolder", "", nil)
	if err != nil {
		log.Fatalf("Error in find delete folder", err)
	}
}