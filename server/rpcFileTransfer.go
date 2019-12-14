package server

import (
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"net/rpc"
	"os"
	"time"
)

var FILE_RPC_PORT string = "7000"
var SERVER_FOLDER_NAME string = "serverFiles"
var LOCAL_FOLDER_NAME string = "localFiles"

type FileTransferRequest struct {
	FileName  string
	FileGroup []string
	Data      []byte
}

type FileTransfer int

// Caller will send the file to the server. Server saves the file
func (t *FileTransfer) SendFile(request FileTransferRequest, _ *[]byte) error {
	filePath := SERVER_FOLDER_NAME + "/" + request.FileName
	fileDes, _ := os.OpenFile(filePath, os.O_TRUNC|os.O_CREATE|os.O_RDWR, 0666)
	defer fileDes.Close()

	fileDes.Write(request.Data)
	LocalFiles.Files[request.FileName] = request.FileGroup
	LocalFiles.UpdateTimes[request.FileName] = time.Now().UnixNano() / int64(time.Millisecond)
	log.Infof("Stored file %s to this server!", request.FileName)

	return nil
}

// Caller will request a file from the server. Server replies with the file
func (t *FileTransfer) GetFile(request FileTransferRequest, data *[]byte) error {
	filePath := SERVER_FOLDER_NAME + "/" + request.FileName
	fileContents, _ := ioutil.ReadFile(filePath)
	*data = fileContents
	log.Infof("Sending file %s to client!", request.FileName)

	return nil
}

// Function specific for processing maps from other workers. 
func (t *FileTransfer) AppendData(request FileTransferRequest, _ *[]byte) error {
	
	// In this case, request.FileName will be the sourcehost name
	filePath := request.FileName + "_tempMapOutput.txt" 
	fileDes, _ := os.OpenFile(filePath, os.O_TRUNC|os.O_CREATE|os.O_RDWR, 0666)
	fileDes.Write(request.Data)
	fileDes.Close()

	log.Infof("Starting to process the aggregate map from %s", request.FileName)
	ProcessAggregateMap(filePath)
	log.Infof("Finished processing the aggregate map from %s", request.FileName)
	os.Remove(filePath)

	return nil
}

// Helper that will invoke the tranfer data RPC as specified by requestType
func CallFileTransferRPC(hostname string, requestType string, request *FileTransferRequest) ([]byte) {
	client, err := rpc.DialHTTP("tcp", hostname+":"+FILE_RPC_PORT)
	if err != nil {
		log.Fatalf("Could not dial server for file transfer: ", err)
		return []byte{}
	}
	defer client.Close()

	var response []byte
	err = client.Call(requestType, &request, &response)
	if err != nil {
		log.Fatalf("Error in request", err)
		return []byte{}
	}

	return response
}