package client

import (
	log "github.com/sirupsen/logrus"
	"cs-425-mp4/server"
	"io/ioutil"
	"math/rand"
	"strconv"
	"time"
)

// Function that will try to dial the lowest number server possible
func initClientRequest(requestType string, fileName string, mjRequest *server.MapleJuiceRequest) (server.ClientResponseArgs) {
	for i := 1; i <= 10; i++ {
		numStr := strconv.Itoa(i)
		if len(numStr) == 1 {
			numStr = "0" + numStr
		}

		connectName := "fa19-cs425-g84-" + numStr + ".cs.illinois.edu"
		if mjRequest != nil {
			server.CallMapleJuiceRPC(connectName, requestType, mjRequest)
			return server.ClientResponseArgs{}
		}

		response, success := server.CallFileSystemRPC(connectName, requestType, fileName)
		if success {
			log.Infof("Connected to server %s and recieved a response", connectName)
			return response
		}
	}

	log.Fatal("Could not connect to any server!")
	return server.ClientResponseArgs{}
}

func ClientPut(args []string) {
	var fileName string
	if len(args) == 1 {
		fileName = args[0]
	} else {
		fileName = args[1]
	}

	response := initClientRequest("ClientRequest.Put", fileName, nil)
	log.Infof("Putting file to %s", response.HostList)

	filePath := server.LOCAL_FOLDER_NAME + "/" + args[0]
	fileContents, _ := ioutil.ReadFile(filePath)

	request := &server.FileTransferRequest{
		FileName:  fileName,
		FileGroup: response.HostList,
		Data:      fileContents,
	}

	for i := 0; i < len(response.HostList); i++ {
		server.CallFileTransferRPC(response.HostList[i], "FileTransfer.SendFile", request)
	}
}

func ClientGet(args []string) {
	fileName := args[0]
	response := initClientRequest("ClientRequest.Get", fileName, nil)

	if !response.Success {
		log.Infof("File %s not found in the sdfs!", fileName)
		return
	}

	rand.Seed(time.Now().UnixNano())
	requestServer := response.HostList[rand.Intn(len(response.HostList))]
	request := &server.FileTransferRequest{
		FileName:  fileName,
		FileGroup: nil,
		Data:      nil,
	}
	transferResponse := server.CallFileTransferRPC(requestServer, "FileTransfer.GetFile", request)

	filePath := server.LOCAL_FOLDER_NAME + "/" + args[1]
	err := ioutil.WriteFile(filePath, transferResponse, 0666)
	if err != nil {
		log.Fatalf("Unable to write bytes from get!", err)
	}
}

func ClientDel(args []string) {
	fileName := args[0]
	response := initClientRequest("ClientRequest.Delete", fileName, nil)

	if response.Success {
		log.Infof("File %s deleted from the sdfs!", fileName)
	} else {
		log.Infof("File %s not found in the sdfs!", fileName)
	}
}

func ClientLs(args []string) {
	fileName := args[0]
	response := initClientRequest("ClientRequest.List", fileName, nil)

	if response.Success {
		log.Infof("File %s is stored at:\n%s", fileName, response.HostList)
	} else {
		log.Infof("File %s not found in the sdfs!", fileName)
	}
}

func ClientMaple(args []string) {
	numMaples, _ := strconv.Atoi(args[1])
	request := &server.MapleJuiceRequest{
		Command: "Maple",
		ExeName: args[0],
		ProcessCount: numMaples,
		FilePrefix: args[2],
		FileDirectory: args[3],
		DeleteInput: false,
	}

	initClientRequest("ClientRequest.Maple", "", request)
}

func ClientJuice(args []string) {
	numMaples, _ := strconv.Atoi(args[1])
	var deleteArg bool
	if args[4] == "0" {
		deleteArg = false
	} else {
		deleteArg = true
	}

	request := &server.MapleJuiceRequest{
		Command: "Juice",
		ExeName: args[0],
		ProcessCount: numMaples,
		FilePrefix: args[2],
		FileDirectory: args[3],
		DeleteInput: bool(deleteArg),
	}

	initClientRequest("ClientRequest.Juice", "", request)
}
