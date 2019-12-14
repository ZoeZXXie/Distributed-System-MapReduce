package server

import (
	log "github.com/sirupsen/logrus"
	"net/rpc"
	"os"
)

type MapleJuiceRequest struct {
	Command       string
	ExeName       string
	ProcessCount  int
	FilePrefix    string
	FileDirectory string
	DeleteInput   bool
}

type ProcessFileResponse struct {
	FileName  string
	Done      bool
}

type ExecuteMapleJuice int

var MAPLEJUICE_RPC_PORT string = "8000"

func (t *ExecuteMapleJuice) ProcessFile(workerHostname string, response *ProcessFileResponse) error {
	ProcessFileMutex.Lock()

	if len(MapleFileList) == 0 {
		response.FileName = ""
		response.Done = true
		ProcessFileMutex.Unlock()
		
		return nil
	}

	log.Infof("Assigning file %s to worker %s be processed!", workerHostname, MapleFileList[0])
	FileAssignmentMap[workerHostname] = append(FileAssignmentMap[workerHostname], MapleFileList[0])
	response.FileName = MapleFileList[0]
	response.Done = false
	MapleFileList = MapleFileList[1:]
	ProcessFileMutex.Unlock()

	return nil
}

func (t *ExecuteMapleJuice) ProcessedMapOutput(workerHostname string, _ *string) error {
	WorkerResponses[workerHostname] = true
	log.Info("Master recieved process success!")
	return nil
}

// Each time this is requested, the master will allocate 250 keys for the process to map
func (t *ExecuteMapleJuice) RequestJuiceFiles(workerHostname string, response *[]string) error {
	JuiceMutex.Lock()

	if len(JuiceFileList) < 20 {
		log.Infof("Giving %s %d files to process", workerHostname, len(JuiceFileList))
		*response = JuiceFileList
		JuiceAssignmentMap[workerHostname] = JuiceFileList
		JuiceFileList = []string{}

	} else {
		log.Infof("Giving %s 20 files to process", workerHostname)
		*response = JuiceFileList[0:20]
		JuiceAssignmentMap[workerHostname] = JuiceFileList[0:20]
		JuiceFileList = JuiceFileList[20:]
	}

	log.Infof("Files left to process %d", len(JuiceFileList))
	JuiceMutex.Unlock()
	return nil
}

func (t *ExecuteMapleJuice) AppendResult(data []byte, _ *string) error {
	log.Info("Writing!")
	JuiceMutex.Lock()
	
	fileDes, _ := os.OpenFile("MJOut.txt", os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	fileDes.Write(data)
	fileDes.Close()

	JuiceMutex.Unlock()

	return nil
}

// Function that will get the next file to process for the worker node.
func CallProcessFileRPC(hostname string, workerHostname string) (ProcessFileResponse) {
	client, err := rpc.DialHTTP("tcp", hostname+":"+MAPLEJUICE_RPC_PORT)
	if err != nil {
		log.Fatalf("Error in dialing. %s", err)
	}

	var response ProcessFileResponse
	err = client.Call("ExecuteMapleJuice.ProcessFile", workerHostname, &response)
	if err != nil {
		log.Fatalf("error in ExecuteMapleJuice.ProcessFile", err)
	}

	return response
}

// Function that will ping the master to tell it that the worker has finished sending out its aggregate map
func CallProcessedMapOutputRPC(hostname string, workerHostname string) {
	client, err := rpc.DialHTTP("tcp", hostname+":"+MAPLEJUICE_RPC_PORT)
	if err != nil {
		log.Fatalf("Error in dialing. %s", err)
	}

	err = client.Call("ExecuteMapleJuice.ProcessedMapOutput", workerHostname, nil)
	if err != nil {
		log.Fatalf("error in ExecuteMapleJuice.ProcessedMapOutput", err)
	}
}

func CallRequestJuiceFilesRPC(hostname string, workerHostname string) ([]string) {
	client, err := rpc.DialHTTP("tcp", hostname+":"+MAPLEJUICE_RPC_PORT)
	if err != nil {
		log.Fatalf("Error in dialing. %s", err)
	}

	var response []string
	err = client.Call("ExecuteMapleJuice.RequestJuiceFiles", workerHostname, &response)
	if err != nil {
		log.Fatalf("error in ExecuteMapleJuice.RequestJuiceFiles", err)
	}

	return response
}

func CallAppendResultRPC(hostname string, data []byte) {
	client, err := rpc.DialHTTP("tcp", hostname+":"+MAPLEJUICE_RPC_PORT)
	if err != nil {
		log.Fatalf("Error in dialing. %s", err)
	}

	err = client.Call("ExecuteMapleJuice.AppendResult", data, nil)
	if err != nil {
		log.Fatalf("error in ExecuteMapleJuice.AppendResult", err)
	}
}