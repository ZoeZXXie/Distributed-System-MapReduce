package server

import (
	log "github.com/sirupsen/logrus"
	"bufio"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"
)

var MAPLE_EXE_FOLDER_NAME string = "mapleExe"
var MAPLE_TEMP_FOLDER_NAME string = "mapleTempOutputs"
var MAPPER_OUTPUT_FILE_NAME string = "mapperOutputFile.txt"
var MAPPER_AGGREGATE_FILE_NAME string = "mapperAggregateOutput.txt"

var ProcessedFiles []string

func MapleWorkerManager() {
	ProcessedFiles = []string{}
	hostname, _ := os.Hostname()
	hasBroadcasted := false

	for {
		// If there is no mapleJuice request or if the top request is not a maple request
		if len(Membership.MJQueue) == 0 || Membership.MJQueue[0].Command != "Maple" {
			runtime.Gosched()
			continue
		}

		// Check if the current server is within the lowert ProcessCount servers or the master node
		workerCount := GetWorkerCount()
		if (hostname == Membership.List[0] || hostname > Membership.List[workerCount]) {
			runtime.Gosched()
			continue
		}

		// Ask the server for next file to process. If the time is the same, there was nothing new
		response := CallProcessFileRPC(Membership.List[0], hostname)
		if response.Done && hasBroadcasted {
			runtime.Gosched()
			continue
		}

		// If the fileName is empty, there are no more files to process, send the map to other workers
		if response.FileName == "" {
			log.Info("Processing the local aggregate map")
			go ProcessAggregateMap(MAPPER_AGGREGATE_FILE_NAME)
			log.Info("Sending out the aggregate map!")
			sendAggregateMap(workerCount)
			os.Remove(MAPPER_AGGREGATE_FILE_NAME)
			hasBroadcasted = true

			CallProcessedMapOutputRPC(Membership.List[0], hostname)			
			runtime.Gosched()
			continue
		} 

		log.Infof("Recieved file %s to process from the master!", response.FileName)
		ProcessedFiles = append(ProcessedFiles, response.FileName)
		exeName := Membership.MJQueue[0].ExeName
		exePath, filePath := fetchFiles(response.FileName, exeName)
		aggregateMapperFile(exePath, filePath)
		runtime.Gosched()
	}
}

// Simple function the get the number of workers. Export it because other files use it too
func GetWorkerCount() (int) {
	processCount := Membership.MJQueue[0].ProcessCount
	maxWorkers := len(Membership.List) - 1
	
	if processCount < maxWorkers {
		return processCount
	} else {
		return maxWorkers
	}
}

// Helper that will fetch the exe and process file if needed and return their relative paths
func fetchFiles(fileName string, exeName string) (exePath string, filePath string) {
	exePath = MAPLE_EXE_FOLDER_NAME + "/" +exeName
	if _, err := os.Stat(exePath); os.IsNotExist(err) {
		FetchFile(exeName, MAPLE_EXE_FOLDER_NAME)
	}

	if _, contains := LocalFiles.Files[fileName]; contains {
		filePath = SERVER_FOLDER_NAME + "/" + fileName
	} else {
		FetchFile(fileName, LOCAL_FOLDER_NAME)
		filePath = LOCAL_FOLDER_NAME + "/" + fileName
	}

	log.Info("Exe and file to process are stored locally!")
	return exePath, filePath
}

// Function that will make a call to fetch a file from the sdsf and stoes it in the specified fileDest
func FetchFile(fileName string, fileDest string) {
	log.Infof("Fetching file %s", fileName)
	response, _ := CallFileSystemRPC(Membership.List[0], "ClientRequest.Get", fileName)

	rand.Seed(time.Now().UnixNano())
	fileLocationHost := response.HostList[rand.Intn(len(response.HostList))]
	request := &FileTransferRequest{
		FileName:  fileName,
		FileGroup: nil,
		Data:      nil,
	}
	transferResponse := CallFileTransferRPC(fileLocationHost, "FileTransfer.GetFile", request)

	filePath := fileDest + "/" + fileName
	err := ioutil.WriteFile(filePath, transferResponse, 0666)
	if err != nil {
		log.Fatalf("Unable to write bytes from get!", err)
	}
}

// This function will call exec and sort the file and will then append it to the aggregate file
func aggregateMapperFile(exePath string, filePath string) {
	mapCommand := exec.Command("go", "run", exePath, filePath, MAPPER_OUTPUT_FILE_NAME)
	mapCommand.Run()

	sortOutCommand := exec.Command("sort", "-o", MAPPER_OUTPUT_FILE_NAME, MAPPER_OUTPUT_FILE_NAME)
	sortOutCommand.Run()

	fileContents, _ := ioutil.ReadFile(MAPPER_OUTPUT_FILE_NAME)
	fileDes, _ := os.OpenFile(MAPPER_AGGREGATE_FILE_NAME, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	fileDes.Write(fileContents)
	fileDes.Close()
	os.Remove(MAPPER_OUTPUT_FILE_NAME)

	sortAggregateCommand := exec.Command("sort", "-o", MAPPER_AGGREGATE_FILE_NAME, MAPPER_AGGREGATE_FILE_NAME)
	sortAggregateCommand.Run()

	log.Info("Finished writing commands!")
}

// Sends the aggregate map stored at this machine to others for processing
func sendAggregateMap(workerCount int) {
	hostname, _ := os.Hostname()
	fileContents, _ := ioutil.ReadFile(MAPPER_AGGREGATE_FILE_NAME)

	request := &FileTransferRequest{
		FileName: hostname,
		FileGroup: []string{},
		Data: fileContents,
	}

	// This will send it to all the other nodes in the system, not just other workers
	for i := 1; i < len(Membership.List); i++ {
		if Membership.List[i] == hostname {
			continue
		}

		go CallFileTransferRPC(Membership.List[i], "FileTransfer.AppendData", request)
	}
}

// Process the mapleOutput file and seperate it out into pre intermediate files
// RPC will also use this function so we need to export it
func ProcessAggregateMap(filePath string) {
	mapleOutputFileDes, _ := os.Open(filePath)
	scanner := bufio.NewScanner(mapleOutputFileDes)

	currentKey := ""
	var openedFile *os.File

	// Loop through each line in the file and create files based on each key in the aggregate map file
	for scanner.Scan()  {
		line := scanner.Text()
		outputKey := strings.Fields(line)[0]

		if currentKey != outputKey {
			openedFile.Close()

			currentKey = outputKey
			filePath := MAPLE_TEMP_FOLDER_NAME + "/" + currentKey + ".txt"
			openedFile, _ = os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
		}

		io.WriteString(openedFile, line + "\n")
	}
}