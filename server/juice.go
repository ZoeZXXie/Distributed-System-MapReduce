package server

import (
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"os"
	"os/exec"
	"runtime"
)

var JUICE_EXE_FOLDER_NAME string = "mapleExe"
var JUICE_OUTPUT_FILE_NAME string = "reducerOutputFile.txt"

func JuiceWorkerManager() {
	hostname, _ := os.Hostname()
	
	for {
		// If there is no mapleJuice request or if the top request is not a maple request
		if len(Membership.MJQueue) == 0 || Membership.MJQueue[0].Command != "Juice" {
			runtime.Gosched()
			continue
		}

		workerCount := GetWorkerCount()
		if (hostname == Membership.List[0] || hostname > Membership.List[workerCount]) {
			runtime.Gosched()
			continue
		}

		// Make RPC call to get files
		os.Remove(Membership.MJQueue[0].FileDirectory)
		saveName := Membership.MJQueue[0].FileDirectory
		fileList := CallRequestJuiceFilesRPC(Membership.List[0], hostname)
		if len(fileList) == 0 {
			runtime.Gosched()
			continue
		}

		exePath := getExePath(Membership.MJQueue[0].ExeName)
		runReducer(exePath, fileList, saveName)
		sendOutputFile(saveName)
	}
}

// Gets the path of the exe, will fetch it if it doesnt exist
func getExePath(exeName string) (string) {
	exePath := JUICE_EXE_FOLDER_NAME + "/" + exeName
	if _, err := os.Stat(exePath); os.IsNotExist(err) {
		FetchFile(exeName, JUICE_EXE_FOLDER_NAME)
	}

	return exePath
}

// Runs the reducer for all the files in the filelist
func runReducer(exePath string, fileList []string, saveName string) {
	for _, fileName := range fileList {
		filePath := MAPLE_TEMP_FOLDER_NAME + "/" + fileName

		mapCommand := exec.Command("go", "run", exePath, filePath, saveName)
		err := mapCommand.Run()
		if err != nil {
			log.Infof("Error %s", err)
		}
	}
}

// Sends the data to the master to append to the final output file
func sendOutputFile(saveName string) {
	fileContents, _ := ioutil.ReadFile(saveName)
	CallAppendResultRPC(Membership.List[0], fileContents)
}