package main

import (
	"bufio"
	"cs-425-mp4/server"
	log "github.com/sirupsen/logrus"
	"os"
	"strings"
	"time"
)

func main() {
	hostname, _ := os.Hostname()

	// Start a goroutine to handle sending out heartbeats
	go server.HeartbeatManager()
	time.Sleep(1 * time.Second)
	
	go server.FileSystemManager()
	go server.MapleWorkerManager()
	go server.JuiceWorkerManager()
	go server.MapleMasterManager()
	go server.JuiceMasterManager()
	
	reader := bufio.NewReader(os.Stdin)
	for {
		input, _ := reader.ReadString('\n')
		input = strings.TrimSuffix(input, "\n")

		switch input {
		case "id":
			log.Infof("Current node ID: %s", hostname)
		case "list":
			log.Infof("Current list:\n%s", server.Membership.List)
		case "store":
			fileList := []string{}
			for fileName, _ := range server.LocalFiles.Files {
				fileList = append(fileList, fileName)
			}
			log.Infof("Files stored in the server:\n%s", fileList)
		case "leave":
			log.Infof("Node %s is leaving the network!", hostname)
			server.Membership.Data[hostname] = -1 * (time.Now().UnixNano() / int64(time.Millisecond))
			time.Sleep(2 * time.Second)
			os.Exit(0)
		default:
			log.Infof("Command \"%s\" not recognized", input)
		}
	}
}
