package server

import (
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"math"
	"net"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

var UDP_PORT_NUM string = "4000"
var NODE_FAIL_TIMEOUT int64 = 4000
var INTRODUCER_NODE string = "fa19-cs425-g84-01.cs.illinois.edu"

// Need to store extra list that maintains order or the list
type MembershipList struct {
	SrcHost string
	Data    map[string]int64
	List    []string

	RequestUTime int64
	Pending      []*Request

	MapleJuiceUTime int64
	MJQueue         []*MapleJuiceRequest
}

// We need to make this a global so RPC can access it
var Membership *MembershipList

// Goroutine that will send out heartbeats every half second.
func HeartbeatManager() {
	serverStartup()
	go listenForUDP()

	hostname, _ := os.Hostname()
	ticker := time.NewTicker(1000 * time.Millisecond)
	var wg sync.WaitGroup

	for {
		<-ticker.C

		wg.Add(1)
		go sendHeartbeats(&wg)
		wg.Wait()

		// If the current node has not left the network then update its time
		if Membership.Data[hostname] > 0 {
			Membership.Data[hostname] = time.Now().UnixNano() / int64(time.Millisecond)
		}

		removeExitedNodes()
	}
}

// Server setup that will make initial connections and set global Membership
func serverStartup() {
	hostname, _ := os.Hostname()

	var requests []*Request
	var mapleJuiceQueue []*MapleJuiceRequest
	currTime := time.Now().UnixNano() / int64(time.Millisecond)
	Membership = &MembershipList{
		SrcHost:         hostname,
		Data:            map[string]int64{hostname: time.Now().UnixNano() / int64(time.Millisecond)},
		List:            []string{hostname},
		RequestUTime:    currTime,
		Pending:         requests,
		MapleJuiceUTime: currTime,
		MJQueue:         mapleJuiceQueue,
	}

	if hostname != INTRODUCER_NODE {
		log.Info("Writing to the introducer!")
		writeMembershipList(INTRODUCER_NODE)

	} else {
		log.Info("Introducer attempting to reconnect to any node!")
		for i := 2; i <= 10; i++ {
			numStr := strconv.Itoa(i)
			if len(numStr) == 1 {
				numStr = "0" + numStr
			}

			connectName := "fa19-cs425-g84-" + numStr + ".cs.illinois.edu"
			writeMembershipList(connectName)
		}
	}
}

// Function called by the heartbeat manager that will send heartbeats to neighbors
func sendHeartbeats(wg *sync.WaitGroup) {
	hostName, _ := os.Hostname()
	index := findHostnameIndex(hostName)
	lastIndex := index

	// Sends two heartbeats to its immediate successors
	for i := 1; i <= 2; i++ {
		nameIndex := (index + i) % len(Membership.List)
		lastIndex = nameIndex
		if nameIndex == index {
			break
		}

		go writeMembershipList(Membership.List[nameIndex])
	}

	// Sends two heartbeats to its immediate predecessors
	for i := -1; i >= -2; i-- {
		nameIndex := (len(Membership.List) + index + i) % len(Membership.List)
		if lastIndex == index || nameIndex == lastIndex {
			break
		}

		go writeMembershipList(Membership.List[nameIndex])
	}

	wg.Done()
}

// This writes the Membership lists to the socket which is called by sendHeartbeats
func writeMembershipList(hostName string) {
	conn, err := net.Dial("udp", hostName+":"+UDP_PORT_NUM)
	if err != nil {
		log.Fatal("Could not connect to node! %s", err)
	}
	defer conn.Close()

	memberSend, err := json.Marshal(Membership)
	if err != nil {
		log.Fatal("Could not encode message %s", err)
		return
	}

	conn.Write(memberSend)
}

// Loops through the Membership and checks if any of the times on the nodes are past the
// allowed timeout. If the last time is 0 then the node has left the network. We call this
// after we send heartbeats because if the current node leaves, it sets its Data map enty
// to 0 and must propogate this out to the other nodes.
func removeExitedNodes() {
	currTime := time.Now().UnixNano() / int64(time.Millisecond)
	tempList := Membership.List[:0]
	rootName, _ := os.Hostname()

	for _, hostName := range Membership.List {
		lastPing := Membership.Data[hostName]
		if lastPing < 0 {
			if hostName != rootName {
				log.Infof("Node %s left the network!", hostName)
			} else {
				tempList = append(tempList, hostName)
			}
		} else if !(currTime-lastPing > NODE_FAIL_TIMEOUT) {
			tempList = append(tempList, hostName)
		} else {
			log.Infof("Node %s timed out!", hostName)
		}
	}

	Membership.List = tempList
}

// Goroutine that constantly listens for incoming UDP calls
func listenForUDP() {
	socketUDP := openUDPConn()

	// This will fail when heartbeats grow beyond 1024 bytes
	buffer := make([]byte, 1024)
	for {
		readLen, _, err := socketUDP.ReadFromUDP(buffer)
		if err != nil {
			log.Infof("Encountered error while reading UDP socket! %s", err)
			continue
		}
		if readLen == 0 {
			continue
		}

		newMembership := &MembershipList{}
		err = json.Unmarshal(buffer[:readLen], &newMembership)
		if err != nil {
			log.Infof("Could not decode request! %s", err)
			return
		}

		processNewMembershipList(newMembership)
		if newMembership.RequestUTime > Membership.RequestUTime {
			Membership.Pending = newMembership.Pending
		}
		if newMembership.MapleJuiceUTime > Membership.MapleJuiceUTime {
			Membership.MJQueue = newMembership.MJQueue
		}
	}
}

// Helper method that will open a UDP connection
func openUDPConn() *net.UDPConn {
	hostname, _ := os.Hostname()
	addr, err := net.ResolveUDPAddr("udp", hostname+":"+UDP_PORT_NUM)
	if err != nil {
		log.Fatal("Could not resolve hostname!: %s", err)
	}
	socketUDP, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatal("Server could not set up UDP listener %s", err)
	}
	log.Infof("Connected to %s!", hostname)

	return socketUDP
}

// Loop through the new Membership and update the timestamps in the current node.
func processNewMembershipList(newMembership *MembershipList) {
	for i := 0; i < len(newMembership.List); i++ {
		nextHostname := newMembership.List[i]
		newPingTime := newMembership.Data[nextHostname]

		// If it finds a node that is not in the data map, add it to list and map
		if pingTime, contains := Membership.Data[nextHostname]; !contains {
			log.Infof("Added %s to membership list!", nextHostname)
			Membership.Data[nextHostname] = newPingTime

			Membership.List = append(Membership.List, nextHostname)
			sort.Strings(Membership.List)

			// If the new Membership has a more recent time, update it
		} else if math.Abs(float64(pingTime)) < math.Abs(float64(newPingTime)) {
			Membership.Data[nextHostname] = newPingTime

			// If the hostname is not in the list but it's in the data map,
			// it was removed from the list because it faile or left, and
			// we just recieved a new time indicating that it's rejoining.
			currTime := time.Now().UnixNano() / int64(time.Millisecond)
			if findHostnameIndex(nextHostname) >= len(Membership.List) &&
				currTime-newPingTime < NODE_FAIL_TIMEOUT {

				Membership.List = append(Membership.List, nextHostname)
				sort.Strings(Membership.List)
				log.Infof("Recieved updated time from node %s. Adding back to list", nextHostname)
			}

			// This will only happen if the node has left the network
		} else if newPingTime < 0 {
			Membership.Data[nextHostname] = -1 * newMembership.Data[nextHostname]
		}
	}
}

// Search that will find the index of the hostname in the list
func findHostnameIndex(hostName string) int {
	for i := 0; i < len(Membership.List); i++ {
		if Membership.List[i] == hostName {
			return i
		}
	}

	return len(Membership.List)
}
