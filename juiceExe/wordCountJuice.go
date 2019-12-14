package main

import(
	"os"
	log "github.com/sirupsen/logrus"
	"bufio"
	"io"
	"strconv"
	"strings"
)

func main() {
	inputFileName := os.Args[1]
	outputFileName := os.Args[2]

	inputFileDes, err := os.Open(inputFileName)
	if err != nil {
		log.Fatalf("Could not open input file!", err)
	}
	defer inputFileDes.Close()

	// Assume that one line in input file means one count <key,1>
	fileScanner := bufio.NewScanner(inputFileDes)

	fileScanner.Scan()
	firstLine := fileScanner.Text()
	key := strings.Split(firstLine, " ")[0]
	val := 1

	for fileScanner.Scan() {
		val++
	}

	outputFileDes, _ := os.OpenFile(outputFileName, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
	io.WriteString(outputFileDes, (key + " " + strconv.Itoa(val) + "\n"))
	outputFileDes.Close()
}