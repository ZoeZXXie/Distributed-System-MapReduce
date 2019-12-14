package main

import(
	"os"
	log "github.com/sirupsen/logrus"
	"bufio"
	"io"
	"strings"
)

func main() {
	// Juice exe. Reducer for reverse graph
	// Need 3 args:
	// 0 -> key : should be string type
	// 1 -> input file (path)
	// 2 -> output file (path)
	if len(os.Args) < 4 {
		log.Fatal("User did not specify enough arguments")
	}
	//key := os.Args[1]
	inputFileName := os.Args[2]
	outputFileName := os.Args[3]
	inputFileDes, err := os.Open(inputFileName)
	if err != nil {
		log.Fatalf("Could not open input file!", err)
	}
	defer inputFileDes.Close()

	outputFileDes, err := os.OpenFile(outputFileName, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Could not open output file", err)
	}
	defer outputFileDes.Close()

	fileScanner := bufio.NewScanner(inputFileDes)
	for fileScanner.Scan() {
		nodes := strings.Fields(fileScanner.Text())
		content := nodes[1] + "\t" + nodes[2] + "\n"
		_, err = io.WriteString(outputFileDes, content)
		if err != nil {
			log.Fatalf("Error during writing juice result:", err)
		}
	}
}