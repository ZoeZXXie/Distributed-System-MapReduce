package main

import(
	log "github.com/sirupsen/logrus"
	"bufio"
	"io"
	"os"
	"strings"
)

func main() { 
	// web-graph reversed - mapper
	// input format: fromNodeId \t ToNodeId
	// for each input line, swap <k,v> -> <v,k> 
	inputFileName := os.Args[1]
	outputFileName := os.Args[2]
	inputFileDes, err := os.Open(inputFileName)
	if err != nil {
		log.Fatalf("Could not open input file!", err)
		return
	}
	defer inputFileDes.Close()

	outputFileDes, err := os.OpenFile(outputFileName, os.O_RDWR | os.O_CREATE | os.O_TRUNC, 0666)
	if err != nil {
		log.Fatalf("Could not open output file", err)
	}
	defer outputFileDes.Close()

	scanner := bufio.NewScanner(inputFileDes)
	for scanner.Scan()  {
		nodes := strings.Fields(scanner.Text())
		if len(nodes) > 1{
			//reduce number of key (intermediate files) - use prefix as tmp key
			var tmpKey string
			if len(nodes[1]) <= 3{
				tmpKey = nodes[1]
			} else {
				tmpKey = nodes[1][:3]
			}
			reversedLink := tmpKey + " " + nodes[1] + " " + nodes[0] + "\n"
			_, err = io.WriteString(outputFileDes, reversedLink)
		} 
	}
}