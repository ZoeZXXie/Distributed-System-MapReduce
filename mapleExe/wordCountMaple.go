package main

import(
	log "github.com/sirupsen/logrus"
	"bufio"
	"io"
	"os"
	"regexp"
	"strings"
)

func main() { 
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

	reg, _ := regexp.Compile("[^A-Za-z0-9]+")
	scanner := bufio.NewScanner(inputFileDes)
	scanner.Split(bufio.ScanWords)

	for scanner.Scan()  {
		word := strings.TrimSpace(reg.ReplaceAllString(scanner.Text(),""))
		if len(word) > 0 {
			wordMapping := strings.ToLower(word) + " 1\n"
			_, err = io.WriteString(outputFileDes, wordMapping)
		}
	}
}