package main

import (
	"os"
	"errors"
	"fmt"
	"github.com/hpcloud/tail"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"time"
	"strconv"
)

var quiet = true

type streamContext struct {
	Region string
	StreamName string
}


func fileNameFromArgs() (string, *streamContext, bool, error) {
	if len(os.Args) < 4 {
		return "",nil, false, errors.New("Usage: loghost <stream name> <file to tail> verbose|quiet")
	}

	defaultRegion := "us-east-1"
	region := os.Getenv("AWS_REGION")
	if region == "" {
		fmt.Println("No AWS_REGION environment variable found - defaulting to",defaultRegion)
		region = defaultRegion
	}

	sc := &streamContext {
		Region: region,
		StreamName: os.Args[1],
	}

	return os.Args[2], sc, os.Args[3] == "verbose", nil
}


func writeLineToFirehose(svc *kinesis.Kinesis, streamName string, line string) {

	partitionKey := strconv.FormatInt(time.Now().UnixNano(), 10)

	params := &kinesis.PutRecordInput{
		Data:                      []byte(line),
		PartitionKey:              aws.String(partitionKey),
		StreamName:                aws.String(streamName),
	}
	_, err := svc.PutRecord(params)

	if err != nil {
		// Print the error, cast err to awserr.Error to get the Code and
		// Message from an error.
		fmt.Println("error: " + err.Error())
		return
	}

	// Pretty-print the response data.
	if quiet == false {
		fmt.Println("data sent")
	}
}


func main() {
	fileToTail, sc, verbose, err := fileNameFromArgs()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		os.Exit(1)
	}

	if verbose {
		quiet = false
	}

	svc := kinesis.New(session.New(),&aws.Config{Region: aws.String(sc.Region)})

	t, err := tail.TailFile(fileToTail, tail.Config{Follow: true})
	for line := range t.Lines {
		go func (line string) {
			if quiet == false {
				fmt.Println("send: ", line)
			}
			writeLineToFirehose(svc, sc.StreamName, line)
		}(line.Text)
	}
}
