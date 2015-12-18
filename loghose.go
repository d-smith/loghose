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


type streamContext struct {
	Region string
	StreamName string
}


func fileNameFromArgs() (string, *streamContext, error) {
	if len(os.Args) != 3 {
		return "",nil, errors.New("Usage: loghost <stream name> <file to tail>")
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

	return os.Args[2], sc, nil
}


func writeLineToFirehose(svc *kinesis.Kinesis, streamName string, line string) {

	partitionKey := strconv.FormatInt(time.Now().UnixNano(), 10)

	params := &kinesis.PutRecordInput{
		Data:                      []byte(line),
		PartitionKey:              aws.String(partitionKey),
		StreamName:                aws.String(streamName),
	}
	resp, err := svc.PutRecord(params)

	if err != nil {
		// Print the error, cast err to awserr.Error to get the Code and
		// Message from an error.
		fmt.Println(err.Error())
		return
	}

	// Pretty-print the response data.
	fmt.Println(resp)
}


func main() {
	fileToTail, sc, err := fileNameFromArgs()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		os.Exit(1)
	}

	svc := kinesis.New(session.New(),&aws.Config{Region: aws.String(sc.Region)})

	t, err := tail.TailFile(fileToTail, tail.Config{Follow: true})
	for line := range t.Lines {
		//fmt.Println(line.Text)
		writeLineToFirehose(svc, sc.StreamName, line.Text)
	}
}
