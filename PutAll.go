package main

import (
	"bufio"
	"fmt"
	"github.com/mitchellh/goamz/aws"
	"github.com/mitchellh/goamz/s3"
	"net/http"
	"os"
)

func main() {
	AWSAuth := aws.Auth{
		AccessKey: "", // change this to yours
		SecretKey: "",
	}

	region := aws.USEast
	// change this to your AWS region
	// click on the bucketname in AWS control panel and click Properties
	// the region for your bucket should be under "Static Website Hosting" tab

	connection := s3.New(AWSAuth, region)

	bucket := connection.Bucket("") 		// change this your bucket name
	s3path := "example/somebigfile" 		// this is the target file and location in S3
	fileToBeUploaded := "somebigfile" 	// AWS recommends multipart upload for file bigger than 100MB

	file, err := os.Open(fileToBeUploaded)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	defer file.Close()

	fileInfo, _ := file.Stat()
	var fileSize int64 = fileInfo.Size()
	bytes := make([]byte, fileSize)

	// read into buffer
	buffer := bufio.NewReader(file)
	_, err = buffer.Read(bytes)

	// then we need to determine the file type
	// see https://www.socketloop.com/tutorials/golang-how-to-verify-uploaded-file-is-image-or-allowed-file-types

	filetype := http.DetectContentType(bytes)

	// set up for multipart upload
	multi, err := bucket.InitMulti(s3path, filetype, s3.ACL("public-read"))

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// this is for PutPart ( see https://godoc.org/launchpad.net/goamz/s3#Multi.PutPart)

	// calculate the number of parts by dividing up the file size by 5MB
	const fileChunk = 5242880 // 5MB in bytes

	parts, err := multi.PutAll(file, fileChunk)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	err = multi.Complete(parts)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	fmt.Println("PutAll upload completes")

}