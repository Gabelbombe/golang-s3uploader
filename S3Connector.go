package main

/**
 *
 *
 * REF : https://www.socketloop.com/tutorials/golang-download-file-example
 * REF : http://golang.org/pkg/io/#Copy
 * REF : http://godoc.org/launchpad.net/goamz/s3
 */

import (
  "bufio"
  "fmt"
  "io"
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
   bucket     := connection.Bucket("<your bucketname>") // change this your bucket name
   path       := "example/big.jpg"                      // this is the target file and location in S3

   // need to read big.jpg into a []byte buffer
   // see https://www.socketloop.com/tutorials/golang-read-binary-file-into-memory
   fileToBeUploaded := "big.jpg"

   file, err := os.Open(fileToBeUploaded)

   if err != nil {
     fmt.Println(err)
     os.Exit(1)
   }

   defer file.Close()

   fileInfo, _ := file.Stat()
   var size int64 = fileInfo.Size()
   bytes := make([]byte, size)

   // read into buffer
   buffer := bufio.NewReader(file)
   _, err = buffer.Read(bytes)

   // then we need to determine the file type
   // see https://www.socketloop.com/tutorials/golang-how-to-verify-uploaded-file-is-image-or-allowed-file-types
   filetype := http.DetectContentType(bytes)

   err = bucket.Put(path, bytes, filetype, s3.ACL("public-read"))

   if err != nil {
     fmt.Println(err)
     os.Exit(1)

    // NOTE : If you get this error message
    // Get : 301 response missing Location header

    // this is because you are using the wrong region for the bucket
    // and if you want to figure out the bucket location automatically
    // see http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGETlocation.html
    // I've try it out with http.Get() and just getting the authenticating
    // requests part right is already too much
    // work for this tutorial.
    // See http://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-authenticating-requests.html

    // UPDATE 15th Jan 2015: See http://camlistore.org/pkg/misc/amazon/s3/#Client.BucketLocation
   }

   fmt.Printf("Uploaded to %s with %v bytes to S3.\n\n", path, size)

   // Download(GET)
   downloadBytes, err := bucket.Get(path)

   if err != nil {
     fmt.Println(err)
     os.Exit(1)
   }

   downloadFile, err := os.Create("download.jpg")

   if err != nil {
     fmt.Println(err)
     os.Exit(1)
   }

   defer downloadFile.Close()

   downloadBuffer := bufio.NewWriter(downloadFile)
   downloadBuffer.Write(downloadBytes)

   io.Copy(downloadBuffer, downloadFile)

   fmt.Printf("Downloaded from S3 and saved to download.jpg. \n\n")

 }
