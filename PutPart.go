package main

import (
  "bufio"
  "fmt"
  "github.com/mitchellh/goamz/aws"
  "github.com/mitchellh/goamz/s3"
  "flag"
  "math"
  "net/http"
  "os"
)

// note, that variables are pointers
var awsKey = flag.String("long-string", "", "Description")

func init() {
  flag.StringVar(awsKey, "k", "key", "Description")
}

func main() {
  flag.Parse()
  println(*awsKey)

  AWSAuth := aws.Auth{
    AccessKey: "", // change this to yours
    SecretKey: "",
  }

  region := aws.USEast
  // change this to your AWS region
  // click on the bucketname in AWS control panel and click Properties
  // the region for your bucket should be under "Static Website Hosting" tab

  connection := s3.New(AWSAuth, region)


  /**
   * TODO: Will be replaced with stdin later
   */

  bucket := connection.Bucket("")     // change this your bucket name
  s3path := "example/somebigfile"     // this is the target file and location in S3
  fileToBeUploaded := "somebigfile"   // AWS recommends multipart upload for file bigger than 100MB

  /**
   * NOTE: If the filesize is smaller than 5MB ( as defined in fileChunk below) you will get this error message:
   * -->   The XML you provided was not well-formed or did not validate against our published schema
   */

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

  // how many parts to process ??
  totalPartsNum := uint64(math.Ceil(float64(fileSize) / float64(fileChunk)))

  parts := []s3.Part{} // collect all the parts for upload completion

  fmt.Println("Uploading...")

  for i := uint64(1); i < totalPartsNum; i++ {

    partSize := int(math.Min(fileChunk, float64(fileSize-int64(i*fileChunk))))
    partBuffer := make([]byte, partSize)

    file.Read(partBuffer)
    part, err := multi.PutPart(int(i), file) // write to S3 bucket part by part
    fmt.Printf("Sending %d part of %d and uploaded %d bytes.\n ", int(i), int(totalPartsNum), int(part.Size))

    parts = append(parts, part)

    if err != nil {
      fmt.Printf("Uploading parts of file error :i %s\n ", err)
      os.Exit(1)
    }
  }

  err = multi.Complete(parts)

  if err != nil {
    fmt.Println("Complete parts error %s\n", err)
    os.Exit(1)
  }

  fmt.Println("\n\nPutPart upload completed")
}
