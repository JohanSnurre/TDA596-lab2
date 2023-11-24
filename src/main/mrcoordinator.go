package main

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//

import (
	"fmt"
	"os"

	"time"

	"6.5840/mr"
)

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}

	m := mr.MakeCoordinator(os.Args[1:], 10)
	for m.Done() == false {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)

	//sess := session.Must(session.NewSession())

	/*sess, err := session.NewSession(&aws.Config{
		Region: aws.String("us-east-1")},
	)
	if err != nil {
		panic(err)
	}

	svc := s3.New(sess)

	result, err := svc.ListBuckets(nil)
	if err != nil {
		exitErrorf("Unable to list buckets, %v", err)

	}
	fmt.Println("Buckets:")

	for _, b := range result.Buckets {
		fmt.Printf("* %s created on %s\n", aws.StringValue(b.Name), aws.TimeValue(b.CreationDate))
	}

	uploader := s3manager.NewUploader(sess)

	file := "pg-grimm.txt"

	f, err := os.Open(file)
	if err != nil {
		fmt.Printf("Failed to open file %q %v", file, err)
		return
	}

	res, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String("tda596-group35-lab2-bucket"),
		Key:    aws.String(file),
		Body:   f,
	})

	/*input := &s3.DeleteObjectInput{
		Bucket: aws.String("tda596-group35-lab2-bucket"),
		Key:    aws.String("pg-grimm.txt"),
	}

	out, err := svc.DeleteObject(input)

	if err != nil {
		fmt.Println("WRONG")
		return
	}

	fmt.Printf("Deletet file from , %b\n", res.Location)
	*/

}
