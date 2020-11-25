/*
Multipart: first, just do something basic, where we use the s3manager to handle the MPU requests for us.  The pro: less code.  The con:
for objects which are extremely large in size (eg: >100GB), we don't have enough RAM to be able to put the object in memory before splitting it.

s3manager example: https://stackoverflow.com/questions/34177137/stream-file-upload-to-aws-s3-using-go


In order to do the 'proper' multipart test, perhaps use an example from here: https://github.com/apoorvam/aws-s3-multipart-upload/blob/master/aws-multipart-upload.go , where we:

1.  use the bufferBytes for the 'part' content, not the object
2.  dispatch the part-requests to the client queue for completion
3.  once all responses for a given object come back, perform the completion call
4.  measure the time for the total upload in order to calculate the b/w (it may not be valid to calc the runtime for each part)

*/
package main

import (
	"bytes"
	"crypto/rand"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	//"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

const (
	opRead  = "Read"
	opWrite = "Write"
	//max that can be deleted at a time via DeleteObjects()
	commitSize = 1000
)

var bufferBytes []byte

func main() {

	optypes := []string{"read", "write", "both"}
	operationListString := strings.Join(optypes[:], ", ")

	endpoint := flag.String("endpoint", "", "S3 endpoint(s) comma separated - http://IP:PORT,http://IP:PORT")
	region := flag.String("region", "vast-west", "AWS region to use, eg: us-west-1|us-east-1, etc")
	accessKey := flag.String("accessKey", "", "the S3 access key")
	accessSecret := flag.String("accessSecret", "", "the S3 access secret")
	operations := flag.String("operations", "write", "ops:"+operationListString)
	multipart := flag.Bool("multipart", false, "perform multipart uploads")
	partSize := flag.Int64("partSize", 500*1024*1024, "size for MPU parts")
	multiUploaders := flag.Int("multiUploaders", 5, "number of MPU uploaders per client..this is multiplicative with numClients..")
	bucketName := flag.String("bucket", "bucketname", "the bucket for which to run the test")
	objectNamePrefix := flag.String("objectNamePrefix", "loadgen_test_", "prefix of the object name that will be used")
	objectSize := flag.Int64("objectSize", 80*1024*1024, "size of individual requests in bytes (must be smaller than main memory)")
	numClients := flag.Int("numClients", 40, "number of concurrent clients")
	batchSize := flag.Int("batchSize", 1000, "per-prefix batchsize")
	numSamples := flag.Int("numSamples", 200, "total number of requests to send")
	skipCleanup := flag.Bool("skipCleanup", false, "skip deleting objects created by this tool at the end of the run")
	verbose := flag.Bool("verbose", false, "print verbose per thread status")
	disableMD5 := flag.Bool("disableMD5", true, "for v4 auth: disable source md5sum calcs (faster)")

	flag.Parse()

	if *numClients > *numSamples || *numSamples < 1 {
		fmt.Printf("numClients(%d) needs to be less than numSamples(%d) and greater than 0\n", *numClients, *numSamples)
		os.Exit(1)
	}

	if *endpoint == "" {
		fmt.Println("You need to specify endpoint(s)")
		flag.PrintDefaults()
		os.Exit(1)
	}

	var opTypeExists = false
	for op := range optypes {
		if optypes[op] == *operations {
			opTypeExists = true
		}
	}

	if !opTypeExists {
		fmt.Printf("operation type must be one of: %s \n", operationListString)
		os.Exit(1)
	}

	//os.Exit(3)
	// Generate the data from which we will do the writting

	// Setup and print summary of the accepted parameters
	params := Params{
		requests:         make(chan Req),
		responses:        make(chan Resp),
		numSamples:       *numSamples,
		batchSize:        *batchSize,
		numClients:       uint(*numClients),
		objectSize:       *objectSize,
		objectNamePrefix: *objectNamePrefix,
		bucketName:       *bucketName,
		endpoints:        strings.Split(*endpoint, ","),
		operations:       *operations,
		multipart:        *multipart,
		partSize:         *partSize,
		multiUploaders:   *multiUploaders,
		disableMD5:       *disableMD5,

		verbose: *verbose,
	}
	fmt.Println(params)
	fmt.Println()

	fmt.Printf("Generating in-memory sample data... ")
	timeGenData := time.Now()

	bufferBytes = make([]byte, *objectSize, *objectSize)
	_, err := rand.Read(bufferBytes)
	if err != nil {
		fmt.Printf("Could not allocate a buffer")
		os.Exit(1)
	}

	fmt.Printf("Done (%s)\n", time.Since(timeGenData))
	fmt.Println()

	// Start the load clients and run a write test followed by a read test

	var httpClient = &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}

	cfg := &aws.Config{
		Credentials:                   credentials.NewStaticCredentials(*accessKey, *accessSecret, ""),
		Region:                        aws.String(*region),
		S3ForcePathStyle:              aws.Bool(true),
		S3DisableContentMD5Validation: aws.Bool(params.disableMD5),
		Endpoint:                      aws.String(params.endpoints[0]),
		HTTPClient:                    httpClient,
	}

	// make a bucket first

	log.Println("creating bucket if required")

	sess := session.New(cfg)

	s3client := s3.New(sess)

	cparams := &s3.CreateBucketInput{
		Bucket: bucketName, // Required
	}
	if _, derr := s3client.CreateBucket(cparams); derr != nil && !isBucketAlreadyOwnedByYouErr(derr) {
		log.Fatal(derr)
	}

	params.StartClients(cfg)

	/*

		here's what we'll try to do.
		1.  allow choice of write, read (maybe later list, etc)
		2.  if write only, just skip doing reads
		3.  if both, then just do writes and then reads
		4.  if reads: then we have to first check if there is existing data in the bucket/prefix we can use.

	*/

	if strings.Contains(params.operations, "write") {

		fmt.Printf("Running %s test...\n", opWrite)
		writeResult := params.Run(opWrite)
		fmt.Println(params)
		fmt.Println(writeResult)
		fmt.Println()

	} else if strings.Contains(params.operations, "read") {

		fmt.Printf("Running %s test...\n", opRead)
		readResult := params.Run(opRead)
		fmt.Println(params)
		fmt.Println(readResult)

		fmt.Println()

	} else if strings.Contains(params.operations, "both") {
		fmt.Printf("Running %s test...\n", opWrite)
		writeResult := params.Run(opWrite)

		fmt.Printf("Running %s test...\n", opRead)
		readResult := params.Run(opRead)
		fmt.Println(writeResult)
		fmt.Println()
		fmt.Println(readResult)
		fmt.Println()

	}

	// Do cleanup if required
	if !*skipCleanup {
		fmt.Println()
		fmt.Printf("Cleaning up %d objects...\n", *numSamples)
		delStartTime := time.Now()
		svc := s3.New(session.New(), cfg)

		numSuccessfullyDeleted := 0

		keyList := make([]*s3.ObjectIdentifier, 0, commitSize)
		for i := 0; i < *numSamples; i++ {
			bar := s3.ObjectIdentifier{
				Key: aws.String(fmt.Sprintf("%s%d", *objectNamePrefix, i)),
			}
			keyList = append(keyList, &bar)
			if len(keyList) == commitSize || i == *numSamples-1 {
				fmt.Printf("Deleting a batch of %d objects in range {%d, %d}... ", len(keyList), i-len(keyList)+1, i)
				params := &s3.DeleteObjectsInput{
					Bucket: aws.String(*bucketName),
					Delete: &s3.Delete{
						Objects: keyList}}
				_, err := svc.DeleteObjects(params)
				if err == nil {
					numSuccessfullyDeleted += len(keyList)
					fmt.Printf("Succeeded\n")
				} else {
					fmt.Printf("Failed (%v)\n", err)
				}
				//set cursor to 0 so we can move to the next batch.
				keyList = keyList[:0]

			}
		}
		fmt.Printf("Successfully deleted %d/%d objects in %s\n", numSuccessfullyDeleted, *numSamples, time.Since(delStartTime))
	}
}

func isBucketAlreadyOwnedByYouErr(err error) bool {
	if aErr, ok := err.(awserr.Error); ok {
		return aErr.Code() == s3.ErrCodeBucketAlreadyOwnedByYou
	}
	return false
}

func (params *Params) Run(op string) Result {
	startTime := time.Now()

	// Start submitting load requests
	go params.submitLoad(op)

	// Collect and aggregate stats for completed requests
	result := Result{opDurations: make([]float64, 0, params.numSamples), operation: op}
	for i := 0; i < params.numSamples; i++ {
		resp := <-params.responses
		errorString := ""
		if resp.err != nil {
			result.numErrors++
			errorString = fmt.Sprintf(", error: %s", resp.err)
		} else {
			result.bytesTransmitted = result.bytesTransmitted + params.objectSize
			result.opDurations = append(result.opDurations, resp.duration.Seconds())
		}
		if params.verbose {
			fmt.Printf("%v operation completed in %0.2fs (%d/%d) - %0.2fMB/s%s\n",
				op, resp.duration.Seconds(), i+1, params.numSamples,
				(float64(result.bytesTransmitted)/(1024*1024))/time.Since(startTime).Seconds(),
				errorString)
		}
	}

	result.totalDuration = time.Since(startTime)
	sort.Float64s(result.opDurations)
	return result
}

// Create an individual load request and submit it to the client queue
func (params *Params) submitLoad(op string) {
	bucket := aws.String(params.bucketName)

	/*
		goal here is to divide up all the requests so that there are no more than $batchSize per prefix-$num/ .
		EG: if the prefix is 'andy' , then we want to make $(numsamples / $batchSize) sub-prefixes , so that the result is:
		bucket/andy/1/
		bucket/andy/2/
		etc
		That way we can fan out the objects more.
	*/
	keyList := make([]*string, 0, params.batchSize)
	bigList := make([]*string, 0, params.numSamples)
	numDirs := 0
	for i := 0; i < params.numSamples; i++ {
		bar := string(i)
		keyList = append(keyList, &bar)
		if len(keyList) == params.batchSize || i == params.numSamples-1 {

			for j := 0; j < len(keyList); j++ {
				pref := params.objectNamePrefix + "/" + strconv.Itoa(numDirs) + "/"
				key := aws.String(fmt.Sprintf("%s%d", pref, j))
				bigList = append(bigList, key)

			}

			//increment the dirname
			numDirs++
			//set cursor to 0 so we can move to the next batch.
			keyList = keyList[:0]

		}
	}

	// now actually submit the load.

	for f := 0; f < len(bigList); f++ {
		if op == opWrite {

			if params.multipart {
				params.requests <- &s3manager.UploadInput{
					Bucket: bucket,
					Key:    bigList[f],
					Body:   bytes.NewReader(bufferBytes),
				}

			} else {
				params.requests <- &s3.PutObjectInput{
					Bucket: bucket,
					Key:    bigList[f],
					Body:   bytes.NewReader(bufferBytes),
				}
			}
		} else if op == opRead {
			params.requests <- &s3.GetObjectInput{
				Bucket: bucket,
				Key:    bigList[f],
			}
		} else {
			panic("Developer error")
		}
	}

}

func (params *Params) StartClients(cfg *aws.Config) {
	for i := 0; i < int(params.numClients); i++ {
		cfg.Endpoint = aws.String(params.endpoints[i%len(params.endpoints)])
		go params.startClient(cfg)
		time.Sleep(1 * time.Millisecond)
	}
}

// Run an individual load request
func (params *Params) startClient(cfg *aws.Config) {

	if params.multipart {
		sess, err := session.NewSession(cfg)
		if err != nil {
			panic("bad session setup?")
		}
		for request := range params.requests {

			putStartTime := time.Now()
			var err error
			numBytes := params.objectSize

			switch r := request.(type) {
			case *s3manager.UploadInput:
				//https://github.com/awsdocs/aws-doc-sdk-examples/blob/master/go/example_code/s3/s3_upload_object.go
				uploader := s3manager.NewUploader(sess, func(u *s3manager.Uploader) {
					u.PartSize = params.partSize
					u.Concurrency = params.multiUploaders
				})

				_, err := uploader.Upload(r)

				if err != nil {
					fmt.Printf("error uploading: %v", err)
				}

			case *s3.GetObjectInput:
				//https://github.com/awsdocs/aws-doc-sdk-examples/blob/master/go/example_code/s3/s3_download_object.go
				downloader := s3manager.NewDownloader(sess, func(u *s3manager.Downloader) {
					u.PartSize = params.partSize
					u.Concurrency = params.multiUploaders
				})
				numBytes = 0
				file, openErr := os.OpenFile("/dev/null", os.O_WRONLY, 644) //perhaps there's a better way, but this seems to work.
				if openErr != nil {
					panic("woops")
				}

				numBytes, err := downloader.Download(file,
					r)
				if err != nil {
					fmt.Printf("error: %v", err)
				}

				if numBytes != params.objectSize {
					err = fmt.Errorf("expected object length %d, actual %d", params.objectSize, numBytes)
				}
			default:
				panic("Developer error")
			}

			params.responses <- Resp{err, time.Since(putStartTime), numBytes}
		}
	} else {

		svc := s3.New(session.New(), cfg)
		for request := range params.requests {
			putStartTime := time.Now()
			var err error
			numBytes := params.objectSize

			switch r := request.(type) {
			case *s3.PutObjectInput:

				req, _ := svc.PutObjectRequest(r)
				// below line shouldn't be required to do the flag in the session setup, but keeping it in case.
				req.HTTPRequest.Header.Add("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
				err = req.Send()

			case *s3.GetObjectInput:
				req, resp := svc.GetObjectRequest(r)
				err = req.Send()
				numBytes = 0
				if err == nil {
					numBytes, err = io.Copy(ioutil.Discard, resp.Body)
				}
				if numBytes != params.objectSize {
					err = fmt.Errorf("expected object length %d, actual %d", params.objectSize, numBytes)
				}
			default:
				panic("Developer error")
			}

			params.responses <- Resp{err, time.Since(putStartTime), numBytes}
		}
	}
}

// Specifies the parameters for a given test
type Params struct {
	operation        string
	operations       string
	requests         chan Req
	responses        chan Resp
	numSamples       int
	batchSize        int
	numClients       uint
	objectSize       int64
	partSize         int64
	multiUploaders   int
	multipart        bool
	objectNamePrefix string
	bucketName       string
	endpoints        []string
	verbose          bool
	disableMD5       bool
}

func (params Params) String() string {
	output := fmt.Sprintln("Test parameters")
	output += fmt.Sprintf("endpoint(s):      %s\n", params.endpoints)
	output += fmt.Sprintf("bucket:           %s\n", params.bucketName)
	output += fmt.Sprintf("objectNamePrefix: %s\n", params.objectNamePrefix)
	output += fmt.Sprintf("objectSize:       %0.4f MB\n", float64(params.objectSize)/(1024*1024))
	output += fmt.Sprintf("numClients:       %d\n", params.numClients)
	output += fmt.Sprintf("numSamples:       %d\n", params.numSamples)
	output += fmt.Sprintf("batchSize:       %d\n", params.batchSize)
	output += fmt.Sprintf("verbose:       %d\n", params.verbose)
	return output
}

// Contains the summary for a given test result
type Result struct {
	operation        string
	bytesTransmitted int64
	numErrors        int
	opDurations      []float64
	totalDuration    time.Duration
}

func (r Result) String() string {
	report := fmt.Sprintf("Results Summary for %s Operation(s)\n", r.operation)
	report += fmt.Sprintf("Total Transferred: %0.3f MB\n", float64(r.bytesTransmitted)/(1024*1024))
	report += fmt.Sprintf("Total Throughput:  %0.2f MB/s\n", (float64(r.bytesTransmitted)/(1024*1024))/r.totalDuration.Seconds())
	report += fmt.Sprintf("Total Duration:    %0.3f s\n", r.totalDuration.Seconds())
	report += fmt.Sprintf("Number of Errors:  %d\n", r.numErrors)
	if len(r.opDurations) > 0 {
		report += fmt.Sprintln("------------------------------------")
		report += fmt.Sprintf("%s times Max:       %0.3f s\n", r.operation, r.percentile(100))
		report += fmt.Sprintf("%s times 99th %%ile: %0.3f s\n", r.operation, r.percentile(99))
		report += fmt.Sprintf("%s times 90th %%ile: %0.3f s\n", r.operation, r.percentile(90))
		report += fmt.Sprintf("%s times 75th %%ile: %0.3f s\n", r.operation, r.percentile(75))
		report += fmt.Sprintf("%s times 50th %%ile: %0.3f s\n", r.operation, r.percentile(50))
		report += fmt.Sprintf("%s times 25th %%ile: %0.3f s\n", r.operation, r.percentile(25))
		report += fmt.Sprintf("%s times Min:       %0.3f s\n", r.operation, r.percentile(0))
	}
	return report
}

func (r Result) percentile(i int) float64 {
	if i >= 100 {
		i = len(r.opDurations) - 1
	} else if i > 0 && i < 100 {
		i = int(float64(i) / 100 * float64(len(r.opDurations)))
	}
	return r.opDurations[i]
}

type Req interface{}

type Resp struct {
	err      error
	duration time.Duration
	numBytes int64
}
