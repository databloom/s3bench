/*

*TODO:
0.  add logic to check if objectSize < MPU size (fallback to regular puts.)
1.  Make a 'runtime' test: where it will re-run over and over for the specified runtime
2.  have a 'simplified' output mode to dump only the relevant bits: so when running on many hosts it can be easier to aggregate.
3.  randomize better: have each 'put' use slightly different data:
	* maybe: create a buffer that is 2x as big as we need, then just read random offsets from it?


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
	mrand "math/rand"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

const (
	opRead  = "Read"
	opWrite = "Write"
)

var bufferBytes []byte

func main() {

	// really need to split this thing up into more functions one day.

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

	if *multipart && *partSize < 5242880 {
		fmt.Printf("multipart size must be at least 5242880 bytes, you specificied %v", *partSize)
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
		accessKey:        *accessKey,
		accessSecret:     *accessSecret,
		region:           *region,
		operations:       *operations,
		multipart:        *multipart,
		partSize:         *partSize,
		multiUploaders:   *multiUploaders,
		disableMD5:       *disableMD5,

		verbose: *verbose,
	}
	fmt.Println(params)
	fmt.Println()

	log.Println("creating bucket if required")

	svc := makeS3session(params, params.endpoints[0])

	makeBucket(bucketName, svc)

	/*
		function flow:
		1.  startClients -> loops through the -numClients and generates s3 sessions, then launches a go routine (startclient)
		1.5 : those go routines 'sit at the ready'
		2.  startclient : these are the 'at the ready' clients, ready to do work
		3.  params.Run : this launches a goroutine to run params.submitLoad
		4. params.submitload : calls params.makeKeyList to make a list of keys
		4.5 for each item in the keylist, it adds a request to the params.requests channel
		5.  Now we are back in 'startclient', where the work gets done.
		5.5 it loops through items in the params.request channel, and performs the actual work
		5.75 : results are put into the params.responses channel
		6.  The params.Run function collects the results, and returns them to the original (main) function.


	*/
	StartClients(params)
	/*

		1.  allow choice of write, read (maybe later list, etc)
		2.  if write only, just skip doing reads
		3.  if both, then just do writes and then reads
		4.  if reads: then we have to first check if there is existing data in the bucket/prefix we can use.

	*/

	if strings.Contains(params.operations, "write") {
		if *multipart {
			bufferBytes = makeData(*partSize)

		} else {
			bufferBytes = makeData(*objectSize)

		}

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
		if *multipart {
			bufferBytes = makeData(*partSize)

		} else {
			bufferBytes = makeData(*objectSize)

		}
		fmt.Printf("Running %s test...\n", opWrite)
		writeResult := params.Run(opWrite)

		fmt.Printf("Running %s test...\n", opRead)
		readResult := params.Run(opRead)
		fmt.Println(writeResult)
		fmt.Println()
		fmt.Println(readResult)
		fmt.Println()

	}

	if !*skipCleanup {
		params.cleanup(svc)

	}

}

//first, make a func which will create the session setup.

func makeS3session(params Params, endpoint string) *s3.S3 {
	var httpClient = &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}
	cfg := &aws.Config{
		Credentials:                   credentials.NewStaticCredentials(params.accessKey, params.accessSecret, ""),
		Region:                        aws.String(params.region),
		S3ForcePathStyle:              aws.Bool(true),
		S3DisableContentMD5Validation: aws.Bool(params.disableMD5),
		Endpoint:                      aws.String(endpoint),
		HTTPClient:                    httpClient,
	}
	sess := session.New(cfg)

	s3client := s3.New(sess)
	return s3client

}

// next, a function to create a bucket

func makeBucket(bucketName *string, svc *s3.S3) {
	cparams := &s3.CreateBucketInput{
		Bucket: bucketName, // Required
	}
	if _, derr := svc.CreateBucket(cparams); derr != nil && !isBucketAlreadyOwnedByYouErr(derr) {
		log.Fatal(derr)
	}

}

// function to generate data

func makeData(dataSize int64) []byte {
	fmt.Printf("Generating %v bytes in-memory sample data... ", dataSize)
	timeGenData := time.Now()

	bufferBytes = make([]byte, dataSize, dataSize)
	_, err := rand.Read(bufferBytes)
	if err != nil {
		fmt.Printf("Could not allocate a buffer")
		os.Exit(1)
	}
	fmt.Printf("Done (%s)\n", time.Since(timeGenData))
	fmt.Println()
	return bufferBytes

}

func (params *Params) cleanup(svc *s3.S3) {
	fmt.Println()
	fmt.Printf("Cleaning up %d objects...\n", params.numSamples)
	delStartTime := time.Now()

	numSuccessfullyDeleted := 0
	bigList := params.makeKeyList()

	keyList := make([]*s3.ObjectIdentifier, 0, params.batchSize)
	for i, key := range bigList {

		bar := s3.ObjectIdentifier{
			Key: key,
		}

		keyList = append(keyList, &bar)
		if len(keyList) == params.batchSize || i == params.numSamples-1 {
			fmt.Printf("Deleting a batch of %d objects in range {%d, %d}... ", len(keyList), i-len(keyList)+1, i)
			params := &s3.DeleteObjectsInput{
				Bucket: aws.String(params.bucketName),
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
		i++
	}
	fmt.Printf("Successfully deleted %d/%d objects in %s\n", numSuccessfullyDeleted, params.numSamples, time.Since(delStartTime))
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
			fmt.Printf("error: %s", resp.err)
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

func (params *Params) makeKeyList() []*string {
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
	return bigList
}

// Create an individual load request and submit it to the client queue
func (params *Params) submitLoad(op string) {
	bucket := aws.String(params.bucketName)
	bigList := params.makeKeyList()

	// now actually submit the load.
	// if we want to do manual multipart, perhaps we need a separate channel.

	for f := 0; f < len(bigList); f++ {
		if op == opWrite {

			if params.multipart {
				// once per object, there will be a separate setup for individual mpu's.
				params.requests <- &s3.CreateMultipartUploadInput{
					Bucket: bucket,
					Key:    bigList[f],
				}
			} else {
				//not multipart
				params.requests <- &s3.PutObjectInput{
					Bucket: bucket,
					Key:    bigList[f],
					Body:   bytes.NewReader(bufferBytes),
				}
			}
		} else if op == opRead {
			//note: we should also do a switch if we're using multipart, since it will be faster and more apples:apples. TBD.
			params.requests <- &s3.GetObjectInput{
				Bucket: bucket,
				Key:    bigList[f],
			}
		} else {
			panic("Developer error")
		}
	}

}

func StartClients(params Params) {

	for i := 0; i < int(params.numClients); i++ {
		svc := makeS3session(params, params.endpoints[i%len(params.endpoints)])

		go params.startClient(svc)
		time.Sleep(1 * time.Millisecond)
	}

}

// Run an individual load request
func (params *Params) startClient(svc *s3.S3) {

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
			} else {
				fmt.Printf("there was an erra: %v , %v", resp, err)
			}
			if numBytes != params.objectSize {
				err = fmt.Errorf("expected object length %d, actual %d", params.objectSize, numBytes)
			}

		case *s3.CreateMultipartUploadInput:
			//first, make a MPU, and get the ID.

			mpu, _ := svc.CreateMultipartUpload(r)
			//we need a place to store completed parts.

			ph := new(partholder)

			//this time , we need to randomize the endpoint list, in case the number of multi-uploaders is smaller than the full list.
			randEndpoints := make([]string, len(params.endpoints))
			perm := mrand.Perm(len(params.endpoints))
			for i, v := range perm {
				randEndpoints[v] = params.endpoints[i]
			}
			//make a channel just for this one object. all part-uploads for this object will flow through it.
			ch := make(chan Req)
			//also make a waitgroup
			wg := new(sync.WaitGroup)
			//and ..make a mutex/locker, so that we can lock before we update the parts list.
			mlock := new(sync.Mutex)
			//we need some more clients, specifically, one per '-multiuploader'
			for i := 0; i < int(params.multiUploaders); i++ {
				svc := makeS3session(*params, randEndpoints[i%len(randEndpoints)])

				//increment wg counter
				wg.Add(1)
				// now, pass along the params, svc, and completedParts list.
				// perhaps its better to make a struct for all this stuff?
				go startMultipartUploader(*params, svc, mpu, ph, wg, ch, mlock)

			}

			//now, need to iterate through the partslist, and shove into a channel.
			var curr, partLength int64
			var remaining = params.objectSize
			partNumber := 1
			for curr = 0; remaining != 0; curr += partLength {
				if remaining < params.partSize {
					//this can probably be simplified, but for now leaving this way to ensure that we don't make extra copies of the slice.
					partLength = remaining
					byteSlice := make([]byte, partLength)
					copy(byteSlice, bufferBytes)

					//actually send the request to the channel.
					ch <- &s3.UploadPartInput{
						Body:          bytes.NewReader(byteSlice),
						Bucket:        mpu.Bucket,
						Key:           mpu.Key,
						PartNumber:    aws.Int64(int64(partNumber)),
						UploadId:      mpu.UploadId,
						ContentLength: aws.Int64(partLength),
					}
				} else {
					partLength = params.partSize

					//actually send the request to the channel.
					ch <- &s3.UploadPartInput{
						Body:          bytes.NewReader(bufferBytes),
						Bucket:        mpu.Bucket,
						Key:           mpu.Key,
						PartNumber:    aws.Int64(int64(partNumber)),
						UploadId:      mpu.UploadId,
						ContentLength: aws.Int64(partLength),
					}
				}

				remaining -= partLength
				partNumber++
			}
			//close the channel
			close(ch)
			//now we wait..
			wg.Wait()
			//sort the parts list
			sort.Sort(partSorter(ph.parts))

			//now, assuming all went well, complete the MPU
			cparams := &s3.CompleteMultipartUploadInput{
				Bucket:          mpu.Bucket,
				Key:             mpu.Key,
				UploadId:        mpu.UploadId,
				MultipartUpload: &s3.CompletedMultipartUpload{Parts: ph.parts},
			}

			_, err = svc.CompleteMultipartUpload(cparams)
			if err != nil {
				fmt.Printf("errror with mpu complete: %v", err)

			}

		default:
			panic("Developer error")
		}

		params.responses <- Resp{err, time.Since(putStartTime), numBytes}
	}
}

func startMultipartUploader(params Params, svc *s3.S3, mpu *s3.CreateMultipartUploadOutput, ph *partholder, wg *sync.WaitGroup, ch chan Req, mlock *sync.Mutex) {
	defer wg.Done()
	for request := range ch {
		switch r := request.(type) {
		case *s3.UploadPartInput:
			uoutput, err := svc.UploadPart(r)

			if err != nil {
				fmt.Printf("errror with mpu: %v", err)
			}
			part := &s3.CompletedPart{}
			part.SetPartNumber(*r.PartNumber)
			part.SetETag(*uoutput.ETag)

			mlock.Lock()

			ph.parts = append(ph.parts, part)

			mlock.Unlock()

		}

	}

}

type partSorter []*s3.CompletedPart

func (a partSorter) Len() int           { return len(a) }
func (a partSorter) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a partSorter) Less(i, j int) bool { return *a[i].PartNumber < *a[j].PartNumber }

// Specifies the parameters for a given test
type Params struct {
	operation  string
	operations string
	requests   chan Req

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
	accessKey        string
	accessSecret     string
	region           string
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
	if params.multipart {
		output += fmt.Sprintf("verbose:       %v\n", params.multipart)
		output += fmt.Sprintf("partSize:       %d\n", float64(params.partSize)/(1024*1024))

	}
	output += fmt.Sprintf("verbose:       %v\n", params.verbose)
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

type partholder struct {
	parts []*s3.CompletedPart
}
