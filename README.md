# S3 Bench
This tool offers the ability to run very basic throughput benchmarking against
an S3-compatible endpoint. It does a series of put operations followed by a
series of get operations and displays the corresponding statistics. The tool
uses the AWS Go SDK.

## Requirements
* Go

## Installation
Run the following command to build the binary.

```
go get github.com/databloom/s3bench
```
The binary will be placed under $GOPATH/bin/s3bench.

manual compile:

```
go build -ldflags "-linkmode external -extldflags -static"
```

If you lack a proper go environment to build on, use docker.  Toss this in a `Dockerfile` and you should have what you need: 

```FROM golang:1.13.5-alpine3.10```


## Usage
The s3bench command is self-describing. In order to see all the available options
just run s3bench -help.

### Note on multipart
Specifying `-multipart` will only impact writes, not reads (so far).  The current multipart implementation does NOT use the `s3manager` package, rather it is creating parts individually and sending them in parallel per object. Also:
* The program will generate `-partsize` bytes of random data to use. Each part will be 100% identical (except for the last part, which is smaller).  This will be changed to be more random later.
* specifying `-numClients` starts up XX sessions, which translates to maximum concurrent object-uploads: however each object-upload will leverage `-multiUploaders` , so the effect is multiplicative.  Use with care.  Put another way:
   * `-numclients 10` && `-multiUploaders 10` means there will be 100 concurrent threads uploading.
* you can specify `-partSize` in bytes.  Note that the minimum supported by the SDK is 5MiB.


### Example input
The following will run a benchmark from 2 concurrent sessions, which in
aggregate will put a total of 10 unique new objects. Each object will be
exactly 1024 bytes. The objects will be placed in a bucket named loadgen.
The S3 endpoint will be ran against http://endpoint1:80 and
http://endpoint2:80. Object name will be prefixed with loadgen.

```
./s3bench -accessKey=KEY -accessSecret=SECRET -bucket=loadgen -endpoint=http://endpoint1:80,http://endpoint2:80 -numClients=2 -numSamples=10 -objectNamePrefix=loadgen -objectSize=1024
```

#### Note on regions & endpoints
By default, the region used will be `vast-west` , a fictitious region which
is suitable for using with the VAST systems.  However, you can elect to
use this tool with Amazon S3, in which case you will need to specify the proper region.

It is also important when using Amazon S3 that you specify the proper endpoint, which
will generally be `http://s3-regionName.amazonaws.com:80`. EG: if the bucket which you are
testing is in Oregon, you would specify:

```
-endpoint http://s3-us-west-2.amazonaws.com:80 -region us-west-2
```

For more information on this, please refer to [AmazonS3 documentation.](https://aws.amazon.com/documentation/s3/)



### Example output
The output will consist of details for every request being made as well as the
current average throughput. At the end of the run summaries of the put and get
operations will be displayed.

```
Test parameters
endpoint(s):      [http://endpoint1:80 http://endpoint2:80]
bucket:           loadgen
objectNamePrefix: loadgen
objectSize:       0.0010 MB
numClients:       2
numSamples:       10


Generating in-memory sample data... Done (95.958µs)

Running Write test...
Write operation completed in 0.37s (1/10) - 0.00MB/s
Write operation completed in 0.39s (2/10) - 0.01MB/s
Write operation completed in 0.34s (3/10) - 0.00MB/s
Write operation completed in 0.72s (4/10) - 0.00MB/s
Write operation completed in 0.53s (5/10) - 0.00MB/s
Write operation completed in 0.38s (6/10) - 0.00MB/s
Write operation completed in 0.54s (7/10) - 0.00MB/s
Write operation completed in 0.59s (8/10) - 0.00MB/s
Write operation completed in 0.79s (9/10) - 0.00MB/s
Write operation completed in 0.60s (10/10) - 0.00MB/s

Running Read test...
Read operation completed in 0.00s (1/10) - 0.51MB/s
Read operation completed in 0.00s (2/10) - 1.00MB/s
Read operation completed in 0.00s (3/10) - 0.85MB/s
Read operation completed in 0.00s (4/10) - 1.13MB/s
Read operation completed in 0.00s (5/10) - 1.02MB/s
Read operation completed in 0.00s (6/10) - 1.15MB/s
Read operation completed in 0.00s (7/10) - 1.12MB/s
Read operation completed in 0.00s (8/10) - 1.26MB/s
Read operation completed in 0.00s (9/10) - 1.20MB/s
Read operation completed in 0.00s (10/10) - 1.28MB/s

Test parameters
endpoint(s):      [http://endpoint1:80 http://endpoint2:80]
bucket:           loadgen
objectNamePrefix: loadgen
objectSize:       0.0010 MB
numClients:       2
numSamples:       10

Results Summary for Write Operation(s)
Total Transferred: 0.010 MB
Total Throughput:  0.00 MB/s
Total Duration:    2.684 s
Number of Errors:  0
------------------------------------
Put times Max:       0.791 s
Put times 99th %ile: 0.791 s
Put times 90th %ile: 0.791 s
Put times 75th %ile: 0.601 s
Put times 50th %ile: 0.543 s
Put times 25th %ile: 0.385 s
Put times Min:       0.336 s


Results Summary for Read Operation(s)
Total Transferred: 0.010 MB
Total Throughput:  1.28 MB/s
Total Duration:    0.008 s
Number of Errors:  0
------------------------------------
Put times Max:       0.002 s
Put times 99th %ile: 0.002 s
Put times 90th %ile: 0.002 s
Put times 75th %ile: 0.002 s
Put times 50th %ile: 0.001 s
Put times 25th %ile: 0.001 s
Put times Min:       0.001 s
```
