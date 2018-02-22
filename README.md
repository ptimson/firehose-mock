# Firehose Mock
An embedded Java mock for AWS Kinesis Firehose

## Getting Started
### Create Default FirehoseMock
```java
FirehoseMock firehoseMock = FirehoseMock.defaultMock();
firehoseMock.start();
Integer port = firehoseMock.getPort(); 
```

### Create Custom FirehoseMock
```java
FirehoseMock firehoseMock = new FirehoseMock.Builder()
        .withPort(7070)                // Optional - If not supplied will use a random free port
        .withAmazonS3Client(amazonS3)  // Optional - If not supplied will use AmazonS3ClientBuilder.defaultClient()
        .withJestClient(jestClient)    // Optional - If not supplied will use JestClientFactory().getObject()
        .build();
firehoseMock.start();
```

## Creating Requests
You can then interact with the mock using the AWS Kinesis Firehose SDK - as you would interact with firehose normally. 
However you will need to set the client's endpoint to `localhost:<port>`

### AWSFirehoseUtil
`AWSFirehoseUtil.class` is an optional helper class used to setup the client and create requests that are supported with 
this mock. 

#### Create Client
```java
AmazonKinesisFirehose firehoseClient = AWSFirehoseUtil.createClient("http://127.0.0.1:7070", "eu-west-1");
```

#### Create PutRecord Request
```java
PutRecordRequest putRequest = AWSFirehoseUtil.createPutRequest("myDeliveryStream", "myData");
firehoseClient.putRecord(putRequest);
```

#### Create CreateDeliveryStream Request
```java
ExtendedS3DestinationConfiguration s3StreamConfig = AWSFirehoseUtil.createS3DeliveryStream()
        .withS3BucketArn("arn:aws:s3:::myBucket")
        .withBufferIntervalSeconds(10)
        .withBufferSizeMB(1024)
        .withCompressionFormat(CompressionFormat.GZIP)
        .withS3Prefix("myPrefix")
        .build();
CreateDeliveryStreamRequest createDeliveryStreamRequest = AWSFirehoseUtil.createS3DeliveryStreamRequest("myDeliverStream", s3StreamConfig);
firehoseClient.createDeliveryStream(createDeliveryStreamRequest);
```

```java
ElasticsearchDestinationConfiguration elasticsearchDestinationConfiguration = AWSFirehoseUtil.createElasticsearchDeliveryStream()
        .withIndexName("myESIndex")
        .withTypeName("myTypeName")
        .withRoleARN("arn:aws:iam::123456789012:user/es-user")
        .withDomainARN("arn:aws:es:us-east-1:123456789012:domain/test/*")
        .withIndexRotationPeriod("oneHour")
        .withBufferSizeMb(5)
        .withBufferSizeMb(300)
        .build();
CreateDeliveryStreamRequest createDeliveryStreamRequest = AWSFirehoseUtil.createElasticsearchDeliveryStreamRequest("myStream", elasticsearchDestinationConfiguration)
firehoseClient.createDeliveryStream(createDeliveryStreamRequest);
```

#### Create DeleteDeliveryStream Request
```java
DeleteDeliveryStreamRequest deleteStreamRequest = AWSFirehoseUtil.deleteDeliveryStreamRequest(streamName);
firehoseClient.deleteDeliveryStream(deleteStreamRequest);
```
## Supported Requests
Firehose Mock is a WIP and so far only supports the following 3 APIs.

### PutRecord
```
{
   "DeliveryStreamName": "string",
   "Record": { 
      "Data": "myBase64EncodedString"
   }
}
```
See [AWS PutRecord API Docs](http://docs.aws.amazon.com/firehose/latest/APIReference/API_PutRecord.html) for more details.

### CreateDeliveryStream
Currently FirehoseMock supports S3 Delivery Streams and Elasticsearch Delivery Streams with the following options:  
```
{
    "DeliveryStreamName": "string",
    "ExtendedS3DestinationConfiguration": {
        "BucketARN": "string",               // Required
        "BufferingHints": {
            "IntervalInSeconds": number,     // Optional - Default: 300
            "SizeInMBs": number              // Optional - Default: 5
        },
        "CompressionFormat": "string",       // Optional - Default: UNCOMPRESSED
        "Prefix": "string"                   // Optional - Default: ""
    }
    "ElasticsearchDestinationConfiguration": { 
         "BufferingHints": { 
            "IntervalInSeconds": number,     // Optional - Default: 300
            "SizeInMBs": number              // Optional - Default: 5
         },
         "DomainARN": "string",
         "IndexName": "string",
         "IndexRotationPeriod": "string",    // Optional - Default: OneMonth
         "RoleARN": "string",
         "TypeName": "string"
    },
}
``` 
See [AWS CreateDeliveryStream API Docs](http://docs.aws.amazon.com/firehose/latest/APIReference/API_CreateDeliveryStream.html) for more details.

### DeleteDeliveryStream

```
{
   "DeliveryStreamName": "string"
}
```
See [AWS DeleteDeliveryStream API Docs](http://docs.aws.amazon.com/firehose/latest/APIReference/API_DeleteDeliveryStream.html) for more details.

## License
Copyright (C) 2017 Peter Timson
Copyright (C) 2018 Tomasz Bielaszewski

Licensed under the Apache License, Version 2.0