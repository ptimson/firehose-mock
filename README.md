# Firehose Mock
An embedded Java mock for AWS Kinesis Firehose

## Supported Requests
### PutRecord
```json
{
   "DeliveryStreamName": "string",
   "Record": { 
      "Data": blob
   }
}
```

### CreateDeliveryStream
```
{
    "DeliveryStreamName": "string",
    "ExtendedS3DestinationConfiguration": {
        "BucketARN": "string",               // Required
        "BufferingHints": {
            "IntervalInSeconds": number,     // Optional - Default: 300
            "SizeInMBs": number              // Optional - Default: 5
        },
        "CompressionFormat": "string",       // Optional
        "Prefix": "string"                   // Optional - Default: UNCOMPRESSED
    }
}
```
    
[AWS CreateDeliveryStream API Docs](http://docs.aws.amazon.com/firehose/latest/APIReference/API_CreateDeliveryStream.html)

### DeleteDeliveryStream

```json
{
   "DeliveryStreamName": "string"
}
```

## Getting Started
```java
FirehoseMock firehoseMock = new FirehoseMock.Builder()
        .withPort(7070)                  // Optional - If not supplied will use free random port
        .withAmazonS3Client(amazonS3)    // Optional - If not supplied will use AmazonS3ClientBuilder.defaultClient()
        .build();
```

## Creating Requests
### AWSFirehoseUtil

`AWSFirehoseUtil` is an optional helper class used to create requests that are supported with this mock. 

## Create Client
```java
AmazonKinesisFirehose firehoseClient = AWSFirehoseUtil.createClient("http://127.0.0.1:7070", "eu-west-1");
```

## Create PutRecord Request
```java
PutRecordRequest putRequest = AWSFirehoseUtil.createPutRequest("myDeliveryStream", "myData");
firehoseClient.putRecord(putRequest);
```

## Create CreateDeliveryStream Request
```java
ExtendedS3DestinationConfiguration s3StreamConfig = AWSFirehoseUtil.createS3DeliveryStream()
        .withS3BucketArn("arn:myBucketArn")
        .withBufferIntervalSeconds(10)
        .withBufferSizeMB(1024)
        .withCompressionFormat(CompressionFormat.GZIP)
        .withS3Prefix("myPrefix")
        .build();
CreateDeliveryStreamRequest createDeliveryStreamRequest = AWSFirehoseUtil.createDeliveryStreamRequest("myDeliverStream", s3StreamConfig);
firehoseClient.createDeliveryStream(createDeliveryStreamRequest);
```

## Create DeleteDeliveryStream Request
```java
DeleteDeliveryStreamRequest deleteStreamRequest = AWSFirehoseUtil.deleteDeliveryStreamRequest(streamName);
firehoseClient.deleteDeliveryStream(deleteStreamRequest);
```

## License
Copyright (C) 2017 Peter Timson

Licensed under the Apache License, Version 2.0