package io.timson.firehose;

import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.model.CompressionFormat;
import com.amazonaws.services.kinesisfirehose.model.CreateDeliveryStreamRequest;
import com.amazonaws.services.kinesisfirehose.model.CreateDeliveryStreamResult;
import com.amazonaws.services.kinesisfirehose.model.DeleteDeliveryStreamRequest;
import com.amazonaws.services.kinesisfirehose.model.ExtendedS3DestinationConfiguration;
import com.amazonaws.services.kinesisfirehose.model.PutRecordRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordResult;
import io.timson.firehose.aws.AWSFirehoseUtil;
import io.timson.firehose.test.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

// TODO Should be ITs
public class FirehoseMockIT {

    private static final String ENDPOINT = "http://127.0.0.1:7070";
    private static final String REGION = "eu-west-1";
    private static final int KILOBYTES = 1024;

    private final AmazonKinesisFirehose firehoseClient = AWSFirehoseUtil.createClient(ENDPOINT, REGION);

    private FirehoseMock firehoseMock;

    @Before
    public void setUp() throws Exception {
        firehoseMock = new FirehoseMock.Builder()
                .withPort(7070)
                .build();
        firehoseMock.start();
    }

    @After
    public void tearDown() throws Exception {
        firehoseMock.stop();
    }

    @Test
    public void shouldAcceptPutRecordRequest() throws Exception {
        final String streamName = "myDeliveryStream";
        createS3DeliveryStream(streamName);
        PutRecordRequest putRequest = AWSFirehoseUtil.createPutRequest(streamName, "myData");
        firehoseClient.putRecord(putRequest);
    }

    @Test
    public void shouldAcceptCreateDeliveryStreamRequest() throws Exception {
        final ExtendedS3DestinationConfiguration s3StreamConfig = AWSFirehoseUtil.createS3DeliveryStream()
                .withBufferIntervalSeconds(10)
                .withBufferSizeMB(1024)
                .withCompressionFormat(CompressionFormat.GZIP)
                .withS3BucketArn("arn:aws:s3:::myBucketArn")
                .withS3Prefix("myPrefix")
                .build();
        final String streamName = "myDeliveryStream";
        CreateDeliveryStreamRequest createStreamRequest = AWSFirehoseUtil.createDeliveryStreamRequest(streamName, s3StreamConfig);
        firehoseClient.createDeliveryStream(createStreamRequest);
    }

    @Test
    public void shouldAcceptDeleteDeliveryStreamRequest() throws Exception {
        final String streamName = "myDeliveryStream";
        createS3DeliveryStream(streamName);
        DeleteDeliveryStreamRequest deleteStreamRequest = AWSFirehoseUtil.deleteDeliveryStreamRequest(streamName);
        firehoseClient.deleteDeliveryStream(deleteStreamRequest);
    }

    @Test
    public void shouldWriteToS3WhenReachedBufferSize() throws Exception {
        final String streamName = "myDeliveryStream";
        createS3DeliveryStream(streamName);
        String data = TestUtil.createStringOfSize(512 * KILOBYTES);
        PutRecordRequest putRequest = AWSFirehoseUtil.createPutRequest(streamName, data);
        firehoseClient.putRecord(putRequest);
        firehoseClient.putRecord(putRequest);
        firehoseClient.putRecord(putRequest);
    }

    private void createS3DeliveryStream(String streamName) {
        final ExtendedS3DestinationConfiguration s3StreamConfig = AWSFirehoseUtil.createS3DeliveryStream()
                .withBufferIntervalSeconds(4)
                .withBufferSizeMB(1)
                .withCompressionFormat(CompressionFormat.GZIP)
                .withS3BucketArn("arn:aws:s3:::firehose-mock-testing")
                .withS3Prefix("kfh/")
                .build();
        CreateDeliveryStreamRequest createStreamRequest = AWSFirehoseUtil.createDeliveryStreamRequest(streamName, s3StreamConfig);
        firehoseClient.createDeliveryStream(createStreamRequest);
    }

}