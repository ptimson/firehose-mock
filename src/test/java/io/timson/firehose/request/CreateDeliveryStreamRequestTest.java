package io.timson.firehose.request;

import com.amazonaws.services.kinesisfirehose.model.CompressionFormat;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

public class CreateDeliveryStreamRequestTest {

    @Test
    public void shouldMakeCreateStreamRequest_WhenValidJson() throws Exception {
        final String JSON = "{\"DeliveryStreamName\":\"myDeliveryStream\","
                + "\"ExtendedS3DestinationConfiguration\":{\"BucketARN\":\"arn:aws:s3:::scv-consumer-lambda-temp\","
                + "\"Prefix\":\"kfh/\",\"BufferingHints\":{\"SizeInMBs\":1,\"IntervalInSeconds\":4},"
                + "\"CompressionFormat\":\"GZIP\"}}";

        CreateDeliveryStreamRequest createRequest = CreateDeliveryStreamRequest.fromJson(JSON);

        S3DeliveryStreamConfig s3Config = createRequest.getS3DeliveryStreamRequest();
        assertThat(createRequest.getName(), is("myDeliveryStream"));
        assertThat(s3Config, not(nullValue()));
        assertThat(s3Config.getS3BucketArn(), is("arn:aws:s3:::scv-consumer-lambda-temp"));
        assertThat(s3Config.getS3Prefix(), is("kfh/"));
        assertThat(s3Config.getBufferingHints().getBufferSizeMB(), is(1));
        assertThat(s3Config.getBufferingHints().getBufferIntervalSeconds(), is(4));
        assertThat(s3Config.getCompressionFormat(), is(CompressionFormat.GZIP));
    }

    @Test
    public void shouldMakeCreateStreamRequest_WhenValidJsonWithoutBuffer() throws Exception {
        final String JSON = "{\"DeliveryStreamName\":\"myDeliveryStream\","
                + "\"ExtendedS3DestinationConfiguration\":{\"BucketARN\":\"arn:aws:s3:::scv-consumer-lambda-temp\","
                + "\"Prefix\":\"kfh/\",\"CompressionFormat\":\"GZIP\"}}";

        CreateDeliveryStreamRequest createRequest = CreateDeliveryStreamRequest.fromJson(JSON);

        S3DeliveryStreamConfig s3Config = createRequest.getS3DeliveryStreamRequest();
        assertThat(s3Config, not(nullValue()));
        assertThat(s3Config.getBufferingHints(), is(nullValue()));
    }

    @Test
    public void shouldMakeCreateStreamRequest_WhenValidJsonWithExtraParams() throws Exception {
        final String JSON = "{\"extra\":\"test\",\"DeliveryStreamName\":\"myDeliveryStream\","
                + "\"ExtendedS3DestinationConfiguration\":{\"BucketARN\":\"arn:aws:s3:::scv-consumer-lambda-temp\","
                + "\"Prefix\":\"kfh/\",\"BufferingHints\":{\"SizeInMBs\":1,\"IntervalInSeconds\":4,\"extra\":\"test\"},"
                + "\"CompressionFormat\":\"GZIP\",\"extra\":\"test\"}}";

        CreateDeliveryStreamRequest createRequest = CreateDeliveryStreamRequest.fromJson(JSON);

        S3DeliveryStreamConfig s3Config = createRequest.getS3DeliveryStreamRequest();
        assertThat(createRequest.getName(), is("myDeliveryStream"));
        assertThat(s3Config, not(nullValue()));
        assertThat(s3Config.getS3BucketArn(), is("arn:aws:s3:::scv-consumer-lambda-temp"));
        assertThat(s3Config.getS3Prefix(), is("kfh/"));
        assertThat(s3Config.getBufferingHints().getBufferSizeMB(), is(1));
        assertThat(s3Config.getBufferingHints().getBufferIntervalSeconds(), is(4));
        assertThat(s3Config.getCompressionFormat(), is(CompressionFormat.GZIP));
    }

    @Test
    public void shouldMakeCreateStreamRequestWithNullFields_WhenJsonMissingParams() throws Exception {
        final String JSON = "{}";
        CreateDeliveryStreamRequest createRequest = CreateDeliveryStreamRequest.fromJson(JSON);

        assertThat(createRequest.getName(), is(nullValue()));
        assertThat(createRequest.getS3DeliveryStreamRequest(), is(nullValue()));
    }

    @Test(expected = IOException.class)
    public void shouldThrowIOException_WhenInvalidJson() throws Exception {
        final String JSON = "invalid";
        CreateDeliveryStreamRequest.fromJson(JSON);
    }

    //TODO
}