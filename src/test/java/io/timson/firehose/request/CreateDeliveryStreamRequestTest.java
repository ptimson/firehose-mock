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
    public void shouldMakeCreateStreamRequest_WhenValidJsonWithS3Config() throws Exception {
        final String JSON = "{\"DeliveryStreamName\":\"myDeliveryStream\","
                + "\"ExtendedS3DestinationConfiguration\":{\"BucketARN\":\"arn:aws:s3:::scv-consumer-lambda-temp\","
                + "\"Prefix\":\"kfh/\",\"BufferingHints\":{\"SizeInMBs\":1,\"IntervalInSeconds\":4},"
                + "\"CompressionFormat\":\"GZIP\"}}";

        CreateDeliveryStreamRequest createRequest = CreateDeliveryStreamRequest.fromJson(JSON);

        S3DeliveryStreamConfig s3Config = createRequest.getS3DeliveryStreamRequest();
        ElasticsearchDeliveryStreamConfig esConfig = createRequest.getElasticsearchDeliveryStreamRequest();
        assertThat(createRequest.getName(), is("myDeliveryStream"));
        assertThat(s3Config, not(nullValue()));
        assertThat(esConfig, is(nullValue()));
        assertThat(s3Config.getS3BucketArn(), is("arn:aws:s3:::scv-consumer-lambda-temp"));
        assertThat(s3Config.getS3Prefix(), is("kfh/"));
        assertThat(s3Config.getBufferingHints().getBufferSizeMB(), is(1));
        assertThat(s3Config.getBufferingHints().getBufferIntervalSeconds(), is(4));
        assertThat(s3Config.getCompressionFormat(), is(CompressionFormat.GZIP));
    }

    @Test
    public void shouldMakeCreateStreamRequest_WhenValidJsonWithESConfig() throws Exception {
        final String JSON = "{\"DeliveryStreamName\":\"myDeliveryStream\","
                + "\"ElasticsearchDestinationConfiguration\":{\"DomainARN\":\"domain\","
                + "\"IndexRotationPeriod\":\"period\",\"BufferingHints\":{\"SizeInMBs\":1,\"IntervalInSeconds\":4},"
                + "\"IndexName\":\"index\",\"TypeName\":\"type\",\"RoleARN\":\"role\"}}";

        CreateDeliveryStreamRequest createRequest = CreateDeliveryStreamRequest.fromJson(JSON);

        S3DeliveryStreamConfig s3Config = createRequest.getS3DeliveryStreamRequest();
        ElasticsearchDeliveryStreamConfig esConfig = createRequest.getElasticsearchDeliveryStreamRequest();
        assertThat(createRequest.getName(), is("myDeliveryStream"));
        assertThat(s3Config, is(nullValue()));
        assertThat(esConfig, not(nullValue()));
        assertThat(esConfig.getDomainARN(), is("domain"));
        assertThat(esConfig.getRoleARN(), is("role"));
        assertThat(esConfig.getIndexName(), is("index"));
        assertThat(esConfig.getTypeName(), is("type"));
        assertThat(esConfig.getIndexRotationPeriod(), is("period"));
        assertThat(esConfig.getBufferingHints().getBufferSizeMB(), is(1));
        assertThat(esConfig.getBufferingHints().getBufferIntervalSeconds(), is(4));
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
    public void shouldMakeCreateStreamRequest_WhenValidJsonWithExtraParamsInS3Config() throws Exception {
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
        assertThat(s3Config.any().get("extra"), is("test"));
    }

    @Test
    public void shouldMakeCreateStreamRequest_WhenValidJsonWithExtraParamsInESConfig() throws Exception {
        final String JSON = "{\"DeliveryStreamName\":\"myDeliveryStream\","
                + "\"ElasticsearchDestinationConfiguration\":{\"DomainARN\":\"domain\","
                + "\"IndexRotationPeriod\":\"period\",\"BufferingHints\":{\"SizeInMBs\":1,\"IntervalInSeconds\":4},"
                + "\"IndexName\":\"index\",\"TypeName\":\"type\",\"RoleARN\":\"role\",\"extra\":\"param\"}}";

        CreateDeliveryStreamRequest createRequest = CreateDeliveryStreamRequest.fromJson(JSON);

        ElasticsearchDeliveryStreamConfig esConfig = createRequest.getElasticsearchDeliveryStreamRequest();
        assertThat(createRequest.getName(), is("myDeliveryStream"));
        assertThat(esConfig, not(nullValue()));
        assertThat(esConfig.any().get("extra"), is("param"));
    }

    @Test
    public void shouldMakeCreateStreamRequestWithNullFields_WhenJsonMissingParams() throws Exception {
        final String JSON = "{}";
        CreateDeliveryStreamRequest createRequest = CreateDeliveryStreamRequest.fromJson(JSON);

        assertThat(createRequest.getName(), is(nullValue()));
        assertThat(createRequest.getS3DeliveryStreamRequest(), is(nullValue()));
        assertThat(createRequest.getElasticsearchDeliveryStreamRequest(), is(nullValue()));
    }

    @Test(expected = IOException.class)
    public void shouldThrowIOException_WhenInvalidJson() throws Exception {
        final String JSON = "invalid";
        CreateDeliveryStreamRequest.fromJson(JSON);
    }
}