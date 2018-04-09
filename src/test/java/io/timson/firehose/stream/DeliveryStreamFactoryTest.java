package io.timson.firehose.stream;

import io.searchbox.client.JestClient;
import io.timson.firehose.aws.S3Client;
import io.timson.firehose.request.CreateDeliveryStreamRequest;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@RunWith(MockitoJUnitRunner.class)
public class DeliveryStreamFactoryTest {

    @Mock
    private S3Client s3Client;

    @Mock
    private JestClient jestClient;

    private DeliveryStreamFactory deliveryStreamFactory;

    @Before
    public void setUp() throws Exception {
        deliveryStreamFactory = new DeliveryStreamFactory(s3Client, jestClient);
    }

    @Test
    public void shouldCreateS3DeliveryStream_WhenAllRequestsParamsProvided() throws Exception {
        final String JSON = "{\"DeliveryStreamName\":\"myDeliveryStream\","
                + "\"ExtendedS3DestinationConfiguration\":{\"BucketARN\":\"arn:aws:s3:::scv-consumer-lambda-temp\","
                + "\"Prefix\":\"kfh/\",\"BufferingHints\":{\"SizeInMBs\":1,\"IntervalInSeconds\":4},"
                + "\"CompressionFormat\":\"GZIP\"}}";
        CreateDeliveryStreamRequest createRequest = CreateDeliveryStreamRequest.fromJson(JSON);

        DeliveryStream deliveryStream = deliveryStreamFactory.fromRequest(createRequest);

        assertThat(deliveryStream.getName(), is("myDeliveryStream"));
    }

    @Test
    public void shouldCreateElasticsearchDeliveryStream_WhenAllRequestsParamsProvided() throws Exception {
        final String JSON = "{\"DeliveryStreamName\":\"myDeliveryStream\","
                + "\"ElasticsearchDestinationConfiguration\":{\"DomainARN\":\"domain\","
                + "\"IndexRotationPeriod\":\"period\",\"BufferingHints\":{\"SizeInMBs\":1,\"IntervalInSeconds\":4},"
                + "\"IndexName\":\"index\",\"TypeName\":\"type\",\"RoleARN\":\"role\"}}";
        CreateDeliveryStreamRequest createRequest = CreateDeliveryStreamRequest.fromJson(JSON);

        DeliveryStream deliveryStream = deliveryStreamFactory.fromRequest(createRequest);

        assertThat(deliveryStream.getName(), is("myDeliveryStream"));
    }

    @Test
    public void shouldCreateElasticsearchDeliveryStream_WhenAllRequestsMandatoryParamsProvided() throws Exception {
        final String JSON = "{\"DeliveryStreamName\":\"myDeliveryStream\","
                + "\"ElasticsearchDestinationConfiguration\":{\"DomainARN\":\"domain\","
                + "\"IndexName\":\"index\",\"TypeName\":\"type\",\"RoleARN\":\"role\"}}";
        CreateDeliveryStreamRequest createRequest = CreateDeliveryStreamRequest.fromJson(JSON);

        DeliveryStream deliveryStream = deliveryStreamFactory.fromRequest(createRequest);

        assertThat(deliveryStream.getName(), is("myDeliveryStream"));
    }

    @Test
    public void shouldCreateS3DeliveryStream_WhenAllRequestsMandatoryParamsProvided() throws Exception {
        final String JSON = "{\"DeliveryStreamName\":\"myDeliveryStream\","
                + "\"ExtendedS3DestinationConfiguration\":{\"BucketARN\":\"arn:aws:s3:::scv-consumer-lambda-temp\"}}";
        CreateDeliveryStreamRequest createRequest = CreateDeliveryStreamRequest.fromJson(JSON);

        DeliveryStream deliveryStream = deliveryStreamFactory.fromRequest(createRequest);

        assertThat(deliveryStream.getName(), is("myDeliveryStream"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentException_WhenStreamNameIsMissing() throws Exception {
        final String JSON = "{\"ExtendedS3DestinationConfiguration\":{\"BucketARN\":\"arn:aws:s3:::scv-consumer-lambda-temp\"}}";
        CreateDeliveryStreamRequest createRequest = CreateDeliveryStreamRequest.fromJson(JSON);

        deliveryStreamFactory.fromRequest(createRequest);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentException_WhenBucketArnIsMissing() throws Exception {
        final String JSON = "{\"DeliveryStreamName\":\"myDeliveryStream\"}";
        CreateDeliveryStreamRequest createRequest = CreateDeliveryStreamRequest.fromJson(JSON);

        deliveryStreamFactory.fromRequest(createRequest);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldCreateElasticsearchDeliveryStream_WhenRoleArnIsMissing() throws Exception {
        final String JSON = "{\"DeliveryStreamName\":\"myDeliveryStream\","
                + "\"ElasticsearchDestinationConfiguration\":{\"DomainARN\":\"domain\","
                + "\"IndexName\":\"index\",\"TypeName\":\"type\"}}";
        CreateDeliveryStreamRequest createRequest = CreateDeliveryStreamRequest.fromJson(JSON);

        DeliveryStream deliveryStream = deliveryStreamFactory.fromRequest(createRequest);

        assertThat(deliveryStream.getName(), is("myDeliveryStream"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldCreateElasticsearchDeliveryStream_WhenTypeNameIsMissing() throws Exception {
        final String JSON = "{\"DeliveryStreamName\":\"myDeliveryStream\","
                + "\"ElasticsearchDestinationConfiguration\":{\"DomainARN\":\"domain\","
                + "\"IndexName\":\"index\",\"RoleARN\":\"role\"}}";
        CreateDeliveryStreamRequest createRequest = CreateDeliveryStreamRequest.fromJson(JSON);

        DeliveryStream deliveryStream = deliveryStreamFactory.fromRequest(createRequest);

        assertThat(deliveryStream.getName(), is("myDeliveryStream"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldCreateElasticsearchDeliveryStream_WhenIndexNameIsMissing() throws Exception {
        final String JSON = "{\"DeliveryStreamName\":\"myDeliveryStream\","
                + "\"ElasticsearchDestinationConfiguration\":{\"DomainARN\":\"domain\","
                + "\"TypeName\":\"type\",\"RoleARN\":\"role\"}}";
        CreateDeliveryStreamRequest createRequest = CreateDeliveryStreamRequest.fromJson(JSON);

        DeliveryStream deliveryStream = deliveryStreamFactory.fromRequest(createRequest);

        assertThat(deliveryStream.getName(), is("myDeliveryStream"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldCreateElasticsearchDeliveryStream_WhenDomainArnIsMissing() throws Exception {
        final String JSON = "{\"DeliveryStreamName\":\"myDeliveryStream\","
                + "\"ElasticsearchDestinationConfiguration\":{"
                + "\"IndexName\":\"index\",\"TypeName\":\"type\",\"RoleARN\":\"role\"}}";
        CreateDeliveryStreamRequest createRequest = CreateDeliveryStreamRequest.fromJson(JSON);

        DeliveryStream deliveryStream = deliveryStreamFactory.fromRequest(createRequest);

        assertThat(deliveryStream.getName(), is("myDeliveryStream"));
    }
}