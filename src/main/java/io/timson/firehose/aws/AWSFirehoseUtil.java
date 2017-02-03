package io.timson.firehose.aws;

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClientBuilder;
import com.amazonaws.services.kinesisfirehose.model.CreateDeliveryStreamRequest;
import com.amazonaws.services.kinesisfirehose.model.DeleteDeliveryStreamRequest;
import com.amazonaws.services.kinesisfirehose.model.ExtendedS3DestinationConfiguration;
import com.amazonaws.services.kinesisfirehose.model.PutRecordRequest;
import com.amazonaws.services.kinesisfirehose.model.Record;

import java.nio.ByteBuffer;

public class AWSFirehoseUtil {

    /**
     * Create a AWS Firehose Client with custom endpoint &amp; region.
     *
     * @param endpoint The endpoint to your Firehose server if using mock 127.0.0.1:{port}.
     * @param region   AWS region. Firehose mock does not use this.
     * @return An AmazonKinesisFirehose with configured params.
     */
    public static AmazonKinesisFirehose createClient(String endpoint, String region) {
        final AmazonKinesisFirehoseClientBuilder builder = AmazonKinesisFirehoseClientBuilder.standard();

        builder.setEndpointConfiguration(new EndpointConfiguration(endpoint, region));
        return builder.build();
    }

    /**
     * Create a PutRecordRequest for the AWS Firehose Client.
     *
     * @param deliveryStreamName Name of Firehose stream.
     * @param data               String of data to append to firehose (UTF-8) e.g. "My String".
     * @return A PutRecordRequest to be sent using the Firehose client.
     */
    public static PutRecordRequest createPutRequest(String deliveryStreamName, String data) {
        final PutRecordRequest putRecordRequest = new PutRecordRequest();
        putRecordRequest.setDeliveryStreamName(deliveryStreamName);
        putRecordRequest.setRecord(createRecord(data));
        return putRecordRequest;
    }

    private static Record createRecord(String data) {
        Record record = new Record();
        ByteBuffer dataBuffer = ByteBuffer.wrap(data.getBytes());
        record.setData(dataBuffer);
        return record;
    }

    /**
     * Create a CreateDeliveryStreamRequest request for a new Firehose S3 Delivery Stream.
     *
     * @param deliveryStreamName Name of Firehose stream.
     * @param config             ExtendedS3DestinationConfiguration containing config for the new S3 Delivery Stream.
     * @return A CreateDeliveryStreamRequest to be sent using the Firehose client.
     */
    public static CreateDeliveryStreamRequest createDeliveryStreamRequest(String deliveryStreamName, ExtendedS3DestinationConfiguration config) {
        CreateDeliveryStreamRequest createDeliveryStreamRequest = new CreateDeliveryStreamRequest();
        createDeliveryStreamRequest.setDeliveryStreamName(deliveryStreamName);
        createDeliveryStreamRequest.setExtendedS3DestinationConfiguration(config);
        return createDeliveryStreamRequest;
    }

    /**
     * Builder to create ExtendedS3DestinationConfiguration for a new S3 Delivery Stream.
     *
     * @return A S3StreamRequestBuilder object.
     */
    public static S3StreamRequestBuilder createS3DeliveryStream() {
        return new S3StreamRequestBuilder();
    }

    /**
     * Create a DeleteDeliveryStreamRequest to delete a Firehose Delivery Stream.
     *
     * @param deliveryStreamName Name of Firehose stream.
     * @return A DeleteDeliveryStreamRequest to be sent using the Firehose client.
     */
    public static DeleteDeliveryStreamRequest deleteDeliveryStreamRequest(String deliveryStreamName) {
        DeleteDeliveryStreamRequest deleteDeliveryStreamRequest = new DeleteDeliveryStreamRequest();
        deleteDeliveryStreamRequest.setDeliveryStreamName(deliveryStreamName);
        return deleteDeliveryStreamRequest;
    }

}
