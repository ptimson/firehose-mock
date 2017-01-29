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

    public static AmazonKinesisFirehose createClient(String endpoint, String region) {
        final AmazonKinesisFirehoseClientBuilder builder = AmazonKinesisFirehoseClientBuilder.standard();
        builder.setEndpointConfiguration(new EndpointConfiguration(endpoint, region));
        return builder.build();
    }

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

    public static CreateDeliveryStreamRequest createDeliveryStreamRequest(String deliveryStreamName, ExtendedS3DestinationConfiguration config) {
        CreateDeliveryStreamRequest createDeliveryStreamRequest = new CreateDeliveryStreamRequest();
        createDeliveryStreamRequest.setDeliveryStreamName(deliveryStreamName);
        createDeliveryStreamRequest.setExtendedS3DestinationConfiguration(config);
        return createDeliveryStreamRequest;
    }

    public static S3StreamRequestBuilder createS3DeliveryStream() {
        return new S3StreamRequestBuilder();
    }

    public static DeleteDeliveryStreamRequest deleteDeliveryStreamRequest(String deliveryStreamName) {
        DeleteDeliveryStreamRequest deleteDeliveryStreamRequest = new DeleteDeliveryStreamRequest();
        deleteDeliveryStreamRequest.setDeliveryStreamName(deliveryStreamName);
        return deleteDeliveryStreamRequest;
    }

}
