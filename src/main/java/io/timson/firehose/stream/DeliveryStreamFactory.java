package io.timson.firehose.stream;

import io.searchbox.client.JestClient;
import io.timson.firehose.aws.S3Client;
import io.timson.firehose.request.BufferingHints;
import io.timson.firehose.request.CreateDeliveryStreamRequest;
import io.timson.firehose.request.ElasticsearchDeliveryStreamConfig;
import io.timson.firehose.request.S3DeliveryStreamConfig;
        import io.timson.firehose.stream.S3DeliveryStream.S3DeliveryStreamBuilder;

import java.util.Optional;

public class DeliveryStreamFactory {

    private final S3Client s3Client;
    private final JestClient jestClient;

    public DeliveryStreamFactory(S3Client s3Client, JestClient jestClient) {
        this.s3Client = s3Client;
        this.jestClient = jestClient;
    }

    public DeliveryStream fromRequest(CreateDeliveryStreamRequest request) {
        final String streamName = request.getName();
        if (request.getS3DeliveryStreamRequest() != null) {
            return s3DeliveryStream(streamName, request.getS3DeliveryStreamRequest());
        }
        if (request.getElasticsearchDeliveryStreamRequest() != null) {
            return elasticsearchDeliveryStream(streamName, request.getElasticsearchDeliveryStreamRequest());
        }
        throw new IllegalArgumentException("No compatible delivery stream in payload");
    }

    private S3DeliveryStream s3DeliveryStream(String streamName, S3DeliveryStreamConfig request) {
        return new S3DeliveryStreamBuilder()
                .withName(streamName)
                .withS3Client(s3Client)
                .withS3BucketArn(request.getS3BucketArn())
                .withS3Prefix(request.getS3Prefix())
                .withBufferIntervalSeconds(Optional.ofNullable(request.getBufferingHints()).orElseGet(BufferingHints::new).getBufferIntervalSeconds())
                .withBufferSizeMB(Optional.ofNullable(request.getBufferingHints()).orElseGet(BufferingHints::new).getBufferSizeMB())
                .withCompressionFormat(request.getCompressionFormat())
                .build();
    }

    private ElasticsearchDeliveryStream elasticsearchDeliveryStream(String streamName, ElasticsearchDeliveryStreamConfig request) {
        return new ElasticsearchDeliveryStream.ElasticsearchDeliveryStreamBuilder()
                .withStreamName(streamName)
                .withIndexName(request.getIndexName())
                .withDocType(request.getTypeName())
                .withJestClient(jestClient)
                .withBufferIntervalSeconds(Optional.ofNullable(request.getBufferingHints()).orElseGet(BufferingHints::new).getBufferIntervalSeconds())
                .withBufferSizeMB(Optional.ofNullable(request.getBufferingHints()).orElseGet(BufferingHints::new).getBufferSizeMB())
                .withDomainARN(request.getDomainARN())
                .withRoleARN(request.getRoleARN())
                .withIndexRotationPeriod(request.getIndexRotationPeriod())
                .build();
    }

}
