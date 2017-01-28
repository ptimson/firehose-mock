package io.timson.firehose.stream;

import io.timson.firehose.aws.S3Client;
        import io.timson.firehose.request.CreateDeliveryStreamRequest;
        import io.timson.firehose.request.S3DeliveryStreamConfig;
        import io.timson.firehose.stream.S3DeliveryStream.S3DeliveryStreamBuilder;

public class DeliveryStreamFactory {

    private final S3Client s3Client;

    public DeliveryStreamFactory(S3Client s3Client) {
        this.s3Client = s3Client;
    }

    public DeliveryStream fromRequest(CreateDeliveryStreamRequest request) {
        final String streamName = request.getName();
        if (request.getS3DeliveryStreamRequest() != null) {
            return s3DeliveryStream(streamName, request.getS3DeliveryStreamRequest());
        }
        throw new IllegalArgumentException("No compatible delivery stream in payload");
    }

    private S3DeliveryStream s3DeliveryStream(String streamName, S3DeliveryStreamConfig request) {
        return new S3DeliveryStreamBuilder().withName(streamName)
                .withS3Client(s3Client)
                .withS3BucketArn(request.getS3BucketArn())
                .withS3Prefix(request.getS3Prefix())
                .withBufferIntervalSeconds(request.getBufferIntervalSeconds())
                .withBufferSizeMB(request.getBufferSizeMB())
                .withCompressionFormat(request.getCompressionFormat())
                .build();
    }

}
