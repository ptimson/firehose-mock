package io.timson.firehose.aws;

import com.amazonaws.services.kinesisfirehose.model.BufferingHints;
import com.amazonaws.services.kinesisfirehose.model.CompressionFormat;
import com.amazonaws.services.kinesisfirehose.model.ExtendedS3DestinationConfiguration;

public final class S3StreamRequestBuilder {
    private String s3BucketArn;
    private String s3Prefix;
    private CompressionFormat compressionFormat;
    private Integer bufferIntervalSeconds;
    private Integer bufferSizeMb;

    public ExtendedS3DestinationConfiguration build() {
        ExtendedS3DestinationConfiguration s3Config = new ExtendedS3DestinationConfiguration();
        s3Config.setBucketARN(s3BucketArn);
        if (s3Prefix != null) s3Config.setPrefix(s3Prefix);
        if (compressionFormat != null) s3Config.setCompressionFormat(compressionFormat);
        if (bufferIntervalSeconds != null || bufferSizeMb != null) {
            BufferingHints bufferingHints = new BufferingHints();
            if (bufferIntervalSeconds != null) bufferingHints.setIntervalInSeconds(bufferIntervalSeconds);
            if (bufferSizeMb != null) bufferingHints.setSizeInMBs(bufferSizeMb);
            s3Config.setBufferingHints(bufferingHints);
        }
        return s3Config;
    }

    public S3StreamRequestBuilder withS3BucketArn(String s3BucketArn) {
        this.s3BucketArn = s3BucketArn;
        return this;
    }

    public S3StreamRequestBuilder withS3Prefix(String s3Prefix) {
        this.s3Prefix = s3Prefix;
        return this;
    }

    public S3StreamRequestBuilder withCompressionFormat(CompressionFormat compressionFormat) {
        this.compressionFormat = compressionFormat;
        return this;
    }

    public S3StreamRequestBuilder withBufferIntervalSeconds(Integer bufferIntervalSeconds) {
        this.bufferIntervalSeconds = bufferIntervalSeconds;
        return this;
    }

    public S3StreamRequestBuilder withBufferSizeMB(Integer bufferSizeMb) {
        this.bufferSizeMb = bufferSizeMb;
        return this;
    }

}