package io.timson.firehose.aws;

import com.amazonaws.services.kinesisfirehose.model.BufferingHints;
import com.amazonaws.services.kinesisfirehose.model.CompressionFormat;
import com.amazonaws.services.kinesisfirehose.model.ExtendedS3DestinationConfiguration;

public final class S3StreamBuilder {
    private String s3BucketArn;
    private String s3Prefix;
    private CompressionFormat compressionFormat;
    private Integer bufferIntervalSeconds;
    private Integer bufferSizeMb;

    public ExtendedS3DestinationConfiguration build() {
        ExtendedS3DestinationConfiguration s3Config = new ExtendedS3DestinationConfiguration();
        s3Config.setBucketARN(s3BucketArn);
        s3Config.setPrefix(s3Prefix);
        s3Config.setCompressionFormat(compressionFormat);
        BufferingHints bufferingHints = new BufferingHints();
        bufferingHints.setIntervalInSeconds(bufferIntervalSeconds);
        bufferingHints.setSizeInMBs(bufferSizeMb);
        s3Config.setBufferingHints(bufferingHints);
        return s3Config;
    }

    public S3StreamBuilder withS3BucketArn(String s3BucketArn) {
        this.s3BucketArn = s3BucketArn;
        return this;
    }

    public S3StreamBuilder withS3Prefix(String s3Prefix) {
        this.s3Prefix = s3Prefix;
        return this;
    }

    public S3StreamBuilder withCompressionFormat(CompressionFormat compressionFormat) {
        this.compressionFormat = compressionFormat;
        return this;
    }

    public S3StreamBuilder withBufferIntervalSeconds(Integer bufferIntervalSeconds) {
        this.bufferIntervalSeconds = bufferIntervalSeconds;
        return this;
    }

    public S3StreamBuilder withBufferSizeMB(Integer bufferSizeMb) {
        this.bufferSizeMb = bufferSizeMb;
        return this;
    }

}