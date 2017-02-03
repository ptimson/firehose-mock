package io.timson.firehose.aws;

import com.amazonaws.services.kinesisfirehose.model.CompressionFormat;
import com.amazonaws.services.kinesisfirehose.model.ExtendedS3DestinationConfiguration;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class S3StreamRequestBuilderTest {

    @Test
    public void shouldCreateS3Config_WhenAllParamsProvided() throws Exception {
        S3StreamRequestBuilder builder = new S3StreamRequestBuilder();

        ExtendedS3DestinationConfiguration s3Config = builder
                .withS3Prefix("s3Prefix")
                .withS3BucketArn("s3BucketArn")
                .withCompressionFormat(CompressionFormat.GZIP)
                .withBufferSizeMB(1)
                .withBufferIntervalSeconds(30)
                .build();

        assertThat(s3Config.getPrefix(), is("s3Prefix"));
        assertThat(s3Config.getBucketARN(), is("s3BucketArn"));
        assertThat(s3Config.getCompressionFormat(), is("GZIP"));
        assertThat(s3Config.getBufferingHints().getSizeInMBs(), is(1));
        assertThat(s3Config.getBufferingHints().getIntervalInSeconds(), is(30));
    }

    @Test
    public void shouldCreateS3Config_WhenBufferParamsMissing() throws Exception {
        S3StreamRequestBuilder builder = new S3StreamRequestBuilder();

        ExtendedS3DestinationConfiguration s3Config = builder
                .withS3Prefix("s3Prefix")
                .withS3BucketArn("s3BucketArn")
                .withCompressionFormat(CompressionFormat.GZIP)
                .build();

        assertThat(s3Config.getPrefix(), is("s3Prefix"));
        assertThat(s3Config.getBucketARN(), is("s3BucketArn"));
        assertThat(s3Config.getCompressionFormat(), is("GZIP"));
        assertThat(s3Config.getBufferingHints(), is(nullValue()));
    }

}