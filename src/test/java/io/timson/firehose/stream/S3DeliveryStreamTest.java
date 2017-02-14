package io.timson.firehose.stream;

import com.amazonaws.services.kinesisfirehose.model.CompressionFormat;
import io.timson.firehose.aws.S3Client;
import io.timson.firehose.stream.S3DeliveryStream.S3DeliveryStreamBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static com.jcabi.matchers.RegexMatchers.matchesPattern;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith(MockitoJUnitRunner.class)
public class S3DeliveryStreamTest {

    private static final String STREAM_NAME = "myStream";
    private static final String S3_BUCKET = "myBucketArn";
    private static final String S3_PREFIX = "myPrefix/";
    private static final String MESSAGE_1 = "m1";
    private static final String MESSAGE_2 = "m2";
    private static final String LONG_MESSAGE = "LongMessage!";

    @Mock
    private S3Client s3Client;

    @Captor
    private ArgumentCaptor<String> s3Path;

    private S3DeliveryStream stream;

    @Before
    public void setUp() throws Exception {
        stream = new S3DeliveryStreamBuilder()
                .withName(STREAM_NAME)
                .withS3Client(s3Client)
                .withS3BucketArn("arn:aws:s3:::" + S3_BUCKET)
                .withS3Prefix(S3_PREFIX)
                .withCompressionFormat(CompressionFormat.UNCOMPRESSED)
                .withBufferSizeBytes(10L)
                .withBufferIntervalMilliseconds(50L)
                .build();
    }

    @Test
    public void shouldNotStartFlushTimer_WhenNoMessagesHaveBeenSent() throws Exception {
        Thread.sleep(190);
        stream.write(MESSAGE_1);
        stream.write(MESSAGE_2);

        verifyNoMoreInteractions(s3Client);
    }

    @Test
    public void shouldNotFlush_WhenFlushTimerNotExpired() throws Exception {
        Thread.sleep(40);
        stream.write(MESSAGE_1);
        Thread.sleep(45);
        stream.write(MESSAGE_2);

        verifyNoMoreInteractions(s3Client);
    }

    @Test
    public void shouldFlushOnce_WhenFlushTimerExpired() throws Exception {
        stream.write(MESSAGE_1);
        stream.write(MESSAGE_2);
        stream.write(MESSAGE_2);
        Thread.sleep(55);
        Thread.sleep(55);

        verify(s3Client, only()).createObject(eq(S3_BUCKET), anyString(), eq("m1m2m2"));
    }

    @Test
    public void shouldFlushOnce_WhenReachesBufferSize() throws Exception {
        stream.write(MESSAGE_1);
        verifyNoMoreInteractions(s3Client);

        stream.write(LONG_MESSAGE);
        verify(s3Client, only()).createObject(eq(S3_BUCKET), anyString(), eq("m1LongMessage!"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentException_WhenInvalidS3Arn() throws Exception {
        new S3DeliveryStreamBuilder().withS3BucketArn("Invalid").build();
    }

    @Test
    public void shouldCreateS3ObjectWithValidPrefix() throws Exception {
        stream.write(LONG_MESSAGE);

        final String regex = "myPrefix\\/" + "[0-9]{4}\\/(?:[0-9]{2}\\/){3}" + STREAM_NAME
                + "-1-[0-9]{4}-(?:[0-9]{2}-){5}" + "[0-9a-f\\-]{36}";

        verify(s3Client, only()).createObject(eq(S3_BUCKET), s3Path.capture(), eq(LONG_MESSAGE));
        assertThat(s3Path.getValue(), matchesPattern(regex));
    }

    @Test
    public void shouldCreateGzipS3Object_WhenEncryptionTypeIsGzip() throws Exception {
        stream = new S3DeliveryStreamBuilder().withName(STREAM_NAME)
                .withS3Client(s3Client)
                .withS3BucketArn("arn:aws:s3:::" + S3_BUCKET)
                .withBufferSizeBytes(1L)
                .withCompressionFormat(CompressionFormat.GZIP)
                .build();
        stream.write(MESSAGE_1);

        verify(s3Client, only()).createGzipObject(eq(S3_BUCKET), anyString(), eq(MESSAGE_1));
    }

    @Test
    public void shouldCreateZipS3Object_WhenEncryptionTypeIsZip() throws Exception {
        stream = new S3DeliveryStreamBuilder().withName(STREAM_NAME)
                .withS3Client(s3Client)
                .withS3BucketArn("arn:aws:s3:::" + S3_BUCKET)
                .withBufferSizeBytes(1L)
                .withCompressionFormat(CompressionFormat.ZIP)
                .build();
        stream.write(MESSAGE_1);

        verify(s3Client, only()).createZipObject(eq(S3_BUCKET), anyString(), eq(MESSAGE_1));
    }

    @Test
    public void shouldCreateSnappyS3Object_WhenEncryptionTypeIsSnappy() throws Exception {
        stream = new S3DeliveryStreamBuilder().withName(STREAM_NAME)
                .withS3Client(s3Client)
                .withS3BucketArn("arn:aws:s3:::" + S3_BUCKET)
                .withBufferSizeBytes(1L)
                .withCompressionFormat(CompressionFormat.Snappy)
                .build();
        stream.write(MESSAGE_1);

        verify(s3Client, only()).createSnappyObject(eq(S3_BUCKET), anyString(), eq(MESSAGE_1));
    }

    @Test
    public void shouldThrowNullPointer_WhenStoppedBeforeMessageWritten() throws Exception {
        stream.stop();
    }

}