package io.timson.firehose.stream;

import com.amazonaws.services.kinesisfirehose.model.CompressionFormat;
import io.timson.firehose.aws.S3Client;

import java.io.IOException;
import java.nio.charset.Charset;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.amazonaws.services.kinesisfirehose.model.CompressionFormat.UNCOMPRESSED;
import static io.timson.firehose.util.FirehoseUtil.isEmpty;
import static java.time.ZoneOffset.UTC;

public class S3DeliveryStream implements DeliveryStream {

    private static final Logger logger = Logger.getLogger(S3DeliveryStream.class.getName());

    private static final Charset UTF_8 = Charset.forName("UTF-8");
    private static final long MEGABYTE = 1024L * 1024L;
    private static final int VERSION = 1;
    private static final DateTimeFormatter YYYY_MM_DD_HH = DateTimeFormatter.ofPattern("yyyy/MM/dd/HH");
    private static final DateTimeFormatter YYYY_MM_DD_HH_MM_SS = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss");

    private final String name;
    private final S3Client s3Client;
    private final String s3Bucket;
    private final String s3Prefix;
    private final Integer bufferIntervalMs;
    private final int bufferFlushSizeMb;
    private final long bufferFlushSizeBytes;
    private final CompressionFormat compressionFormat;

    private StringBuilder buffer = new StringBuilder();
    private long bufferSize = 0;
    private Timer flushTimer;
    private TimerTask flushTimerTask;

    private S3DeliveryStream(String name, S3Client s3Client, String s3BucketArn, String s3Prefix, Integer bufferIntervalSeconds, Integer bufferSizeMb, CompressionFormat compressionFormat) {
        this.name = name;
        this.s3Client = s3Client;
        this.s3Bucket = extractBucketName(s3BucketArn);
        this.s3Prefix = s3Prefix;
        this.bufferIntervalMs = bufferIntervalSeconds * 1000;
        this.bufferFlushSizeMb = bufferSizeMb;
        this.bufferFlushSizeBytes = bufferSizeMb * MEGABYTE;
        this.compressionFormat = compressionFormat;
        startFlushTimer();
    }

    private String extractBucketName(String s3BucketArn) {
        if (!s3BucketArn.startsWith("arn:aws:s3:::")) {
            throw new IllegalArgumentException(String.format("Invalid Bucket ARN %s", s3BucketArn));
        }
        return s3BucketArn.substring("arn:aws:s3:::".length());
    }

    private void startFlushTimer() {
        flushTimerTask = new TimerTask() {
            @Override
            public void run() {
                flush();
            }
        };
        flushTimer = new Timer();
        flushTimer.schedule(flushTimerTask, 0, bufferIntervalMs);
    }

    private void stopFlushTimer() {
        flushTimer.cancel();
    }

    @Override
    public synchronized void write(String data) {
        buffer.append(data);
        bufferSize += data.getBytes(UTF_8).length;
        if (bufferSize >= bufferFlushSizeBytes) {
            resetFlushTimer();
            flush();
        }
    }

    private void resetFlushTimer() {
        stopFlushTimer();
        startFlushTimer();
    }

    @Override
    public void stop() {
        stopFlushTimer();
    }

    @Override
    public String getName() {
        return name;
    }

    private synchronized void flush() {
        if (bufferSize > 0) {
            String s3Path = generateS3Path();
            try {
                createS3Object(s3Bucket, s3Path, buffer.toString());
                bufferSize = 0;
                buffer.setLength(0);
            } catch (IOException e) {
                logger.log(Level.SEVERE, String.format("Unable to save S3 Object s3://%s/%s", s3Bucket, s3Path), e);
            }
        }
    }

    private void createS3Object(String s3Bucket, String s3Path, String content) throws IOException {
        switch (compressionFormat) {
            case Snappy:
                s3Client.createSnappyObject(s3Bucket, s3Path, content);
                break;
            case GZIP:
                s3Client.createGzipObject(s3Bucket, s3Path, content);
                break;
            case ZIP:
                s3Client.createZipObject(s3Bucket, s3Path, content);
                break;
            case UNCOMPRESSED:
            default:
                s3Client.createObject(s3Bucket, s3Path, content);
        }
    }

    private String generateS3Path() {
        final LocalDateTime now = LocalDateTime.now(UTC);
        return s3Prefix + now.format(YYYY_MM_DD_HH) + '/' + name + '-' + VERSION + '-' + now.format(YYYY_MM_DD_HH_MM_SS) + '-' + UUID.randomUUID().toString();
    }

    public static class S3DeliveryStreamBuilder {
        private String name;
        private S3Client s3Client;
        private String s3BucketArn;
        private String s3Prefix = "";
        private Integer bufferIntervalSeconds = 300;
        private Integer bufferSizeMb = 5;
        private CompressionFormat compressionFormat = UNCOMPRESSED;

        public S3DeliveryStreamBuilder withName(String name) {
            this.name = name;
            return this;
        }

        public S3DeliveryStreamBuilder withS3Client(S3Client s3Client) {
            this.s3Client = s3Client;
            return this;
        }

        public S3DeliveryStreamBuilder withS3BucketArn(String s3BucketArn) {
            this.s3BucketArn = s3BucketArn;
            return this;
        }

        public S3DeliveryStreamBuilder withS3Prefix(String s3Prefix) {
            if (s3Prefix == null) return this;
            this.s3Prefix = s3Prefix;
            return this;
        }

        public S3DeliveryStreamBuilder withBufferIntervalSeconds(Integer bufferIntervalSeconds) {
            if (bufferIntervalSeconds == null) return this;
            this.bufferIntervalSeconds = bufferIntervalSeconds;
            return this;
        }

        public S3DeliveryStreamBuilder withBufferSizeMB(Integer bufferSizeMb) {
            if (bufferSizeMb == null) return this;
            this.bufferSizeMb = bufferSizeMb;
            return this;
        }

        public S3DeliveryStreamBuilder withCompressionFormat(CompressionFormat compressionFormat) {
            if (compressionFormat == null) return this;
            this.compressionFormat = compressionFormat;
            return this;
        }

        public S3DeliveryStream build() {
            if (isEmpty(name)) throw new IllegalArgumentException("Delivery stream name cannot be empty");
            if (isEmpty(s3BucketArn)) throw new IllegalArgumentException("S3 Bucket ARN cannot be empty");
            return new S3DeliveryStream(name, s3Client, s3BucketArn, s3Prefix, bufferIntervalSeconds, bufferSizeMb, compressionFormat);
        }

    }

}