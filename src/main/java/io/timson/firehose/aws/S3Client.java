package io.timson.firehose.aws;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipOutputStream;

import static java.nio.charset.StandardCharsets.UTF_8;

public class S3Client {

    private final AmazonS3 s3Client;

    public S3Client(AmazonS3 s3Client) {
        this.s3Client = s3Client;
    }

    public void createObject(String bucket, String path, String content) {
        byte[] contentBytes = content.getBytes(UTF_8);
        createS3Object(bucket, path, contentBytes);
    }

    public void createGzipObject(String bucket, String path, String content) throws IOException {
        byte[] zippedBytes = gzipBytes(content.getBytes(UTF_8));
        createS3Object(bucket, path, zippedBytes);
    }

    public void createZipObject(String bucket, String path, String content) throws IOException {
        byte[] zippedBytes = zipBytes(content.getBytes(UTF_8));
        createS3Object(bucket, path, zippedBytes);
    }

    public void createSnappyObject(String bucket, String path, String content) throws IOException {
        byte[] compressed = Snappy.compress(content.getBytes(UTF_8));
        createS3Object(bucket, path, compressed);
    }

    private void createS3Object(String bucket, String path, byte[] contentBytes) {
        ByteArrayInputStream contentStream = new ByteArrayInputStream(contentBytes);
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(contentBytes.length);
        s3Client.putObject(new PutObjectRequest(bucket, path, contentStream, metadata));
    }

    private byte[] gzipBytes(byte[] bytes) throws IOException {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        GZIPOutputStream out = new GZIPOutputStream(stream);
        out.write(bytes, 0, bytes.length);
        out.finish();
        out.close();
        return stream.toByteArray();
    }

    private byte[] zipBytes(byte[] bytes) throws IOException {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        ZipOutputStream out = new ZipOutputStream(stream);
        out.write(bytes, 0, bytes.length);
        out.finish();
        out.close();
        return stream.toByteArray();
    }

}
