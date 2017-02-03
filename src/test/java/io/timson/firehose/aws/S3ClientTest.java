package io.timson.firehose.aws;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.DataFormatException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class S3ClientTest {

    private static final String BUCKET = "myBucket";
    private static final String KEY = "myKey";

    private ArgumentCaptor<PutObjectRequest> putObjectRequestCaptor;

    @Mock
    private AmazonS3 amazonS3;

    private S3Client s3Client;

    @Before
    public void setUp() throws Exception {
        s3Client = new S3Client(amazonS3);
        putObjectRequestCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
    }

    @Test
    public void shouldCreateS3ObjectWhenPassedString() throws Exception {

        s3Client.createObject(BUCKET, KEY, "myString");

        verify(amazonS3, times(1)).putObject(putObjectRequestCaptor.capture());
        PutObjectRequest putObjectRequest = putObjectRequestCaptor.getValue();
        byte[] bytes = IOUtils.toByteArray(putObjectRequest.getInputStream());
        assertThat(bytes, is("myString".getBytes()));
    }

    @Test
    public void createGzipObject() throws Exception {
        byte[] expectedBytes = {31, -117, 8, 0, 0, 0, 0, 0, 0, 0, -53, -83, 12, 46, 41, -54, -52, 75, 7, 0, -57, -23, -20, 127, 8, 0, 0, 0};

        s3Client.createGzipObject(BUCKET, KEY, "myString");

        verify(amazonS3, times(1)).putObject(putObjectRequestCaptor.capture());
        PutObjectRequest putObjectRequest = putObjectRequestCaptor.getValue();
        byte[] bytes = IOUtils.toByteArray(putObjectRequest.getInputStream());
        assertThat(bytes, is(expectedBytes));
    }

    @Test
    public void createZipObject() throws Exception {
        s3Client.createZipObject(BUCKET, KEY, "myString");

        verify(amazonS3, times(1)).putObject(putObjectRequestCaptor.capture());
        PutObjectRequest putObjectRequest = putObjectRequestCaptor.getValue();
        byte[] bytes = IOUtils.toByteArray(putObjectRequest.getInputStream());
        assertThat(unzip(bytes), is("myString".getBytes()));
    }

    @Test
    public void createSnappyObject() throws Exception {
        byte[] expectedBytes = {8, 28, 109, 121, 83, 116, 114, 105, 110, 103};

        s3Client.createSnappyObject(BUCKET, KEY, "myString");

        verify(amazonS3, times(1)).putObject(putObjectRequestCaptor.capture());
        PutObjectRequest putObjectRequest = putObjectRequestCaptor.getValue();
        byte[] bytes = IOUtils.toByteArray(putObjectRequest.getInputStream());
        assertThat(bytes, is(expectedBytes));
    }

    private static byte[] unzip(byte[] bytes) throws DataFormatException, IOException {
        final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        final ZipInputStream input = new ZipInputStream(byteArrayInputStream);
        final ZipEntry entry = input.getNextEntry();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] byteBuff = new byte[1024];
        int bytesRead;
        while ((bytesRead = input.read(byteBuff)) != -1) {
            out.write(byteBuff, 0, bytesRead);
        }
        out.close();
        return out.toByteArray();
    }

}