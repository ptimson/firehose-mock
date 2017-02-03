package io.timson.firehose.stream;

import io.timson.firehose.request.CreateDeliveryStreamRequest;
import io.timson.firehose.request.S3DeliveryStreamConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.HashSet;
import java.util.Set;

import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DeliveryStreamServiceTest {

    @Mock
    private DeliveryStreamFactory deliveryStreamFactory;

    @Mock
    private DeliveryStream deliveryStream;

    @Mock
    private CreateDeliveryStreamRequest createStreamRequest;

    @Mock
    private S3DeliveryStreamConfig s3Config;


    private DeliveryStreamService streamService;


    @Before
    public void setUp() throws Exception {
        streamService = new DeliveryStreamService(deliveryStreamFactory);
        when(createStreamRequest.getName()).thenReturn("myDeliveryStream");
        when(createStreamRequest.getS3DeliveryStreamRequest()).thenReturn(s3Config);
        when(s3Config.getS3BucketArn()).thenReturn("myBucketArn");
        when(deliveryStreamFactory.fromRequest(any())).thenReturn(deliveryStream);
        streamService.createStream(createStreamRequest);
    }

    @Test
    public void shouldWriteToDeliveryStream_WhenStreamExists() throws Exception {
        streamService.write("myDeliveryStream", "myData");

        verify(deliveryStream, times(1)).write("myData");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowException_WhenWriteToNonExistentStream() throws Exception {
        streamService.write("unknownStream", "myData");
    }

    @Test
    public void shouldCreateDeliveryStream_WhenOneWithSameNameDoesNotExist() throws Exception {
        when(createStreamRequest.getName()).thenReturn("newDeliveryStream");

        streamService.createStream(createStreamRequest);

        assertThat(streamService.listStreams().size(), is(2));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentException_WhenCreatingStreamWithSameName() throws Exception {
        streamService.createStream(createStreamRequest);
    }

    @Test
    public void shouldDeleteDeliveryStream_WhenExists() throws Exception {
        streamService.deleteStream("myDeliveryStream");

        assertThat(streamService.listStreams().size(), is(0));
        verify(deliveryStream, only()).stop();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentException_WhenStreamDoesNotExist() throws Exception {
        streamService.deleteStream("unknownStream");
    }

    @Test
    public void shouldListDeliveryStreamNames_WhenExist() throws Exception {
        Set<String> streamNames = streamService.listStreams();

        assertThat(streamNames, is(new HashSet<>(singletonList("myDeliveryStream"))));
    }

}