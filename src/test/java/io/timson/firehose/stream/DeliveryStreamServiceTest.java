package io.timson.firehose.stream;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DeliveryStreamServiceTest {

    @Mock
    private DeliveryStreamFactory deliveryStreamFactory;

    @Mock
    private DeliveryStream deliveryStream;

    private DeliveryStreamService streamService;


    @Test
    public void shouldWriteToDeliveryStream_WhenStreamExists() throws Exception {
        when(deliveryStream.getName()).thenReturn("myDeliveryStream");
        streamService = new DeliveryStreamService(deliveryStreamFactory);

        streamService.write("myDeliveryStream", "myData");

        verify(deliveryStream, times(1)).write("myData");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowException_WhenWriteToNonExistentStream() throws Exception {
        streamService = new DeliveryStreamService(deliveryStreamFactory);

        streamService.write("myDeliveryStream", "myData");
    }

}