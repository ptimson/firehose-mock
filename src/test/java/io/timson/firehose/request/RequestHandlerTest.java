package io.timson.firehose.request;

import io.timson.firehose.stream.DeliveryStreamService;
import io.timson.firehose.test.TestUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RequestHandlerTest {

    @Mock
    private DeliveryStreamService deliveryStreamService;

    @Mock
    private HttpServletRequest request;

    @Mock
    private HttpServletResponse response;

    private RequestHandler requestHandler;

    @Before
    public void setUp() throws Exception {
        requestHandler = new RequestHandler(deliveryStreamService);
    }

    @Test
    public void shouldSetResponseTo200Success_WhenValidPutRequest() throws Exception {
        final String JSON = "{\"DeliveryStreamName\":\"myDeliveryStream\",\"Record\":{\"Data\":\"bXlEYXRh\"}}";
        when(request.getReader()).thenReturn(TestUtil.createBufferedReader(JSON));

        requestHandler.handlePutRequest(request, response);

        verify(response).setStatus(200);
    }

    @Test
    public void shouldSetResponseTo400BadRequest_WhenInvalidPutRequest() throws Exception {
        final String JSON = "invalid";
        when(request.getReader()).thenReturn(TestUtil.createBufferedReader(JSON));

        requestHandler.handlePutRequest(request, response);

        verify(response).setStatus(400);
    }

    @Test
    public void shouldSetResponseTo400BadRequest_WhenPutRequestMissingParams() throws Exception {
        final String JSON = "{}";
        when(request.getReader()).thenReturn(TestUtil.createBufferedReader(JSON));
        doThrow(new IllegalArgumentException("Missing args")).when(deliveryStreamService).write(isNull(String.class), isNull(String.class));

        requestHandler.handlePutRequest(request, response);

        verify(response).setStatus(400);
    }

    @Test
    public void shouldSetResponseTo200Success_WhenValidCreateStreamRequest() throws Exception {
        final String JSON = "{\"DeliveryStreamName\":\"myDeliveryStream\","
                + "\"ExtendedS3DestinationConfiguration\":{\"BucketARN\":\"arn:aws:s3:::scv-consumer-lambda-temp\","
                + "\"Prefix\":\"kfh/\",\"BufferingHints\":{\"SizeInMBs\":1,\"IntervalInSeconds\":4},"
                + "\"CompressionFormat\":\"GZIP\"}}";

        when(request.getReader()).thenReturn(TestUtil.createBufferedReader(JSON));

        requestHandler.handleCreateStreamRequest(request, response);

        verify(response).setStatus(200);
    }

    @Test
    public void shouldSetResponseTo400BadRequest_WhenInvalidCreateStreamRequest() throws Exception {
        final String JSON = "invalid";
        when(request.getReader()).thenReturn(TestUtil.createBufferedReader(JSON));

        requestHandler.handleCreateStreamRequest(request, response);

        verify(response).setStatus(400);
    }

    @Test
    public void shouldSetResponseTo400BadRequest_WhenCreateStreamRequestMissingParams() throws Exception {
        final String JSON = "{}";
        when(request.getReader()).thenReturn(TestUtil.createBufferedReader(JSON));
        doThrow(new IllegalArgumentException("Missing args")).when(deliveryStreamService).createStream(any(CreateDeliveryStreamRequest.class));

        requestHandler.handleCreateStreamRequest(request, response);

        verify(response).setStatus(400);
    }

    @Test
    public void shouldSetResponseTo200Success_WhenValidDeleteStreamRequest() throws Exception {
        final String JSON = "{\"DeliveryStreamName\":\"myDeliveryStream\"}";

        when(request.getReader()).thenReturn(TestUtil.createBufferedReader(JSON));

        requestHandler.handleDeleteStreamRequest(request, response);

        verify(response).setStatus(200);
    }

    @Test
    public void shouldSetResponseTo400BadRequest_WhenInvalidDeleteStreamRequest() throws Exception {
        final String JSON = "invalid";
        when(request.getReader()).thenReturn(TestUtil.createBufferedReader(JSON));

        requestHandler.handleDeleteStreamRequest(request, response);

        verify(response).setStatus(400);
    }

    @Test
    public void shouldSetResponseTo400BadRequest_WhenDeleteStreamRequestMissingParams() throws Exception {
        final String JSON = "{}";
        when(request.getReader()).thenReturn(TestUtil.createBufferedReader(JSON));
        doThrow(new IllegalArgumentException("Missing args")).when(deliveryStreamService).deleteStream(anyString());

        requestHandler.handleDeleteStreamRequest(request, response);

        verify(response).setStatus(400);
    }

}