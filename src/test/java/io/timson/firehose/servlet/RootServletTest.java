package io.timson.firehose.servlet;

import io.timson.firehose.request.RequestHandler;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import static org.mockito.Mockito.only;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RootServletTest {

    private static final String AMZ_TARGET_HEADER = "X-Amz-Target";

    @Mock
    private RequestHandler requestHandler;

    @Mock
    private HttpServletRequest request;

    @Mock
    private HttpServletResponse response;

    private RootServlet servlet;

    @Before
    public void setUp() throws Exception {
        servlet = new RootServlet(requestHandler);
    }

    @Test
    public void shouldHandlePutRequest_WhenRequestHeaderContainsPutRecord() throws Exception {
        when(request.getHeader(AMZ_TARGET_HEADER)).thenReturn("Firehose_20150804.PutRecord");

        servlet.doPost(request, response);

        verify(requestHandler, only()).handlePutRequest(request, response);
    }

    @Test
    public void shouldHandleCreateStreamRequest_WhenRequestHeaderContainsCreateStream() throws Exception {
        when(request.getHeader(AMZ_TARGET_HEADER)).thenReturn("Firehose_20150804.CreateDeliveryStream");

        servlet.doPost(request, response);

        verify(requestHandler, only()).handleCreateStreamRequest(request, response);
    }

    @Test
    public void shouldHandleDeleteStreamRequest_WhenRequestHeaderContainsDeleteStream() throws Exception {
        when(request.getHeader(AMZ_TARGET_HEADER)).thenReturn("Firehose_20150804.DeleteDeliveryStream");

        servlet.doPost(request, response);

        verify(requestHandler, only()).handleDeleteStreamRequest(request, response);
    }

    @Test
    public void shouldSet400BadRequest_WhenRequestHeaderHasInvalidPrefix() throws Exception {
        when(request.getHeader(AMZ_TARGET_HEADER)).thenReturn("InvalidPrefix.PutRecord");

        servlet.doPost(request, response);

        verifyNoMoreInteractions(requestHandler);
        verify(response).setStatus(400);
    }

    @Test
    public void shouldSet400BadRequest_WhenRequestHeaderHasInvalidAction() throws Exception {
        when(request.getHeader(AMZ_TARGET_HEADER)).thenReturn("Firehose_20150804.InvalidAction");

        servlet.doPost(request, response);

        verifyNoMoreInteractions(requestHandler);
        verify(response).setStatus(400);
    }

    @Test
    public void shouldSet400BadRequest_WhenRequestHeaderIsMissing() throws Exception {
        servlet.doPost(request, response);

        verifyNoMoreInteractions(requestHandler);
        verify(response).setStatus(400);
    }

}