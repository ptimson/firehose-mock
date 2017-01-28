package io.timson.firehose.request;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import io.timson.firehose.stream.DeliveryStreamService;
import org.apache.commons.io.IOUtils;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;

public class RequestHandler {

    private final DeliveryStreamService deliveryStreamService;

    public RequestHandler(DeliveryStreamService deliveryStreamService) {
        this.deliveryStreamService = deliveryStreamService;
    }

    public void handlePutRequest(HttpServletRequest request, HttpServletResponse response) throws IOException {

        try {
            String json = extractRequestBody(request);
            PutRequest putRequest = PutRequest.fromJson(json);
            deliveryStreamService.write(putRequest.getDeliveryStream(), putRequest.getData());
        } catch (IllegalArgumentException | JsonParseException | JsonMappingException e) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }

        response.setStatus(HttpServletResponse.SC_OK);
    }

    public void handleCreateStreamRequest(HttpServletRequest request, HttpServletResponse response) throws IOException {

        try {
            String json = extractRequestBody(request);
            CreateDeliveryStreamRequest createDeliveryStreamRequest = CreateDeliveryStreamRequest.fromJson(json);
            deliveryStreamService.createStream(createDeliveryStreamRequest);
        } catch (IllegalArgumentException | JsonParseException | JsonMappingException e) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }

        response.setStatus(HttpServletResponse.SC_OK);
    }

    public void handleDeleteStreamRequest(HttpServletRequest request, HttpServletResponse response) throws IOException {

        try {
            String json = extractRequestBody(request);
            DeleteDeliveryStreamRequest deleteDeliveryStreamRequest = DeleteDeliveryStreamRequest.fromJson(json);
            deliveryStreamService.deleteStream(deleteDeliveryStreamRequest.getName());
        } catch (IllegalArgumentException | JsonParseException | JsonMappingException e) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }

        response.setStatus(HttpServletResponse.SC_OK);
    }

    private String extractRequestBody(HttpServletRequest request) throws IOException {
        final BufferedReader reader = request.getReader();
        return IOUtils.toString(reader);
    }

}
