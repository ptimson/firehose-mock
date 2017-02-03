package io.timson.firehose.servlet;

import io.timson.firehose.request.RequestHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

import static javax.servlet.http.HttpServletResponse.SC_BAD_REQUEST;


public class RootServlet extends HttpServlet {

    private final RequestHandler requestHandler;

    public RootServlet(RequestHandler requestHandler) {
        this.requestHandler = requestHandler;
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        final String amzTarget = extractAmzTarget(request);

        switch (amzTarget) {
            case "PutRecord":
                requestHandler.handlePutRequest(request, response);
                return;
            case "CreateDeliveryStream":
                requestHandler.handleCreateStreamRequest(request, response);
                return;
            case "DeleteDeliveryStream":
                requestHandler.handleDeleteStreamRequest(request, response);
                return;
            default:
                response.setStatus(SC_BAD_REQUEST);
        }

    }

    private String extractAmzTarget(HttpServletRequest request) {
        final String amzTarget = request.getHeader("X-Amz-Target");
        if (amzTarget == null || !amzTarget.startsWith("Firehose_20150804.")) {
            return "Unknown";
        }
        return amzTarget.substring("Firehose_20150804.".length());
    }

}
