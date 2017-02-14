package io.timson.firehose.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class CreateDeliveryStreamResponse {

    private static final ObjectMapper mapper = new ObjectMapper();

    private static final String ARN_TEMPLATE = "arn:aws:firehose:us-east-1:123456789012:deliverystream/%s";

    @JsonProperty("DeliveryStreamARN")
    private final String deliveryStreamArn;

    public CreateDeliveryStreamResponse(String deliveryStreamName) {
        this.deliveryStreamArn = String.format(ARN_TEMPLATE, deliveryStreamName);
    }

    public String body() throws JsonProcessingException {
        return mapper.writeValueAsString(this);
    }

}
