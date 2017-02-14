package io.timson.firehose.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DeleteDeliveryStreamResponse {

    private static final ObjectMapper mapper = new ObjectMapper();

    @JsonProperty("DeliveryStreamName")
    private final String deliveryStreamName;

    public DeleteDeliveryStreamResponse(String deliveryStreamName) {
        this.deliveryStreamName = deliveryStreamName;
    }

    public String body() throws JsonProcessingException {
        return mapper.writeValueAsString(this);
    }

}
