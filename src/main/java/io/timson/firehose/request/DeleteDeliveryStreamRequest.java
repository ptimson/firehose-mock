package io.timson.firehose.request;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DeleteDeliveryStreamRequest {

    private static final ObjectMapper mapper = new ObjectMapper();

    @JsonProperty("DeliveryStreamName")
    private String name;

    private Map<String, Object> otherProperties = new HashMap<>();

    @JsonAnyGetter
    public Map<String, Object> any() {
        return otherProperties;
    }

    @JsonAnySetter
    public void set(String name, Object value) {
        otherProperties.put(name, value);
    }

    public static DeleteDeliveryStreamRequest fromJson(String json) throws IOException {
        return mapper.readValue(json, DeleteDeliveryStreamRequest.class);
    }

    public String getName() {
        return name;
    }

}
