package io.timson.firehose.request;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Base64;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

public class PutRequest {

    private static final ObjectMapper mapper = new ObjectMapper();

    @JsonProperty("DeliveryStreamName")
    private String deliveryStream;

    private String data;

    @JsonProperty("Record")
    public void setData(Map<String, String> record) {
        String encodedData = record.get("Data");
        byte[] decodedData = Base64.getDecoder().decode(encodedData);
        this.data = new String(decodedData, UTF_8);
    }

    public static PutRequest fromJson(String json) throws IOException {
        return mapper.readValue(json, PutRequest.class);
    }

    public String getDeliveryStream() {
        return deliveryStream;
    }

    public String getData() {
        return data;
    }

}
