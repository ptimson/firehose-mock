package io.timson.firehose.request;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Map;

import static com.sun.xml.internal.messaging.saaj.util.Base64.base64Decode;

public class PutRequest {

    private static final ObjectMapper mapper = new ObjectMapper();

    @JsonProperty("DeliveryStreamName")
    private String deliveryStream;

    private String data;

    @JsonProperty("Record")
    public void setData(Map<String, String> record) {
        String encodedData = record.get("Data");
        this.data = base64Decode(encodedData);
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
