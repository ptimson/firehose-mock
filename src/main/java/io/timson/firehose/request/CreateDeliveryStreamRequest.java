package io.timson.firehose.request;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CreateDeliveryStreamRequest {

    private static final ObjectMapper mapper = new ObjectMapper();

    @JsonProperty("DeliveryStreamName")
    private String name;

    @JsonProperty("ExtendedS3DestinationConfiguration")
    private S3DeliveryStreamConfig s3DeliveryStreamRequest;

    @JsonProperty("ElasticsearchDestinationConfiguration")
    private ElasticsearchDeliveryStreamConfig elasticsearchDeliveryStreamConfig;

    private Map<String, Object> otherProperties = new HashMap<>();

    @JsonAnyGetter
    public Map<String, Object> any() {
        return otherProperties;
    }

    @JsonAnySetter
    public void set(String name, Object value) {
        otherProperties.put(name, value);
    }

    public static CreateDeliveryStreamRequest fromJson(String json) throws IOException {
        return mapper.readValue(json, CreateDeliveryStreamRequest.class);
    }

    public String getName() {
        return name;
    }

    public S3DeliveryStreamConfig getS3DeliveryStreamRequest() {
        return s3DeliveryStreamRequest;
    }

    public ElasticsearchDeliveryStreamConfig getElasticsearchDeliveryStreamRequest() {
        return elasticsearchDeliveryStreamConfig;
    }
}
