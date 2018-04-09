package io.timson.firehose.request;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;

public class BufferingHints {

    @JsonProperty("IntervalInSeconds")
    private Integer bufferIntervalSeconds;

    @JsonProperty("SizeInMBs")
    private Integer bufferSizeMb;

    private Map<String, Object> otherProperties = new HashMap<>();

    @JsonAnyGetter
    public Map<String, Object> any() {
        return otherProperties;
    }

    @JsonAnySetter
    public void set(String name, Object value) {
        otherProperties.put(name, value);
    }

    public Integer getBufferIntervalSeconds() {
        return bufferIntervalSeconds;
    }

    public Integer getBufferSizeMB() {
        return bufferSizeMb;
    }
}
