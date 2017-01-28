package io.timson.firehose.request;

import com.amazonaws.services.kinesisfirehose.model.CompressionFormat;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;

public class S3DeliveryStreamConfig {

    @JsonProperty("BucketARN")
    private String s3BucketArn;

    @JsonProperty("Prefix")
    private String s3Prefix;

    @JsonProperty("CompressionFormat")
    private CompressionFormat compressionFormat;

    private Integer bufferIntervalSeconds;
    private Integer bufferSizeMb;

    private Map<String, Object> otherProperties = new HashMap<>();

    @JsonProperty("BufferingHints")
    public void setBuffer(Map<String, Integer> bufferConfig) {
        this.bufferIntervalSeconds = bufferConfig.get("IntervalInSeconds");
        this.bufferSizeMb = bufferConfig.get("SizeInMBs");
    }

    @JsonAnyGetter
    public Map<String, Object> any() {
        return otherProperties;
    }

    @JsonAnySetter
    public void set(String name, Object value) {
        otherProperties.put(name, value);
    }

    public String getS3BucketArn() {
        return s3BucketArn;
    }

    public String getS3Prefix() {
        return s3Prefix;
    }

    public CompressionFormat getCompressionFormat() {
        return compressionFormat;
    }

    public Integer getBufferIntervalSeconds() {
        return bufferIntervalSeconds;
    }

    public Integer getBufferSizeMB() {
        return bufferSizeMb;
    }

}
