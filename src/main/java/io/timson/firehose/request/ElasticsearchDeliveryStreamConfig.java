package io.timson.firehose.request;

import com.amazonaws.services.kinesisfirehose.model.CompressionFormat;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;

public class ElasticsearchDeliveryStreamConfig {

    @JsonProperty("DomainARN")
    private String domainARN;

    @JsonProperty("IndexRotationPeriod")
    private String indexRotationPeriod;

    @JsonProperty("IndexName")
    private String indexName;

    @JsonProperty("TypeName")
    private String docType;

    @JsonProperty("BufferingHints")
    private BufferingHints bufferingHints;

    @JsonProperty("RoleARN")
    private String roleARN;

    private Map<String, Object> otherProperties = new HashMap<>();


    @JsonAnyGetter
    public Map<String, Object> any() {
        return otherProperties;
    }

    @JsonAnySetter
    public void set(String name, Object value) {
        otherProperties.put(name, value);
    }

    public String getIndexName() {
        return indexName;
    }

    public String getDocType() {
        return docType;
    }

    public String getIndexRotationPeriod() {
        return indexRotationPeriod;
    }

    public BufferingHints getBufferingHints() {
        return bufferingHints;
    }

    public String getDomainARN() {
        return domainARN;
    }

    public String getRoleARN() {
        return roleARN;
    }
}
