package io.timson.firehose.request;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ElasticSearchDeliveryStreamConfig {

    @JsonProperty("IndexName")
    private String indexName;

    @JsonProperty("TypeName")
    private String docType;

    public String getIndexName() {
        return indexName;
    }

    public String getDocType() {
        return docType;
    }
}
