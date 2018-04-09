package io.timson.firehose.aws;

import com.amazonaws.services.kinesisfirehose.model.*;

public class ElasticsearchStreamRequestBuilder {
    private String roleARN;
    private String domainARN;
    private String indexName;
    private String typeName;
    private String indexRotationPeriod;
    private Integer bufferIntervalSeconds;
    private Integer bufferSizeMb;


    public ElasticsearchDestinationConfiguration build() {
        ElasticsearchDestinationConfiguration config = new ElasticsearchDestinationConfiguration();
        config.setRoleARN(roleARN);
        config.setDomainARN(domainARN);
        config.setTypeName(typeName);
        config.setIndexName(indexName);
        config.setIndexRotationPeriod(indexRotationPeriod);
        if(bufferIntervalSeconds != null || bufferSizeMb != null) {
            ElasticsearchBufferingHints bufferingHints = new ElasticsearchBufferingHints();
            if (bufferIntervalSeconds != null) bufferingHints.setIntervalInSeconds(bufferIntervalSeconds);
            if (bufferSizeMb != null) bufferingHints.setSizeInMBs(bufferSizeMb);
            config.setBufferingHints(bufferingHints);
        }
        return config;
    }

    public ElasticsearchStreamRequestBuilder withIndexName(String indexName) {
        this.indexName = indexName;
        return this;
    }

    public ElasticsearchStreamRequestBuilder withTypeName(String typeName) {
        this.typeName = typeName;
        return this;
    }

    public ElasticsearchStreamRequestBuilder withRoleARN(String roleARN) {
        this.roleARN = roleARN;
        return this;
    }

    public ElasticsearchStreamRequestBuilder withDomainARN(String domainARN) {
        this.domainARN = domainARN;
        return this;
    }

    public ElasticsearchStreamRequestBuilder withIndexRotationPeriod(String indexRotationPeriod) {
        this.indexRotationPeriod = indexRotationPeriod;
        return this;
    }

    public ElasticsearchStreamRequestBuilder withBufferIntervalSeconds(Integer bufferIntervalSeconds) {
        this.bufferIntervalSeconds = bufferIntervalSeconds;
        return this;
    }

    public ElasticsearchStreamRequestBuilder withBufferSizeMb(Integer bufferSizeMb) {
        this.bufferSizeMb = bufferSizeMb;
        return this;
    }
}
