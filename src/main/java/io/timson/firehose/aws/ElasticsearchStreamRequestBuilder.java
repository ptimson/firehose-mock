package io.timson.firehose.aws;

import com.amazonaws.services.kinesisfirehose.model.ElasticsearchDestinationConfiguration;

public class ElasticsearchStreamRequestBuilder {

    private String indexName;
    private String docType;

    public ElasticsearchDestinationConfiguration build() {
        ElasticsearchDestinationConfiguration config = new ElasticsearchDestinationConfiguration();
        config.setTypeName(docType);
        config.setIndexName(indexName);
        return config;
    }

    public ElasticsearchStreamRequestBuilder withIndexName(String indexName) {
        this.indexName = indexName;
        return this;
    }

    public ElasticsearchStreamRequestBuilder withDocType(String docType) {
        this.docType = docType;
        return this;
    }
}
