package io.timson.firehose.stream;

import io.searchbox.client.JestClient;
import io.searchbox.core.DocumentResult;
import io.searchbox.core.Index;

import java.io.IOException;
import java.util.logging.Logger;

public class ElasticSearchDeliveryStream implements DeliveryStream {
    private static final Logger logger = Logger.getLogger(S3DeliveryStream.class.getName());

    private final JestClient jestClient;
    private final String streamName;
    private final String indexName;
    private final String docType;

    public ElasticSearchDeliveryStream(String streamName, JestClient jestClient, String indexName, String docType) {
        this.jestClient = jestClient;
        this.streamName = streamName;
        this.indexName = indexName;
        this.docType = docType;
    }

    @Override
    public void write(String data) {
        try {
            DocumentResult result = result = jestClient.execute(new Index.Builder(data).index(indexName).type(docType).build());
            if (result.isSucceeded()) {
                logger.info("ES object added ");
            } else {
                logger.info("Failed to add ES object. Error: " + result.getErrorMessage());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stop() {
        jestClient.shutdownClient();
    }

    @Override
    public String getName() {
        return streamName;
    }
}
