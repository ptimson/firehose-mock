package io.timson.firehose.stream;

import com.amazonaws.services.kinesisfirehose.model.CompressionFormat;
import io.searchbox.client.JestClient;
import io.searchbox.core.DocumentResult;
import io.searchbox.core.Index;

import java.io.IOException;
import java.util.logging.Logger;

import static com.amazonaws.services.kinesisfirehose.model.CompressionFormat.UNCOMPRESSED;
import static io.timson.firehose.util.FirehoseUtil.isEmpty;

public class ElasticsearchDeliveryStream implements DeliveryStream {
    private static final Logger logger = Logger.getLogger(ElasticsearchDeliveryStream.class.getName());

    private static final long MEGABYTE = 1024L * 1024L;

    private final JestClient jestClient;
    private final String streamName;
    private final String indexName;
    private final String typeName;
    private final String indexRotationPeriod;
    private final Long bufferIntervalSeconds;
    private final Long bufferSizeMB;

    public ElasticsearchDeliveryStream(JestClient jestClient,
                                       String streamName,
                                       String indexName,
                                       String typeName,
                                       String indexRotationPeriod,
                                       Long bufferIntervalSeconds,
                                       Long bufferSizeMB) {
        this.jestClient = jestClient;
        this.streamName = streamName;
        this.indexName = indexName;
        this.typeName = typeName;
        this.indexRotationPeriod = indexRotationPeriod;
        this.bufferIntervalSeconds = bufferIntervalSeconds;
        this.bufferSizeMB = bufferSizeMB;
    }

    @Override
    public void write(String data) {
        //TODO add buffering before writing to ES like in true Firehose. See S3DeliveryStream
        try {
            DocumentResult result = jestClient.execute(new Index.Builder(data).index(indexName).type(typeName).build());
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

    public static class ElasticsearchDeliveryStreamBuilder {
        private JestClient jestClient;
        private String streamName;
        private String indexName;
        private String typeName;
        private String domainARN;
        private String roleARN;
        private String indexRotationPeriod = "MONTH";
        private Long bufferIntervalSeconds = 300 * 1000L; // 300 s
        private Long bufferSizeMB = 5 * MEGABYTE; // 5 MiB

        public ElasticsearchDeliveryStreamBuilder withJestClient(JestClient jestClient) {
            this.jestClient = jestClient;
            return this;
        }

        public ElasticsearchDeliveryStreamBuilder withStreamName(String streamName) {
            this.streamName = streamName;
            return this;
        }

        public ElasticsearchDeliveryStreamBuilder withIndexName(String indexName) {
            this.indexName = indexName;
            return this;
        }

        public ElasticsearchDeliveryStreamBuilder withDocType(String typeName) {
            this.typeName = typeName;
            return this;
        }

        public ElasticsearchDeliveryStreamBuilder withDomainARN(String domainARN) {
            this.domainARN = domainARN;
            return this;
        }

        public ElasticsearchDeliveryStreamBuilder withRoleARN(String roleARN) {
            this.roleARN = roleARN;
            return this;
        }

        public ElasticsearchDeliveryStreamBuilder withIndexRotationPeriod(String indexRotationPeriod) {
            if(indexRotationPeriod == null) return this;
            this.indexRotationPeriod = indexRotationPeriod;
            return this;
        }

        public ElasticsearchDeliveryStreamBuilder withBufferIntervalSeconds(Integer bufferIntervalSeconds) {
            if (bufferIntervalSeconds == null) return this;
            this.bufferIntervalSeconds = bufferIntervalSeconds * 1000L;
            return this;
        }

        public ElasticsearchDeliveryStreamBuilder withBufferSizeMB(Integer bufferSizeMB) {
            if (bufferSizeMB == null) return this;
            this.bufferSizeMB = bufferSizeMB * MEGABYTE;
            return this;
        }

        public ElasticsearchDeliveryStream build() {
            if (isEmpty(streamName)) throw new IllegalArgumentException("Delivery stream name cannot be empty");
            if (isEmpty(indexName)) throw new IllegalArgumentException("ES index name cannot be empty");
            if (isEmpty(typeName)) throw new IllegalArgumentException("ES type name cannot be empty");
            if (isEmpty(domainARN)) throw new IllegalArgumentException("Domain ARN cannot be empty");
            if (isEmpty(roleARN)) throw new IllegalArgumentException("Role ARN cannot be empty");
            //TODO The configuration for the backup Amazon S3 location is also mandatory (by docs)
            return new ElasticsearchDeliveryStream(jestClient, streamName, indexName, typeName, indexRotationPeriod, bufferIntervalSeconds, bufferSizeMB);
        }
    }
}
