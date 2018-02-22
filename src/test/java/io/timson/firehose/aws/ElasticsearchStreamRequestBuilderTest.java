package io.timson.firehose.aws;

import com.amazonaws.services.kinesisfirehose.model.ElasticsearchDestinationConfiguration;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class ElasticsearchStreamRequestBuilderTest {

    @Test
    public void shouldCreateElasticsearchConfig_WhenAllParamsProvided() throws Exception {
        ElasticsearchStreamRequestBuilder builder = new ElasticsearchStreamRequestBuilder();

        ElasticsearchDestinationConfiguration config = builder
                .withIndexName("index")
                .withTypeName("type")
                .withDomainARN("domainARN")
                .withroleARN("roleARN")
                .withIndexRotationPeriod("rotation")
                .withBufferSizeMb(1)
                .withBufferIntervalSeconds(2)
                .build();

        assertThat(config.getIndexName(), is("index"));
        assertThat(config.getTypeName(), is("type"));
        assertThat(config.getIndexRotationPeriod(), is("rotation"));
        assertThat(config.getDomainARN(), is("domainARN"));
        assertThat(config.getRoleARN(), is("roleARN"));
        assertThat(config.getBufferingHints().getSizeInMBs(), is(1));
        assertThat(config.getBufferingHints().getIntervalInSeconds(), is(2));
    }

    @Test
    public void shouldCreateElasticsearchConfig_WhenBufferParamsMissing() throws Exception {
        ElasticsearchStreamRequestBuilder builder = new ElasticsearchStreamRequestBuilder();

        ElasticsearchDestinationConfiguration config = builder
                .withIndexName("index")
                .withTypeName("type")
                .withDomainARN("domainARN")
                .withroleARN("roleARN")
                .withIndexRotationPeriod("rotation")
                .build();

        assertThat(config.getIndexName(), is("index"));
        assertThat(config.getTypeName(), is("type"));
        assertThat(config.getIndexRotationPeriod(), is("rotation"));
        assertThat(config.getDomainARN(), is("domainARN"));
        assertThat(config.getRoleARN(), is("roleARN"));
        assertThat(config.getBufferingHints(), is(nullValue()));
    }
}