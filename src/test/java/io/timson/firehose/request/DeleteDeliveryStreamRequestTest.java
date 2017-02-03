package io.timson.firehose.request;

import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

public class DeleteDeliveryStreamRequestTest {

    @Test
    public void shouldCreateDeleteStreamRequest_WhenValidJson() throws Exception {
        final String JSON = "{\"DeliveryStreamName\":\"myDeliveryStream\"}";
        DeleteDeliveryStreamRequest deleteRequest = DeleteDeliveryStreamRequest.fromJson(JSON);

        assertThat(deleteRequest.getName(), is("myDeliveryStream"));
    }

    @Test
    public void shouldCreateDeleteStreamRequest_WhenValidJsonWithExtraParams() throws Exception {
        final String JSON = "{\"DeliveryStreamName\":\"myDeliveryStream\", \"extra\":\"test\"}";
        DeleteDeliveryStreamRequest deleteRequest = DeleteDeliveryStreamRequest.fromJson(JSON);

        assertThat(deleteRequest.getName(), is("myDeliveryStream"));
    }

    @Test
    public void shouldCreateDeleteStreamRequestWithNullFields_WhenJsonMissingParams() throws Exception {
        final String JSON = "{}";
        DeleteDeliveryStreamRequest deleteRequest = DeleteDeliveryStreamRequest.fromJson(JSON);

        assertThat(deleteRequest.getName(), is(nullValue()));
    }

    @Test(expected = IOException.class)
    public void shouldThrowIOException_WhenInvalidJson() throws Exception {
        final String JSON = "invalid";
        DeleteDeliveryStreamRequest.fromJson(JSON);
    }

}