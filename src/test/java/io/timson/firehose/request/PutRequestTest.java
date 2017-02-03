package io.timson.firehose.request;

import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

public class PutRequestTest {

    @Test
    public void shouldCreatePutRequest_WhenValidJson() throws Exception {
        final String JSON = "{\"DeliveryStreamName\":\"myDeliveryStream\",\"Record\":{\"Data\":\"bXlEYXRh\"}}";
        PutRequest putRequest = PutRequest.fromJson(JSON);

        assertThat(putRequest.getDeliveryStream(), is("myDeliveryStream"));
        assertThat(putRequest.getData(), is("myData"));
    }

    @Test
    public void shouldCreatePutRequestWithNullFields_WhenJsonMissingParams() throws Exception {
        final String JSON = "{}";
        PutRequest putRequest = PutRequest.fromJson(JSON);

        assertThat(putRequest.getDeliveryStream(), is(nullValue()));
        assertThat(putRequest.getData(), is(nullValue()));
    }

    @Test(expected = IOException.class)
    public void shouldThrowIOException_WhenInvalidJson() throws Exception {
        final String JSON = "invalid";
        PutRequest.fromJson(JSON);
    }

    @Test(expected = UnrecognizedPropertyException.class)
    public void shouldThrowUnrecognizedException_WhenValidJsonWithExtraParams() throws Exception {
        final String JSON = "{\"DeliveryStreamName\":\"myDeliveryStream\",\"Record\":{\"Data\":\"bXlEYXRh\"},\"extra\":\"test\"}";
        PutRequest.fromJson(JSON);
    }

}