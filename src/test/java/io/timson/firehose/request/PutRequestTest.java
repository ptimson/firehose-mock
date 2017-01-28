package io.timson.firehose.request;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class PutRequestTest {

    @Test
    public void shouldCreatePutRequest_WhenValidJson() throws Exception {
        final String JSON = "{\"DeliveryStreamName\":\"myDeliveryStream\",\"Record\":{\"Data\":\"bXlEYXRh\"}}";
        PutRequest putRequest = PutRequest.fromJson(JSON);

        assertThat(putRequest.getDeliveryStream(), is("myDeliveryStream"));
        assertThat(putRequest.getData(), is("myData"));
    }

}