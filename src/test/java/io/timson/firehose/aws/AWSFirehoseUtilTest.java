package io.timson.firehose.aws;

import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.model.CreateDeliveryStreamRequest;
import com.amazonaws.services.kinesisfirehose.model.DeleteDeliveryStreamRequest;
import com.amazonaws.services.kinesisfirehose.model.ExtendedS3DestinationConfiguration;
import com.amazonaws.services.kinesisfirehose.model.PutRecordRequest;
import org.junit.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class AWSFirehoseUtilTest {

    @Test
    public void shouldCreateFirehoseClient_WhenGivenEndpointAndRegion() throws Exception {
        AmazonKinesisFirehose client = AWSFirehoseUtil.createClient("127.0.0.1:7070", "eu-west-1");

        assertThat(client, not(nullValue()));
    }

    @Test
    public void shouldCreatePutRequest_WhenGivenStreamNameAndData() throws Exception {
        String deliveryStreamName = "myStreamName";
        String data = "myData";

        PutRecordRequest putRequest = AWSFirehoseUtil.createPutRequest(deliveryStreamName, data);

        assertThat(new String(putRequest.getRecord().getData().array(), UTF_8), is(data));
        assertThat(putRequest.getDeliveryStreamName(), is(deliveryStreamName));
    }

    @Test
    public void shouldCreateDeliveryStreamRequest_WhenGivenStreamNameAndConfig() throws Exception {
        String deliveryStreamName = "myStreamName";
        ExtendedS3DestinationConfiguration s3Config = new ExtendedS3DestinationConfiguration();

        CreateDeliveryStreamRequest request = AWSFirehoseUtil.createS3DeliveryStreamRequest(deliveryStreamName, s3Config);

        assertThat(request.getDeliveryStreamName(), is(deliveryStreamName));
        assertThat(request.getExtendedS3DestinationConfiguration(), is(s3Config));
    }

    @Test
    public void shouldCreateDeleteDeliveryStreamRequest_WhenGivenStreamName() throws Exception {
        String deliveryStreamName = "myStreamName";

        DeleteDeliveryStreamRequest request = AWSFirehoseUtil.deleteDeliveryStreamRequest(deliveryStreamName);

        assertThat(request.getDeliveryStreamName(), is(deliveryStreamName));
    }

}