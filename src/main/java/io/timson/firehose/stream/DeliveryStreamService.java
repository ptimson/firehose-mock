package io.timson.firehose.stream;

import io.timson.firehose.request.CreateDeliveryStreamRequest;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class DeliveryStreamService {

    private final Map<String, DeliveryStream> deliveryStreams = new HashMap<>();

    private final DeliveryStreamFactory deliveryStreamFactory;

    public DeliveryStreamService(DeliveryStreamFactory deliveryStreamFactory) {
        this.deliveryStreamFactory = deliveryStreamFactory;
    }

    public void write(String deliveryStream, String data) {
        DeliveryStream stream = getDeliveryStream(deliveryStream);
        stream.write(data);
    }

    public void createStream(CreateDeliveryStreamRequest createStreamRequest) {
        final String name = createStreamRequest.getName();
        if (deliveryStreams.containsKey(name)) {
            throw new IllegalArgumentException(String.format("Stream with name %s already exists", name));
        }
        DeliveryStream deliveryStream = deliveryStreamFactory.fromRequest(createStreamRequest);
        deliveryStreams.put(name, deliveryStream);
    }

    public void deleteStream(String name) {
        if (!deliveryStreams.containsKey(name)) {
            throw new IllegalArgumentException(String.format("Stream with name %s does not exists", name));
        }
        DeliveryStream deliveryStream = getDeliveryStream(name);
        deliveryStream.stop();
        deliveryStreams.remove(name);
    }

    public Set<String> listStreams() {
        return deliveryStreams.keySet();
    }

    private DeliveryStream getDeliveryStream(String name) {
        DeliveryStream stream = deliveryStreams.get(name);
        if (stream == null) {
            throw new IllegalArgumentException(String.format("Unknown delivery stream %s", name));
        }
        return stream;
    }

}
