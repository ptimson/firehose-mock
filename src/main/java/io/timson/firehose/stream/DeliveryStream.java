package io.timson.firehose.stream;

public interface DeliveryStream {

    void write(String data);

    void stop();

    String getName();

}
