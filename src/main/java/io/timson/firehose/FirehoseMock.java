package io.timson.firehose;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.timson.firehose.aws.S3Client;
import io.timson.firehose.request.RequestHandler;
import io.timson.firehose.servlet.RootServlet;
import io.timson.firehose.stream.DeliveryStreamFactory;
import io.timson.firehose.stream.DeliveryStreamService;
import io.timson.firehose.util.FirehoseUtil;
import org.apache.http.impl.client.HttpClientBuilder;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import java.util.HashSet;
import java.util.Set;

public class FirehoseMock {

    private final Server server;

    private final Integer port;

    private final DeliveryStreamService deliveryStreamService;


    private FirehoseMock(Integer port, AmazonS3 amazonS3Client, JestClient jestClient) {
        this.port = port;

        server = new Server(port);

        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        server.setHandler(context);

        S3Client s3Client = new S3Client(amazonS3Client);
        DeliveryStreamFactory deliveryStreamFactory = new DeliveryStreamFactory(s3Client, jestClient);
        deliveryStreamService = new DeliveryStreamService(deliveryStreamFactory);

        RequestHandler requestHandler = new RequestHandler(deliveryStreamService);
        context.addServlet(new ServletHolder(new RootServlet(requestHandler)), "/*");
    }

    /**
     * Start the Firehose Mock
     *
     * @throws Exception if could not start
     */
    public void start() throws Exception {
        if (!server.isStarted()) {
            server.start();
            while (!server.isRunning()) {
                Thread.sleep(1);
            }
        }
    }

    /**
     * Stop the Firehose Mock &amp; Delete Delivery Streams
     *
     * @throws Exception if could not be stopped
     */
    public void stop() throws Exception {
        if (server.isRunning()) {
            Set<String> streams = new HashSet<>(deliveryStreamService.listStreams());
            streams.forEach(deliveryStreamService::deleteStream);
            server.stop();
            while (server.isRunning()) {
                Thread.sleep(1);
            }
        }
    }

    /**
     * Port Firehose Mock is setup to run on
     *
     * @return Number of Firehose Port
     */
    public Integer getPort() {
        return port;
    }

    /**
     * Create a Firehose Mock server with a random free port and a default S3 Client.
     *
     * @return A FirehoseMock object.
     */
    public static FirehoseMock defaultMock() {
        return new Builder().build();
    }

    /**
     * Builder to create a custom Firehose Mock Sever.
     */
    public static class Builder {
        private Integer port;
        private AmazonS3 amazonS3Client;
        private JestClient jestClient;

        public Builder withPort(Integer port) {
            this.port = port;
            return this;
        }

        public Builder withAmazonS3Client(AmazonS3 amazonS3Client) {
            this.amazonS3Client = amazonS3Client;
            return this;
        }

        public Builder withJestClient(JestClient jestClient) {
            this.jestClient = jestClient;
            return this;
        }

        public FirehoseMock build() {
            if (port == null) {
                port = FirehoseUtil.randomFreePort();
            }
            if (amazonS3Client == null) {
                amazonS3Client = AmazonS3ClientBuilder.defaultClient();
            }
            if(jestClient == null) {
                jestClient = new JestClientFactory().getObject();
            }
            return new FirehoseMock(port, amazonS3Client, jestClient);
        }

    }

}
