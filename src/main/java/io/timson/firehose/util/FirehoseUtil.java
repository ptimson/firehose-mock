package io.timson.firehose.util;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;

public class FirehoseUtil {

    public static Integer randomFreePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to find a free port", e);
        }
    }

    public static boolean isEmpty(String str) {
        return (str == null || str.equals(""));
    }

}
