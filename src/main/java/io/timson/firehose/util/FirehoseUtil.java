package io.timson.firehose.util;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.util.Random;

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

    public static String randomString(String chars, int length) {
        StringBuilder sb = new StringBuilder();
        Random random = new Random();
        while (sb.length() < length) {
            int index = (int) (random.nextFloat() * chars.length());
            sb.append(chars.charAt(index));
        }
        return sb.toString();
    }

}
