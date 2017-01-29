package io.timson.firehose.test;

import java.util.Arrays;

public class TestUtil {

    public static String createStringOfSize(int bytes) {
        char[] chars = new char[bytes];
        Arrays.fill(chars, 'a');
        return new String(chars);
    }

}
