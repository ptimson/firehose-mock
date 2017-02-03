package io.timson.firehose.test;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.util.Arrays;

public class TestUtil {

    public static String createStringOfSize(int bytes) {
        char[] chars = new char[bytes];
        Arrays.fill(chars, 'a');
        return new String(chars);
    }

    public static BufferedReader createBufferedReader(String str){
        return new BufferedReader(new InputStreamReader(new ByteArrayInputStream(str.getBytes())));
    }

}
