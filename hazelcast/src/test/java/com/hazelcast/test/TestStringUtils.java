package com.hazelcast.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import static com.hazelcast.nio.IOUtil.closeResource;

public class TestStringUtils {
    private static final int READ_BUFFER_SIZE = 8192;

    public static String fileAsText(File file) {
        FileInputStream stream = null;
        InputStreamReader streamReader = null;
        BufferedReader reader = null;
        try {
            stream = new FileInputStream(file);
            streamReader = new InputStreamReader(stream);
            reader = new BufferedReader(streamReader);

            StringBuilder builder = new StringBuilder();
            char[] buffer = new char[READ_BUFFER_SIZE];
            int read;
            while ((read = reader.read(buffer, 0, buffer.length)) > 0) {
                builder.append(buffer, 0, read);
            }
            return builder.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            closeResource(reader);
            closeResource(streamReader);
            closeResource(stream);
        }
    }
}
