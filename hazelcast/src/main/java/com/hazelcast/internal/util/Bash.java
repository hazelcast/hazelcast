package com.hazelcast.internal.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Bash {

    public static String bash(String command) {
        String[] cmd = {"bash", "-c", command};
        try {
            ProcessBuilder processBuilder = new ProcessBuilder(cmd);
            processBuilder.redirectError();
            Process process = processBuilder.start();

            int result = process.waitFor();
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            StringBuilder builder = new StringBuilder();
            String line;
            boolean first = true;
            while ((line = reader.readLine()) != null) {
                if (first) {
                    first = false;
                } else {
                    builder.append(System.getProperty("line.separator"));
                }
                builder.append(line);
            }
            if (result != 0) {
                System.out.println(builder.toString());
                throw new RuntimeException("Bash ["+command + "] failed with exit code:"+result);
            }

            return builder.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
