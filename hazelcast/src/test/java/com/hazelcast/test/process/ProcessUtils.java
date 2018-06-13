package com.hazelcast.test.process;

import com.hazelcast.util.StringUtil;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;

import static com.hazelcast.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.util.StringUtil.LINE_SEPARATOR;

/**
 * Utility to deal with processes.
 * This is intended to be used in tests only.
 *
 */
public final class ProcessUtils {
    private ProcessUtils() {

    }

    /**
     * Best effort utility to get PID of own JVM
     *
     * @return own process ID or null
     */
    public static Long getOwnPidOrNull() {
        // there is no public API to get own PID in pre-JDK9
        // this is a hack which may or may not work depending on JDK impl
        final String jvmName = ManagementFactory.getRuntimeMXBean().getName();
        final int index = jvmName.indexOf('@');
        if (index < 1) {
            return null;
        }
        try {
            return Long.parseLong(jvmName.substring(0, index));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * Executor external process
     *
     * @param maxOutputLines Maximum number of lines to capture
     * @param command A string array containing the program and its arguments
     * @return Execution Result
     * @throws IOException when command cannot be executed
     */
    public static ExecutionResult executeBlocking(int maxOutputLines, String...command) throws IOException {
        ProcessBuilder processBuilder = new ProcessBuilder(command);
        try {
            Process process = processBuilder.start();
            StringBuilder sysout = new StringBuilder();
            StringBuilder errout = new StringBuilder();

            StreamPump sysoutThread = new StreamPump(process.getInputStream(), sysout, maxOutputLines);
            sysoutThread.start();
            StreamPump erroutThread = new StreamPump(process.getErrorStream(), errout, maxOutputLines);
            erroutThread.start();

            int exitCode = process.waitFor();
            sysoutThread.requestStop();
            erroutThread.requestStop();
            sysoutThread.join();
            erroutThread.join();

            return new ExecutionResult(exitCode, sysout.toString(), errout.toString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for external process", e);
        }
    }

    public static void printResult(String name, ExecutionResult result) {
        StringBuilder sb = new StringBuilder();
        if (result.getExitCode() == 0) {
            sb.append(name).append(LINE_SEPARATOR).append(result.getSysout());
        } else {
            sb.append("Error while generating ").append(name).append("Exit code").append(result.getExitCode())
                    .append("Standard output: ").append(LINE_SEPARATOR).append(result.getSysout())
                    .append("Error output: ").append(LINE_SEPARATOR).append(result.getExitCode());
        }
        System.out.println(sb);
    }

    private static class StreamPump extends Thread {
        private final InputStream from;
        private final StringBuilder sb;
        private final int maxLine;

        private volatile boolean stopRequested;

        private StreamPump(InputStream from, StringBuilder sb, int maxLines) {
            this.from = from;
            this.sb = sb;
            this.maxLine = maxLines;
        }

        @Override
        public void run() {
            BufferedReader reader = new BufferedReader(new InputStreamReader(from));
            int truncatedLines = 0;
            for (int i = 0; !stopRequested; i++) {
                try {
                    String line = reader.readLine();
                    if (i < maxLine) {
                        if (line != null) {
                            sb.append(line).append(StringUtil.LINE_SEPARATOR);
                        }
                    } else {
                        truncatedLines++;
                    }
                } catch (IOException e) {
                    sneakyThrow(e);
                }
            }
            if (truncatedLines != 0) {
                sb.append("[... truncated ").append(truncatedLines).append(" lines ...]");
            }
        }

        private void requestStop() {
            stopRequested = true;
            this.interrupt();
        }
    }
}
