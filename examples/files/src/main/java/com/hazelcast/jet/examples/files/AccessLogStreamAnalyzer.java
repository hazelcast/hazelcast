/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.examples.files;

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.WindowDefinition;

import java.io.BufferedWriter;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.locks.LockSupport;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Analyzes access log files from a HTTP server. Demonstrates the usage of
 * {@link Sources#fileWatcher(String)} (String, Charset, String)} to read
 * files line by line in streaming fashion - by running indefinitely and
 * watching for changes as they appear.
 * <p>
 * It uses sliding window aggregation to output frequency of visits to each
 * page continuously.
 * <p>
 * This analyzer could be run on a Jet cluster deployed on the same
 * machines as those forming the web server cluster. This way each instance
 * will process local files locally and subsequently merge the results
 * globally.
 * <p>
 * This sample does not work well on Windows. On Windows, even though new
 * lines are flushed, WatchService is not notified of changes, until the
 * file is closed.
 */
public class AccessLogStreamAnalyzer {

    private static Pipeline buildPipeline(Path tempDir) {
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.fileWatcher(tempDir.toString()))
         .withoutTimestamps()
         .map(LogLine::parse)
         .filter(line -> line.getResponseCode() >= 200 && line.getResponseCode() < 400)
         .addTimestamps(LogLine::getTimestamp, 1000)
         .window(WindowDefinition.sliding(10_000, 1_000))
         .groupingKey(LogLine::getEndpoint)
         .aggregate(AggregateOperations.counting())
         .writeTo(Sinks.logger());
        return p;
    }

    public static void main(String[] args) throws Exception {
        Path tempDir = Files.createTempDirectory(AccessLogStreamAnalyzer.class.getSimpleName());
        Pipeline p = buildPipeline(tempDir);

        JetInstance instance = Jet.bootstrappedInstance();
        try {
            instance.newJob(p);
            // job is running in its own threads. Let's generate some random traffic in this thread.
            startGenerator(tempDir);
            // wait for all writes to be picked up
            LockSupport.parkNanos(SECONDS.toNanos(1));
        } finally {
            Jet.shutdownAll();
            IOUtil.delete(tempDir.toFile());
        }
    }

    private static void startGenerator(Path tempDir) throws Exception {
        Random random = new Random();
        try (BufferedWriter w = Files.newBufferedWriter(tempDir.resolve("access_log"), StandardOpenOption.CREATE)) {
            for (int i = 0; i < 60_000; i++) {
                int articleNum = Math.min(10, Math.max(0, (int) (random.nextGaussian() * 2 + 5)));
                w.append("129.21.37.3 - - [")
                 .append(LogLine.DATE_TIME_FORMATTER.format(ZonedDateTime.now()))
                 .append("] \"GET /article")
                 .append(String.valueOf(articleNum))
                 .append(" HTTP/1.0\" 200 12345");

                w.newLine();
                w.flush();
                LockSupport.parkNanos(MILLISECONDS.toNanos(1));
            }
        }
    }

    /**
     * Immutable data transfer object mapping the log line.
     */
    private static class LogLine implements Serializable {

        public static final DateTimeFormatter DATE_TIME_FORMATTER =
                DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z", Locale.US);

        // Example Apache log line:
        //   127.0.0.1 - - [21/Jul/2014:9:55:27 -0800] "GET /home.html HTTP/1.1" 200 2048
        private static final String LOG_ENTRY_PATTERN =
                // 1:IP  2:client 3:user 4:date time                   5:method 6:req 7:proto   8:respcode 9:size
                "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\d+)";
        private static final Pattern PATTERN = Pattern.compile(LOG_ENTRY_PATTERN);

        private final String ipAddress;
        private final String clientIdentd;
        private final String userID;
        private final long timestamp;
        private final String method;
        private final String endpoint;
        private final String protocol;
        private final int responseCode;
        private final long contentSize;

        LogLine(String ipAddress, String clientIdentd, String userID, long timestamp, String method, String endpoint,
                String protocol, int responseCode, long contentSize) {
            this.ipAddress = ipAddress;
            this.clientIdentd = clientIdentd;
            this.userID = userID;
            this.timestamp = timestamp;
            this.method = method;
            this.endpoint = endpoint;
            this.protocol = protocol;
            this.responseCode = responseCode;
            this.contentSize = contentSize;
        }

        public static LogLine parse(String line) {
            Matcher m = PATTERN.matcher(line);
            if (!m.find()) {
                throw new IllegalArgumentException("Cannot parse log line: " + line);
            }
            long time = ZonedDateTime.parse(m.group(4), DATE_TIME_FORMATTER).toInstant().toEpochMilli();
            return new LogLine(m.group(1), m.group(2), m.group(3), time, m.group(5), m.group(6), m.group(7),
                    Integer.parseInt(m.group(8)), Long.parseLong(m.group(9)));
        }

        public String getIpAddress() {
            return ipAddress;
        }

        public String getClientIdentd() {
            return clientIdentd;
        }

        public String getUserID() {
            return userID;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public String getMethod() {
            return method;
        }

        public String getEndpoint() {
            return endpoint;
        }

        public String getProtocol() {
            return protocol;
        }

        public int getResponseCode() {
            return responseCode;
        }

        public long getContentSize() {
            return contentSize;
        }
    }
}
