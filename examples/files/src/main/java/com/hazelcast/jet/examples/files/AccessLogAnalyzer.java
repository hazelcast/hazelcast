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

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;

import java.io.Serializable;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.hazelcast.function.Functions.wholeItem;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;

/**
 * Demonstrates the usage of the file {@link Sources#files sources} and
 * {@link Sinks#files(String) sinks} in a job that reads a Web access
 * log file and counts accesses to particular URL paths. It gives the
 * results for the whole path hierarchy, i.e., a path {@code a/b/c}
 * increases the count for {@code a}, {@code a/b} and {@code a/b/c}.
 * The sample demonstrates how to write a simple {@link Traverser} to
 * implement the flatmapping logic that turns a path into a list of its
 * subpaths: {@link #explodeSubPaths(LogLine) explodeSubPaths()}.
 * <p>
 * This analyzer could be run on a Jet cluster deployed on the same
 * machines as those forming the web server cluster. This way each
 * instance would analyze the local log files and merge the counts for the
 * whole cluster. Since the sample uses a file-writing sink, there will be
 * an output file on each member, containing a slice of the full set of
 * analyzed paths. If you use an {@code IMap} instead, you'd get all the
 * data in one place.
 * <p>
 * The sample log files are in {@code {module.dir}/data/access_log.processed}.
 */
public class AccessLogAnalyzer {

    private static Pipeline buildPipeline(String sourceDir, String targetDir) {
        Pipeline p = Pipeline.create();

        p.readFrom(Sources.files(sourceDir))
         .map(LogLine::parse)
         .filter((LogLine log) -> log.getResponseCode() >= 200 && log.getResponseCode() < 400)
         .flatMap(AccessLogAnalyzer::explodeSubPaths)
         .groupingKey(wholeItem())
         .aggregate(counting())
         .writeTo(Sinks.files(targetDir));

        return p;
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage:");
            System.err.println("  " + AccessLogAnalyzer.class.getSimpleName() + " <sourceDir> <targetDir>");
            System.exit(1);
        }
        final String sourceDir = args[0];
        final String targetDir = args[1];

        Pipeline p = buildPipeline(sourceDir, targetDir);

        JetInstance instance = Jet.bootstrappedInstance();
        try {
            instance.newJob(p).join();
        } finally {
            Jet.shutdownAll();
        }
    }

    /**
     * Explodes a string e.g. {@code "/path/to/file"} to following sequence:
     * <pre>
     *  {
     *     "/path/",
     *     "/path/to/",
     *     "/path/to/file"
     *  }
     * </pre>
     */
    private static Traverser<String> explodeSubPaths(LogLine logLine) {
        // remove the query string after "?"
        int qmarkPos = logLine.getEndpoint().indexOf('?');
        String endpoint = qmarkPos < 0 ? logLine.getEndpoint() : logLine.getEndpoint().substring(0, qmarkPos);

        return new Traverser<String>() {
            private int indexOfSlash;

            @Override
            public String next() {
                if (indexOfSlash < 0) {
                    return null;
                }
                int nextIndexOfSlash = endpoint.indexOf('/', indexOfSlash + 1);
                try {
                    return nextIndexOfSlash < 0 ? endpoint : endpoint.substring(0, nextIndexOfSlash + 1);
                } finally {
                    indexOfSlash = nextIndexOfSlash;
                }
            }
        };
    }

    /**
     * Immutable data transfer object mapping the log line.
     */
    private static class LogLine implements Serializable {
        // Example Apache log line:
        //   127.0.0.1 - - [21/Jul/2014:9:55:27 -0800] "GET /home.html HTTP/1.1" 200 2048
        private static final String LOG_ENTRY_PATTERN =
                // 1:IP  2:client 3:user 4:date time                   5:method 6:req 7:proto   8:respcode 9:size
                "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\d+)";
        private static final Pattern PATTERN = Pattern.compile(LOG_ENTRY_PATTERN);

        private static final DateTimeFormatter DATE_TIME_FORMATTER =
                DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z", Locale.US);

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
