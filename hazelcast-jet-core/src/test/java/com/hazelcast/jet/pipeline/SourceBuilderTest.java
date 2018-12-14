/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.pipeline;

import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.datamodel.TimestampedItem;
import com.hazelcast.jet.function.DistributedFunction;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static com.hazelcast.jet.pipeline.WindowDefinition.tumbling;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;

public class SourceBuilderTest extends PipelineTestSupport {

    private static final String LINE_PREFIX = "line";
    private static final int PREFERRED_LOCAL_PARALLELISM = 2;

    @Test
    public void batch_fileSource() throws Exception {
        // Given
        File textFile = createTestFile();

        // When
        BatchSource<String> fileSource = SourceBuilder
                .batch("file-source", x -> fileReader(textFile))
                .<String>fillBufferFn((in, buf) -> {
                    String line = in.readLine();
                    if (line != null) {
                        buf.add(line);
                    } else {
                        buf.close();
                    }
                })
                .destroyFn(BufferedReader::close)
                .build();

        // Then
        Pipeline p = Pipeline.create();
        p.drawFrom(fileSource)
                .drainTo(sinkList());

        jet().newJob(p).join();

        assertEquals(
                IntStream.range(0, itemCount).mapToObj(i -> "line" + i).collect(toList()),
                new ArrayList<>(sinkList)
        );
    }

    @Test
    public void batch_fileSource_distributed() throws Exception {
        // Given
        File textFile = createTestFile();

        // When
        BatchSource<String> fileSource = SourceBuilder
                .batch("distributed-file-source", ctx -> fileReader(textFile))
                .<String>fillBufferFn((in, buf) -> {
                    String line = in.readLine();
                    if (line != null) {
                        buf.add(line);
                    } else {
                        buf.close();
                    }
                })
                .destroyFn(BufferedReader::close)
                .distributed(PREFERRED_LOCAL_PARALLELISM)
                .build();

        // Then
        Pipeline p = Pipeline.create();
        p.drawFrom(fileSource)
                .drainTo(sinkList());
        jet().newJob(p).join();

        Map<String, Integer> actual = sinkToBag();
        Map<String, Integer> expected = IntStream.range(0, itemCount)
                .boxed()
                .collect(Collectors.toMap(i -> "line" + i, i -> PREFERRED_LOCAL_PARALLELISM * MEMBER_COUNT));

        assertEquals(expected, actual);
    }

    @Test
    public void stream_socketSource() throws IOException {
        // Given
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            startServer(serverSocket);

            // When
            int localPort = serverSocket.getLocalPort();
            StreamSource<String> socketSource = SourceBuilder
                    .stream("socket-source", ctx -> socketReader(localPort))
                    .<String>fillBufferFn((in, buf) -> {
                        String line = in.readLine();
                        if (line != null) {
                            buf.add(line);
                        } else {
                            buf.close();
                        }
                    })
                    .destroyFn(BufferedReader::close)
                    .build();

            // Then
            Pipeline p = Pipeline.create();
            p.drawFrom(socketSource)
             .withoutTimestamps()
             .drainTo(sinkList());
            jet().newJob(p).join();
            List<String> expected = IntStream.range(0, itemCount).mapToObj(i -> "line" + i).collect(toList());
            assertEquals(expected, new ArrayList<>(sinkList));
        }
    }

    @Test
    public void stream_socketSource_distributed() throws IOException {
        // Given
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            startServer(serverSocket);

            // When
            int localPort = serverSocket.getLocalPort();
            StreamSource<String> socketSource = SourceBuilder
                    .stream("distributed-socket-source", ctx -> socketReader(localPort))
                    .<String>fillBufferFn((in, buf) -> {
                        String line = in.readLine();
                        if (line != null) {
                            buf.add(line);
                        } else {
                            buf.close();
                        }
                    })
                    .destroyFn(BufferedReader::close)
                    .distributed(PREFERRED_LOCAL_PARALLELISM)
                    .build();

            // Then
            Pipeline p = Pipeline.create();
            p.drawFrom(socketSource)
             .withoutTimestamps()
             .drainTo(sinkList());
            jet().newJob(p).join();

            Map<String, Integer> expected = IntStream.range(0, itemCount)
                    .boxed()
                    .collect(Collectors.toMap(i -> "line" + i, i -> PREFERRED_LOCAL_PARALLELISM * MEMBER_COUNT));

            assertEquals(expected, sinkToBag());
        }
    }

    @Test
    public void stream_socketSource_withTimestamps() throws IOException {
        // Given
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            startServer(serverSocket);

            // When
            int localPort = serverSocket.getLocalPort();
            DistributedFunction<String, Long> timestampFn = line -> Long.valueOf(line.substring(LINE_PREFIX.length()));

            StreamSource<String> socketSource = SourceBuilder
                    .timestampedStream("socket-source-with-timestamps", ctx -> socketReader(localPort))
                    .<String>fillBufferFn((in, buf) -> {
                        String line = in.readLine();
                        if (line != null) {
                            buf.add(line, timestampFn.apply(line));
                        } else {
                            buf.close();
                        }
                    })
                    .destroyFn(BufferedReader::close)
                    .build();

            // Then
            Pipeline p = Pipeline.create();
            p.drawFrom(socketSource)
                    .withNativeTimestamps(0)
                    .window(tumbling(1))
                    .aggregate(AggregateOperations.counting())
                    .drainTo(sinkList());

            jet().newJob(p).join();

            List<TimestampedItem<Long>> expected = LongStream.range(1, itemCount + 1)
                    .mapToObj(i -> new TimestampedItem<>(i, 1L))
                    .collect(toList());

            assertEquals(expected, new ArrayList<>(sinkList));
        }
    }

    @Test
    public void stream_socketSource_withTimestamps_andLateness() throws IOException {
        // Given
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            startServer(serverSocket);

            // When
            int localPort = serverSocket.getLocalPort();
            DistributedFunction<String, Long> timestampFn = line -> Long.valueOf(line.substring(LINE_PREFIX.length()));

            int lateness = 10;
            StreamSource<String> socketSource = SourceBuilder
                    .timestampedStream("socket-source-with-timestamps", ctx -> socketReader(localPort))
                    .<String>fillBufferFn((in, buf) -> {
                        String line = in.readLine();
                        if (line != null) {
                            buf.add(line, timestampFn.apply(line));
                        }
                    })
                    .destroyFn(BufferedReader::close)
                    .build();

            // Then
            Pipeline p = Pipeline.create();
            p.drawFrom(socketSource)
                    .withNativeTimestamps(lateness)
                    .window(tumbling(1))
                    .aggregate(AggregateOperations.counting())
                    .drainTo(sinkList());

            jet().newJob(p);

            List<TimestampedItem<Long>> expected = LongStream.range(1, itemCount - lateness)
                    .mapToObj(i -> new TimestampedItem<>(i, 1L))
                    .collect(toList());

            assertTrueEventually(() -> assertEquals(expected, new ArrayList<>(sinkList)), 10);
        }
    }

    @Test
    public void stream_distributed_socketSource_withTimestamps() throws IOException {
        // Given
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            startServer(serverSocket);

            // When
            int localPort = serverSocket.getLocalPort();
            DistributedFunction<String, Long> timestampFn = line -> Long.valueOf(line.substring(LINE_PREFIX.length()));

            StreamSource<String> socketSource = SourceBuilder
                    .timestampedStream("socket-source-with-timestamps", ctx -> socketReader(localPort))
                    .<String>fillBufferFn((in, buf) -> {
                        String line = in.readLine();
                        if (line != null) {
                            buf.add(line, timestampFn.apply(line));
                        } else {
                            buf.close();
                        }
                    })
                    .destroyFn(BufferedReader::close)
                    .distributed(PREFERRED_LOCAL_PARALLELISM)
                    .build();

            // Then
            Pipeline p = Pipeline.create();
            p.drawFrom(socketSource)
                    .withNativeTimestamps(0)
                    .window(tumbling(1))
                    .aggregate(AggregateOperations.counting())
                    .drainTo(sinkList());

            jet().newJob(p).join();

            List<TimestampedItem<Long>> expected = LongStream.range(1, itemCount + 1)
                    .mapToObj(i -> new TimestampedItem<>(i, (long) PREFERRED_LOCAL_PARALLELISM * MEMBER_COUNT))
                    .collect(toList());

            assertEquals(expected, new ArrayList<>(sinkList));
        }
    }


    private void startServer(ServerSocket serverSocket) {
        spawnSafe(() -> {
            while (!serverSocket.isClosed()) {
                Socket socket = serverSocket.accept();
                System.out.println("Accepted connection from " + socket.getPort());
                spawnSafe(() -> {
                    try (PrintWriter out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()))) {
                        for (int i = 0; i < itemCount; i++) {
                            out.println(LINE_PREFIX + i);
                        }
                    } finally {
                        socket.close();
                    }
                });
            }
        });
    }

    private File createTestFile() throws IOException {
        File dir = createTempDirectory();
        File textFile = new File(dir, "stuff.txt");
        try (PrintWriter out = new PrintWriter(new FileWriter(textFile))) {
            for (int i = 0; i < itemCount; i++) {
                out.println("line" + i);
            }
        }
        return textFile;
    }

    private static BufferedReader socketReader(int port) throws IOException {
        return new BufferedReader(new InputStreamReader(new Socket("localhost", port).getInputStream()));
    }

    private static BufferedReader fileReader(File textFile) throws FileNotFoundException {
        return new BufferedReader(new FileReader(textFile));
    }

}
