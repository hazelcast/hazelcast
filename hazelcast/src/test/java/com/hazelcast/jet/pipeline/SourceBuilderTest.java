/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.collection.IList;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.ToLongFunctionEx;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.datamodel.WindowResult;
import com.hazelcast.jet.impl.JobRepository;
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
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.pipeline.WindowDefinition.tumbling;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SourceBuilderTest extends PipelineStreamTestSupport {

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
        p.readFrom(fileSource)
                .writeTo(sinkList());

        hz().getJet().newJob(p).join();

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
        p.readFrom(fileSource)
                .writeTo(sinkList());
        hz().getJet().newJob(p).join();

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
            BatchSource<String> socketSource = SourceBuilder
                    .batch("socket-source", ctx -> socketReader(localPort))
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
            p.readFrom(socketSource)
             .writeTo(sinkList());
            hz().getJet().newJob(p).join();
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
            BatchSource<String> socketSource = SourceBuilder
                    .batch("distributed-socket-source", ctx -> socketReader(localPort))
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
            p.readFrom(socketSource)
             .writeTo(sinkList());
            hz().getJet().newJob(p).join();

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
            ToLongFunctionEx<String> timestampFn = line -> Long.valueOf(line.substring(LINE_PREFIX.length()));

            BatchSource<String> socketSource = SourceBuilder
                    .batch("socket-source-with-timestamps", ctx -> socketReader(localPort))
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
            p.readFrom(socketSource)
                    .addTimestamps(timestampFn, 0)
                    .window(tumbling(1))
                    .aggregate(AggregateOperations.counting())
                    .writeTo(sinkList());

            hz().getJet().newJob(p).join();

            List<WindowResult<Long>> expected = LongStream
                    .range(1, itemCount + 1)
                    .mapToObj(i -> new WindowResult<>(i - 1, i, 1L))
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
            FunctionEx<String, Long> timestampFn = line -> Long.valueOf(line.substring(LINE_PREFIX.length()));

            int lateness = 10;
            long lastExpectedTs = itemCount - lateness;
            StreamSource<String> socketSource = SourceBuilder
                    .timestampedStream("socket-source-with-timestamps", ctx -> socketReader(localPort))
                    .<String>fillBufferFn((in, buf) -> {
                        String line = in.readLine();
                        if (line != null) {
                            long ts = timestampFn.apply(line);
                            buf.add(line, ts);
                            if (ts >= lastExpectedTs) {
                                System.out.println(line);
                            }
                        }
                    })
                    .destroyFn(BufferedReader::close)
                    .build();

            // Then
            Pipeline p = Pipeline.create();
            p.readFrom(socketSource)
                    .withNativeTimestamps(lateness)
                    .window(tumbling(1))
                    .aggregate(AggregateOperations.counting())
                    .writeTo(sinkList());

            hz().getJet().newJob(p);

            List<WindowResult<Long>> expected = LongStream.range(1, itemCount - lateness)
                    .mapToObj(i -> new WindowResult<>(i - 1, i, 1L))
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
            ToLongFunctionEx<String> timestampFn = line -> Long.valueOf(line.substring(LINE_PREFIX.length()));

            BatchSource<String> socketSource = SourceBuilder
                    .batch("socket-source-with-timestamps", ctx -> socketReader(localPort))
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
            p.readFrom(socketSource)
                    .addTimestamps(timestampFn, 1000)
                    .window(tumbling(1))
                    .aggregate(AggregateOperations.counting())
                    .writeTo(sinkList());

            hz().getJet().newJob(p).join();

            List<WindowResult<Long>> expected = LongStream.range(1, itemCount + 1)
                    .mapToObj(i -> new WindowResult<>(i - 1, i, (long) PREFERRED_LOCAL_PARALLELISM * MEMBER_COUNT))
                    .collect(toList());

            assertEquals(expected, new ArrayList<>(sinkList));
        }
    }

    @Test
    public void test_faultTolerance() {
        StreamSource<Integer> source = integerSequenceSource(true);
        testFaultTolerance(source);
    }

    @Test
    public void test_faultTolerance_snapshotWithUserDefinedObject() {
        StreamSource<WrappedInt> source = SourceBuilder
                .timestampedStream("src", ctx -> new WrappedNumberGeneratorContext())
                .<WrappedInt>fillBufferFn((src, buffer) -> {
                    for (int i = 0; i < 100; i++) {
                        buffer.add(src.current, src.current.value);
                        src.current = new WrappedInt(src.current.value + 1);
                    }
                    Thread.sleep(100);
                })
                .createSnapshotFn(src -> {
                    System.out.println("Will save " + src.current.value + " to snapshot");
                    return src;
                })
                .restoreSnapshotFn((src, states) -> {
                    assert states.size() == 1;
                    src.restore(states.get(0));
                    System.out.println("Restored " + src.current.value + " from snapshot");
                })
                .build();

        testFaultTolerance(source);
    }

    private void testFaultTolerance(StreamSource<?> source) {
        long windowSize = 100;
        IList<WindowResult<Long>> result = hz().getList("result-" + UuidUtil.newUnsecureUuidString());

        Pipeline p = Pipeline.create();
        p.readFrom(source)
                .withNativeTimestamps(0)
                .window(tumbling(windowSize))
                .aggregate(AggregateOperations.counting())
                .peek()
                .writeTo(Sinks.list(result));

        Job job = hz().getJet().newJob(p, new JobConfig().setProcessingGuarantee(EXACTLY_ONCE));
        assertTrueEventually(() -> assertFalse("result list is still empty", result.isEmpty()));
        // restart the job
        job.restart();
        assertJobStatusEventually(job, JobStatus.RUNNING);

        // wait until more results are added
        int oldSize = result.size();
        assertTrueEventually(() -> assertTrue("no more results added to the list", result.size() > oldSize));
        cancelAndJoin(job);

        // results should contain a monotonic sequence of results, each with count=windowSize
        Iterator<WindowResult<Long>> iterator = result.iterator();
        for (int i = 0; i < result.size(); i++) {
            WindowResult<Long> next = iterator.next();
            assertEquals(windowSize, (long) next.result());
            assertEquals(i * windowSize, next.start());
        }
    }

    @Test
    public void test_faultTolerance_restartTwice() {
        StreamSource<Integer> source = integerSequenceSource(true);

        long windowSize = 100;
        IList<WindowResult<Long>> result = hz().getList("result-" + UuidUtil.newUnsecureUuidString());

        Pipeline p = Pipeline.create();
        p.readFrom(source)
                .withNativeTimestamps(0)
                .window(tumbling(windowSize))
                .aggregate(AggregateOperations.counting())
                .peek()
                .writeTo(Sinks.list(result));

        Job job = hz().getJet().newJob(p, new JobConfig().setProcessingGuarantee(EXACTLY_ONCE));
        assertTrueEventually(() -> assertFalse("result list is still empty", result.isEmpty()));
        // restart the job
        job.restart();
        assertJobStatusEventually(job, JobStatus.RUNNING);

        // wait until more results are added
        int oldSize = result.size();
        assertTrueEventually(() -> assertTrue("no more results added to the list", result.size() > oldSize));

        // restart the job for the second time
        job.restart();
        assertJobStatusEventually(job, JobStatus.RUNNING);

        // wait until more results are added
        int sizeAfterSecondRestart = result.size();
        assertTrueEventually(() -> assertTrue("no more results added to the list",
                result.size() > sizeAfterSecondRestart));

        cancelAndJoin(job);

        // results should contain a monotonic sequence of results, each with count=windowSize
        Iterator<WindowResult<Long>> iterator = result.iterator();
        for (int i = 0; i < result.size(); i++) {
            WindowResult<Long> next = iterator.next();
            assertEquals(windowSize, (long) next.result());
            assertEquals(i * windowSize, next.start());
        }
    }

    @Test
    public void test_nonFaultTolerantSource_processingGuaranteeNone() {
        StreamSource<Integer> source = integerSequenceSource(false);

        long windowSize = 100;
        IList<WindowResult<Long>> result = hz().getList("result-" + UuidUtil.newUnsecureUuidString());

        Pipeline p = Pipeline.create();
        p.readFrom(source)
                .withNativeTimestamps(0)
                .window(tumbling(windowSize))
                .aggregate(AggregateOperations.counting())
                .peek()
                .writeTo(Sinks.list(result));

        Job job = hz().getJet().newJob(p);
        assertTrueEventually(() -> assertFalse("result list is still empty", result.isEmpty()));
        // restart the job
        job.restart();
        assertJobStatusEventually(job, JobStatus.RUNNING);

        // wait until more results are added
        int oldSize = result.size();
        assertTrueEventually(() -> assertTrue("no more results added to the list", result.size() > oldSize));
        cancelAndJoin(job);

        Iterator<WindowResult<Long>> iterator = result.iterator();

        int startAfterRestartIndex = 0;
        for (int i = 0; i < result.size(); i++) {
            WindowResult<Long> next = iterator.next();
            long resultStart = next.start();
            assertEquals(windowSize, (long) next.result());
            if (i != 0 && resultStart == 0) {
                startAfterRestartIndex = i;
                break;
            }
            assertEquals(i * windowSize, resultStart);
        }
        for (int i = 1; i < result.size() - startAfterRestartIndex; i++) {
            WindowResult<Long> next = iterator.next();
            assertEquals(i * windowSize, next.start());
            assertEquals(windowSize, (long) next.result());
        }
    }

    @Test
    public void test_nonFaultTolerantSource_processingGuaranteeOn() {
        StreamSource<Integer> source = SourceBuilder
                .stream("src", procCtx -> "foo")
                .<Integer>fillBufferFn((ctx, buffer) -> {
                    buffer.add(0);
                    Thread.sleep(100);
                })
                .build();

        Pipeline p = Pipeline.create();
        IList<Integer> result = hz().getList("result-" + UuidUtil.newUnsecureUuidString());
        p.readFrom(source)
         .withoutTimestamps()
         .writeTo(Sinks.list(result));

        Job job = hz().getJet().newJob(p, new JobConfig().setProcessingGuarantee(EXACTLY_ONCE).setSnapshotIntervalMillis(100));
        JobRepository jr = new JobRepository(hz());
        waitForFirstSnapshot(jr, job.getId(), 10, true);

        job.restart();
        assertJobStatusEventually(job, JobStatus.RUNNING);
        int currentSize = result.size();
        assertTrueEventually(() -> assertTrue(result.size() > currentSize), 5);
    }

    private StreamSource<Integer> integerSequenceSource(boolean addFaultTolerance) {
        SourceBuilder<NumberGeneratorContext>.TimestampedStream<Integer> builder = SourceBuilder
                .timestampedStream("src", ctx -> new NumberGeneratorContext())
                .fillBufferFn((src, buffer) -> {
                    long expectedCount = NANOSECONDS.toMillis(System.nanoTime() - src.startTime);
                    expectedCount = Math.min(expectedCount, src.current + 100);
                    while (src.current < expectedCount) {
                        buffer.add(src.current, src.current);
                        src.current++;
                    }
                });
        if (addFaultTolerance) {
            builder = builder
                    .createSnapshotFn(src -> {
                        System.out.println("Will save " + src.current + " to snapshot");
                        return src;
                    })
                    .restoreSnapshotFn((src, states) -> {
                        assert states.size() == 1;
                        src.restore(states.get(0));
                        System.out.println("Restored " + src.current + " from snapshot");
                    });
        }
        return builder.build();
    }

    private static final class NumberGeneratorContext implements Serializable {
        long startTime = System.nanoTime();
        int current;

        void restore(NumberGeneratorContext other) {
            this.startTime = other.startTime;
            this.current = other.current;
        }
    }

    private static final class WrappedNumberGeneratorContext implements Serializable {

        long startTime = System.nanoTime();
        WrappedInt current = new WrappedInt(0);

        void restore(WrappedNumberGeneratorContext other) {
            this.startTime = other.startTime;
            this.current = other.current;
        }
    }

    private static final class WrappedInt implements Serializable {

        final int value;

        WrappedInt(int value) {
            this.value = value;
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
