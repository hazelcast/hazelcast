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

package com.hazelcast.jet.impl.connector;

import com.fasterxml.jackson.jr.ob.JSON;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.jet.impl.JobProxy;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.impl.connector.ReadFilesPTest.TestPerson;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.annotation.Nonnull;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.processor.Processors.mapP;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeFileP;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static com.hazelcast.jet.pipeline.FileSinkBuilder.DISABLE_ROLLING;
import static com.hazelcast.jet.pipeline.FileSinkBuilder.TEMP_FILE_SUFFIX;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.joining;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category({QuickTest.class, ParallelJVMTest.class})
public class WriteFilePTest extends SimpleTestInClusterSupport {

    private static final Semaphore semaphore = new Semaphore(0);
    private static final AtomicLong clock = new AtomicLong();

    private Path directory;
    private Path onlyFile;

    @BeforeClass
    public static void beforeClass() {
        initialize(1, null);
        semaphore.drainPermits();
    }

    @Before
    public void setup() throws Exception {
        directory = Files.createTempDirectory("write-file-p");
        onlyFile = directory.resolve("0");
    }

    @After
    public void after() {
        IOUtil.delete(directory.toFile());
    }

    @Test
    public void when_localParallelismMoreThan1_then_multipleFiles() throws Exception {
        // Given
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(0, 1, 2))
         .writeTo(Sinks.files(directory.toString()))
         .setLocalParallelism(2);

        // When
        instance().getJet().newJob(p).join();

        // Then
        try (Stream<Path> stream = Files.list(directory)) {
            assertEquals(2, stream.count());
        }
    }

    @Test
    public void smokeTest_smallFile() throws Exception {
        // Given
        Pipeline p = buildPipeline(null, rangeIterable(0, 10));

        // When
        instance().getJet().newJob(p).join();

        // Then
        checkFileContents(0, 10, false, false, true);
    }

    @Test
    public void smokeTest_bigFile() throws Exception {
        // Given
        Pipeline p = buildPipeline(null, rangeIterable(0, 100_000));

        // When
        instance().getJet().newJob(p).join();

        // Then
        checkFileContents(0, 100_000, false, false, true);
    }

    @Test
    public void when_append_then_previousContentsOfFileIsKept() throws Exception {
        // Given
        Pipeline p = buildPipeline(null, rangeIterable(1, 10));
        try (BufferedWriter writer = Files.newBufferedWriter(onlyFile)) {
            writer.write("0");
            writer.newLine();
        }

        // When
        instance().getJet().newJob(p).join();

        // Then
        checkFileContents(0, 10, false, false, true);
    }

    @Test
    public void when_slowSource_then_fileFlushedAfterEachItem() {
        // Given
        int numItems = 10;

        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", () -> new SlowSourceP(semaphore, numItems))
                           .localParallelism(1);
        Vertex sink = dag.newVertex("sink",
                writeFileP(directory.toString(), StandardCharsets.UTF_8, null, DISABLE_ROLLING, true, Object::toString))
                         .localParallelism(1);
        dag.edge(between(source, sink));

        Job job = instance().getJet().newJob(dag);
        for (int i = 0; i < numItems; i++) {
            // When
            semaphore.release();
            int finalI = i;
            // Then
            assertTrueEventually(() -> checkFileContents(0, finalI + 1, false, false, true), 5);
        }

        // wait for the job to finish
        job.join();
    }

    @Test
    public void testCharset() throws Exception {
        // Given
        Charset charset = Charset.forName("iso-8859-2");
        String text = "ľščťž";
        Pipeline p = buildPipeline(charset, singletonList(text));

        // When
        instance().getJet().newJob(p).join();

        // Then
        assertEquals(text + System.getProperty("line.separator"), new String(Files.readAllBytes(onlyFile), charset));
    }

    @Test
    public void test_createDirectories() {
        // Given
        Path myFile = directory.resolve("subdir1/subdir2/" + onlyFile.getFileName());

        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(rangeIterable(0, 10)))
         .writeTo(Sinks.files(myFile.toString()));

        // When
        instance().getJet().newJob(p).join();

        // Then
        assertTrue(Files.exists(directory.resolve("subdir1")));
        assertTrue(Files.exists(directory.resolve("subdir1/subdir2")));
    }

    @Test
    public void when_toStringF_then_used() throws Exception {
        // Given
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(rangeIterable(1, 11)))
         .writeTo(Sinks.<Integer>filesBuilder(directory.toString())
                 .toStringFn(val -> Integer.toString(val - 1))
                 .build());

        // When
        instance().getJet().newJob(p).join();

        // Then
        checkFileContents(0, 10, false, false, true);
    }

    @Test
    public void test_rollByDate() {
        int numItems = 10;
        DAG dag = new DAG();
        Vertex src = dag.newVertex("src", () -> new SlowSourceP(semaphore, numItems)).localParallelism(1);
        @SuppressWarnings("Convert2MethodRef")
        Vertex sink = dag.newVertex("sink", WriteFileP.metaSupplier(
                directory.toString(), Objects::toString, "utf-8", "SSS", DISABLE_ROLLING, true,
                (LongSupplier & Serializable) () -> clock.get()));
        dag.edge(between(src, sink));

        Job job = instance().getJet().newJob(dag);

        for (int i = 0; i < numItems; i++) {
            // When
            semaphore.release();
            String stringValue = i + System.lineSeparator();
            // Then
            Path file = directory.resolve(String.format("%03d-0", i));
            assertTrueEventually(() -> assertTrue("file not found: " + file, Files.exists(file)), 5);
            assertTrueEventually(() ->
                    assertEquals(stringValue, new String(Files.readAllBytes(file), StandardCharsets.UTF_8)), 5);
            clock.incrementAndGet();
        }

        job.join();
    }

    @Test
    public void test_rollByDateHour() throws Exception {
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(rangeIterable(1, 10)))
                .writeTo(Sinks.filesBuilder(directory.toString())
                        .rollByDate("yyyy-MM-dd.HH")
                        .build());
        instance().getJet().newJob(p).join();

        String expectedNamePattern = "\\d{4}-\\d{2}-\\d{2}\\.\\d{2}-0$";
        long numberOfFilesWithExpectedPattern =
                Stream.of(new File(directory.toString()).listFiles())
                        .filter(f -> f.getName().matches(expectedNamePattern))
                        .count();

        assertEquals(1, numberOfFilesWithExpectedPattern);
        checkFileContents(1, 10, false, false, true);
    }

    @Test
    public void test_rollByFileSize() throws Exception {
        int numItems = 10;
        DAG dag = new DAG();
        Vertex src = dag.newVertex("src", () -> new SlowSourceP(semaphore, numItems)).localParallelism(1);
        Vertex map = dag.newVertex("map", mapP((Integer i) -> i + 100));
        // maxFileSize is always large enough for 1 item but never for 2, both with windows and linux newlines
        long maxFileSize = 6L;
        Vertex sink = dag.newVertex("sink", WriteFileP.metaSupplier(
                directory.toString(), Objects::toString, "utf-8", null, maxFileSize, true));
        dag.edge(between(src, map));
        dag.edge(between(map, sink));

        Job job = instance().getJet().newJob(dag);

        // Then
        for (int i = 0; i < numItems; i++) {
            semaphore.release();
            int finalI = i;
            assertTrueEventually(() -> checkFileContents(100, finalI + 101, false, false, true));
        }
        for (int i = 0, j = 100; i < numItems / 2; i++) {
            Path file = directory.resolve("0-" + i);
            assertEquals((j++) + System.lineSeparator() + (j++) + System.lineSeparator(),
                    new String(Files.readAllBytes(file)));
        }

        job.join();
    }

    @Test
    public void test_JsonFile() throws IOException {
        // Given
        Pipeline p = Pipeline.create();
        TestPerson testPerson = new TestPerson("foo", 5, true);
        p.readFrom(TestSources.items(testPerson))
         .writeTo(Sinks.json(directory.toString()));

        // When
        instance().getJet().newJob(p).join();

        // Then
        List<String> lines = Files
                .list(directory)
                .flatMap(file -> uncheckCall(() -> Files.readAllLines(file).stream()))
                .collect(Collectors.toList());
        assertEquals(1, lines.size());
        TestPerson actual = JSON.std.beanFrom(TestPerson.class, lines.get(0));
        assertEquals(testPerson, actual);
    }

    @Test
    public void test_abortUnfinishedTransaction_whenNoItemsProcessed() throws Exception {
        // test for https://github.com/hazelcast/hazelcast/issues/19774
        ProcessorMetaSupplier metaSupplier = writeFileP(directory.toString(), StandardCharsets.UTF_8, null, DISABLE_ROLLING, true,
                Objects::toString);
        TestProcessorContext processorContext = new TestProcessorContext()
                .setProcessingGuarantee(EXACTLY_ONCE);

        @SuppressWarnings("unchecked")
        WriteFileP<Integer> processor = (WriteFileP<Integer>) TestSupport.supplierFrom(metaSupplier).get();
        processor.init(new TestOutbox(new int[]{128}, 128), processorContext);

        processor.process(0, new TestInbox(singletonList(42)));
        assertTrue(processor.snapshotCommitPrepare());
        checkFileContents(0, 0, true, true, true);

        // Now a tmp file is created. Let's simulate that the prepared snapshot wasn't successful and
        // the job restarted
        @SuppressWarnings("unchecked")
        WriteFileP<Integer> processor2 = (WriteFileP<Integer>) TestSupport.supplierFrom(metaSupplier).get();
        processor2.init(new TestOutbox(128), processorContext);
        processor2.close();
        // now there should be no temp files
        checkFileContents(0, 0, true, false, true);
    }

    @Test
    public void stressTest_noSnapshot() throws Exception {
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(rangeIterable(0, 10)))
         .writeTo(Sinks.files(directory.toString()));

        // When
        JobConfig config = new JobConfig()
                .setProcessingGuarantee(EXACTLY_ONCE)
                .setSnapshotIntervalMillis(HOURS.toMillis(1));
        instance().getJet().newJob(p, config).join();

        // Then
        checkFileContents(0, 10, false, false, true);
    }

    @Test
    public void stressTest_snapshots_noRestarts() throws Exception {
        DAG dag = new DAG();
        int numItems = 5;
        Vertex source = dag.newVertex("source", () -> new SlowSourceP(semaphore, numItems))
                           .localParallelism(1);
        Vertex sink = dag.newVertex("sink",
                writeFileP(directory.toString(), StandardCharsets.UTF_8, null, DISABLE_ROLLING, true, Object::toString))
                         .localParallelism(1);
        dag.edge(between(source, sink));

        JobConfig config = new JobConfig()
                .setProcessingGuarantee(EXACTLY_ONCE)
                .setSnapshotIntervalMillis(500);
        Job job = instance().getJet().newJob(dag, config);
        assertJobStatusEventually(job, RUNNING);

        JobRepository jr = new JobRepository(instance());
        waitForFirstSnapshot(jr, job.getId(), 10, true);
        for (int i = 0; i < numItems; i++) {
            waitForNextSnapshot(jr, job.getId(), 10, true);
            semaphore.release();
        }
        job.join();

        checkFileContents(0, numItems, false, false, true);
    }

    @Test
    public void stressTest_exactlyOnce_graceful() throws Exception {
        stressTest(true, true);
    }

    @Test
    public void stressTest_exactlyOnce_forceful() throws Exception {
        stressTest(false, true);
    }

    @Test
    public void stressTest_atLeastOnce_forceful() throws Exception {
        stressTest(false, false);
    }

    private void stressTest(boolean graceful, boolean exactlyOnce) throws Exception {
        int numItems = 500;
        Pipeline p = Pipeline.create();
        p.readFrom(SourceBuilder.stream("src", procCtx -> tuple2(new int[1], procCtx.logger()))
                                .fillBufferFn((ctx, buf) -> {
                                    if (ctx.f0()[0] < numItems) {
                                        buf.add(ctx.f0()[0]++);
                                        sleepMillis(5);
                                    }
                                })
                                .createSnapshotFn(ctx -> {
                                    ctx.f1().fine("src vertex saved to snapshot: " + ctx.f0()[0]);
                                    return ctx.f0()[0];
                                })
                                .restoreSnapshotFn((ctx, state) -> {
                                    ctx.f0()[0] = state.get(0);
                                    ctx.f1().fine("src vertex restored from snapshot: " + ctx.f0()[0]);
                                })
                                .build())
         .withoutTimestamps()
         .writeTo(Sinks.filesBuilder(directory.toString())
                       .exactlyOnce(exactlyOnce)
                       .build())
         .setLocalParallelism(2);

        JobConfig config = new JobConfig()
                .setProcessingGuarantee(EXACTLY_ONCE)
                .setSnapshotIntervalMillis(50);
        JobProxy job = (JobProxy) instance().getJet().newJob(p, config);

        long endTime = System.nanoTime() + SECONDS.toNanos(60);
        do {
            assertJobStatusEventually(job, RUNNING);
            sleepMillis(100);
            job.restart(graceful);
            try {
                checkFileContents(0, numItems, exactlyOnce, true, false);
                // if content matches, break the loop. Otherwise restart and try again
                break;
            } catch (AssertionError ignored) {
            }
        } while (System.nanoTime() < endTime);

        waitForNextSnapshot(new JobRepository(instance()), job.getId(), 10, true);
        ditchJob(job, instances());
        // when the job is cancelled, there should be no temporary files
        checkFileContents(0, numItems, exactlyOnce, false, false);
    }

    private void checkFileContents(int numFrom, int numTo, boolean exactlyOnce, boolean ignoreTempFiles,
                                   boolean assertSorted
    ) throws Exception {
        List<Integer> actual = Files.list(directory)
                                    .peek(f -> {
                                        if (!ignoreTempFiles && f.getFileName().toString().endsWith(TEMP_FILE_SUFFIX)) {
                                            throw new IllegalArgumentException("Temp file found: " + f);
                                        }
                                    })
                                    .filter(f -> !f.toString().endsWith(TEMP_FILE_SUFFIX))
                                    // sort by sequence number, if there is one
                                    .sorted(Comparator.comparing(f -> f.getFileName().toString().length() == 1 ? 0
                                            : Integer.parseInt(f.getFileName().toString().substring(2))))
                                    .flatMap(file -> uncheckCall(() -> Files.readAllLines(file).stream()))
                                    .map(Integer::parseInt)
                                    .sorted(assertSorted ? (l, r) -> 0 : Comparator.naturalOrder())
                                    .collect(Collectors.toList());

        if (exactlyOnce) {
            String expectedStr = IntStream.range(numFrom, numTo).mapToObj(Integer::toString).collect(joining("\n"));
            String actualStr = actual.stream().map(Object::toString).collect(joining("\n"));
            assertEquals(expectedStr, actualStr);
        } else {
            Map<Integer, Long> actualMap = actual.stream().collect(groupingBy(Function.identity(), Collectors.counting()));
            for (int i = numFrom; i < numTo; i++) {
                actualMap.putIfAbsent(i, 0L);
            }
            assertTrue("some items are missing: " + actualMap, actualMap.values().stream().allMatch(v -> v > 0));
        }
    }

    private <T> Pipeline buildPipeline(Charset charset, Iterable<T> iterable) {
        if (charset == null) {
            charset = StandardCharsets.UTF_8;
        }
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(iterable))
         .writeTo(Sinks.filesBuilder(directory.toString())
                       .toStringFn(Objects::toString)
                       .charset(charset)
                       .build());
        return p;
    }

    private static Iterable<Integer> rangeIterable(int itemsFrom, int itemsTo) {
        return (Iterable<Integer> & Serializable) () -> new Iterator<Integer>() {
            int val = itemsFrom;

            @Override
            public boolean hasNext() {
                return val < itemsTo;
            }

            @Override
            public Integer next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return val++;
            }
        };
    }

    private static class SlowSourceP extends AbstractProcessor {

        private final Semaphore semaphore;
        private final int limit;
        private int number;

        SlowSourceP(Semaphore semaphore, int limit) {
            this.semaphore = semaphore;
            this.limit = limit;
        }

        @Override
        public boolean isCooperative() {
            return false;
        }

        @Override
        public boolean complete() {
            while (number < limit) {
                if (!semaphore.tryAcquire()) {
                    return false;
                }
                assertTrue(tryEmit(number));
                number++;
            }
            return true;
        }

        @Override
        public boolean saveToSnapshot() {
            return tryEmitToSnapshot("key", number);
        }

        @Override
        protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
            assertEquals("key", key);
            number = (int) value;
        }
    }
}
