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

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.Processor.Context;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.processor.SourceProcessors.streamFilesP;
import static java.lang.Thread.interrupted;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class StreamFilesPTest extends JetTestSupport {

    private static final long LINE_COUNT = 1_000;
    private static final int ASSERT_COUNT_TIMEOUT_SECONDS = 10;

    @Rule public TestName testName = new TestName();

    private File workDir;
    private StreamFilesP processor;

    private Thread driverThread;
    private TestOutbox outbox;
    private final List<Entry<String, String>> outboxLines = new CopyOnWriteArrayList<>();

    private volatile int fileOffsetsSize;
    private volatile boolean completedNormally;
    private volatile Throwable driverException;

    @Before
    public void before() throws Exception {
        // On Windows, "file changed" events are not delivered unless enough data is written
        // or the file is closed. Some tests in this class rely that after a flush, the line is picked
        // up by the processor.
        assumeThatNoWindowsOS();

        workDir = Files.createTempDirectory("jet-test-streamFilesPTest").toFile();
        driverThread = new Thread(this::driveProcessor, "driver@" + testName.getMethodName());
    }

    @After
    public void after() throws Throwable {
        if (driverException != null) {
            throw driverException;
        }
        if (driverThread != null) {
            driverThread.interrupt();
            driverThread.join();
        }
        if (workDir != null) {
            IOUtil.delete(workDir);
        }
    }

    @Test
    public void when_metaSupplier_then_returnsCorrectProcessors() throws Exception {
        ProcessorMetaSupplier metaSupplier = streamFilesP(workDir.getAbsolutePath(), UTF_8, "*", false, Util::entry);
        Address a = new Address();
        ProcessorSupplier supplier = metaSupplier.get(singletonList(a)).apply(a);
        supplier.init(new TestProcessorContext());
        assertEquals(1, supplier.get(1).size());
        supplier.close(null);
    }

    @Test
    public void when_writeOneFile_then_seeAllLines() throws Exception {
        initializeProcessor(null);
        driverThread.start();
        try (PrintWriter w = new PrintWriter(new FileWriter(new File(workDir, "a.txt")))) {
            for (int i = 0; i < LINE_COUNT; i++) {
                w.println(i);
            }
            w.flush();
            assertEmittedCountEventually(LINE_COUNT);
        }
    }

    @Test
    public void when_writeTwoFiles_then_seeAllLines() throws Exception {
        // Given
        initializeProcessor(null);
        driverThread.start();
        try (PrintWriter w1 = new PrintWriter(new FileWriter(new File(workDir, "a.txt")));
             PrintWriter w2 = new PrintWriter(new FileWriter(new File(workDir, "b.txt")))
        ) {
            // When
            for (int i = 0; i < LINE_COUNT; i++) {
                w1.println(i);
                w1.flush();
                w2.println(i);
                w2.flush();
            }

            // Then
            assertEmittedCountEventually(2 * LINE_COUNT);
        }
    }

    @Test
    public void when_glob_then_onlyMatchingProcessed() throws Exception {
        // Given
        initializeProcessor("a.*");
        driverThread.start();
        try (PrintWriter w1 = new PrintWriter(new FileWriter(new File(workDir, "a.txt")));
                PrintWriter w2 = new PrintWriter(new FileWriter(new File(workDir, "b.txt")))
        ) {
            // When
            for (int i = 0; i < LINE_COUNT; i++) {
                w1.println(i);
                w1.flush();
                w2.println(i);
                w2.flush();
            }

            // Then
            assertEmittedCountEventually(LINE_COUNT);
        }
    }

    @Test
    public void when_preExistingFile_then_seeAppendedLines() throws Exception {
        // Test will fail because Windows does not notify the watcher
        // if the file is appended to, but not closed.
        assumeThatNoWindowsOS();

        // Given
        try (PrintWriter w = new PrintWriter(new FileWriter(new File(workDir, "a.txt")))) {
            for (int i = 0; i < LINE_COUNT; i++) {
                w.println(i);
            }
            w.write("incomplete line");
            w.flush();
            initializeProcessor(null);
            // Directory watch service is apparently initialized asynchronously so we
            // have to give it some time. This is a hacky, non-repeatable test.
            Thread.sleep(1000);
            driverThread.start();

            // When
            w.write(" still incomplete line ");
            w.flush();
            Thread.sleep(2000);
            for (int i = 0; i < LINE_COUNT; i++) {
                w.println(i);
            }
            w.flush();

            // Then
            assertEmittedCountEventually(LINE_COUNT - 1);
        }
    }

    @Test
    public void when_fileModifiedButNotAppended_then_seeNoLines() throws Exception {
        File file = new File(workDir, "a.txt");
        try (PrintWriter w = new PrintWriter(new FileWriter(file))) {
            for (int i = 0; i < LINE_COUNT; i++) {
                w.println(i);
            }
        }
        initializeProcessor(null);
        // Directory watch service is apparently initialized asynchronously so we
        // have to give it some time. This is a hacky, non-repeatable test.
        Thread.sleep(1000);
        driverThread.start();
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        raf.write(0xFF);
        raf.close();
        Thread.sleep(5000);
        assertEquals(0, outboxLines.size());
    }

    @Test
    public void when_fileDeleted_then_seeDeletion() throws Exception {
        // Given
        File file = new File(workDir, "a.txt");
        try (PrintWriter w = new PrintWriter(new FileWriter(file))) {
            for (int i = 0; i < LINE_COUNT; i++) {
                w.println(i);
            }
        }
        initializeProcessor(null);
        updateFileOffsetsSize();
        assertEquals(1, fileOffsetsSize);
        driverThread.start();

        // When
        assertTrue(file.delete());

        // Then
        assertTrueEventually(() -> assertEquals(0, fileOffsetsSize));
    }

    @Test
    public void when_watchedDirDeleted_then_complete() throws Exception {
        // Given
        initializeProcessor(null);
        driverThread.start();

        // When
        assertTrue(workDir.delete());

        // Then
        assertTrueEventually(() -> assertTrue(completedNormally));
    }

    @Test
    public void when_lineAddedInChunks_then_readAtOnce() throws Exception {
        // Given
        initializeProcessor(null);
        driverThread.start();

        ILogger logger = Logger.getLogger(this.getClass());

        // When
        Path file1 = workDir.toPath().resolve("a.txt");
        Path file2 = workDir.toPath().resolve("b.txt");
        writeToFile(file1, "incomplete1");
        writeToFile(file2, "incomplete2");
        Thread.sleep(2000);
        logger.info("complete1");
        writeToFile(file1, " complete1\n");
        Thread.sleep(2000);
        logger.info("complete2");
        writeToFile(file2, " complete2\n");

        // Then
        List<Entry<String, String>> expected = asList(
                entry("a.txt", "incomplete1 complete1"),
                entry("b.txt", "incomplete2 complete2"));
        assertTrueEventually(() -> assertEquals(expected, outboxLines), ASSERT_COUNT_TIMEOUT_SECONDS);
    }

    @Test
    public void when_multipleLinesAdded_then_seenOneByOne() throws Exception {
        // Given
        initializeProcessor(null);
        driverThread.start();

        // When
        Path file = workDir.toPath().resolve("a.txt");
        writeToFile(file, "line1\n");
        List<Entry<String, String>> expected = new ArrayList<>();
        expected.add(entry("a.txt", "line1"));
        assertTrueEventually(() -> assertEquals(expected, outboxLines), ASSERT_COUNT_TIMEOUT_SECONDS);
        writeToFile(file, "line2\n");
        expected.add(entry("a.txt", "line2"));
        assertTrueEventually(() -> assertEquals(expected, outboxLines), ASSERT_COUNT_TIMEOUT_SECONDS);
        writeToFile(file, "line3\n");
        expected.add(entry("a.txt", "line3"));
        assertTrueEventually(() -> assertEquals(expected, outboxLines), ASSERT_COUNT_TIMEOUT_SECONDS);
    }

    private void writeToFile(Path file, String text) throws IOException {
        try (Writer wr = Files.newBufferedWriter(file, StandardOpenOption.APPEND, StandardOpenOption.CREATE)) {
            wr.append(text);
        }
    }

    private void driveProcessor() {
        try {
            while (!completedNormally && !interrupted()) {
                completedNormally = processor.complete();
                outbox.drainQueueAndReset(0, outboxLines, false);
                updateFileOffsetsSize();
            }
        } catch (Throwable e) {
            driverException = e;
        }
    }

    private void updateFileOffsetsSize() {
        fileOffsetsSize = processor.fileOffsets.size();
    }

    private void initializeProcessor(String glob) throws Exception {
        if (glob == null) {
            glob = "*";
        }
        processor = new StreamFilesP<>(workDir.getAbsolutePath(), UTF_8, glob, false, Util::entry);
        outbox = new TestOutbox(1);
        Context ctx = new TestProcessorContext()
                .setLogger(Logger.getLogger(StreamFilesP.class));
        processor.init(outbox, ctx);
    }

    // Asserts the eventual stable state of the item counter as follows:
    // 1. Wait for the count to reach at least the expected value
    // 2. Ensure the value is exactly as expected
    // 3. Wait a bit more
    // 4. Ensure the value hasn't increased
    private void assertEmittedCountEventually(long expected) throws Exception {
        assertTrueEventually(() -> assertTrue("emittedCount=" + outboxLines.size() + ", expected=" + expected,
                outboxLines.size() >= expected), ASSERT_COUNT_TIMEOUT_SECONDS);
        assertEquals(expected, outboxLines.size());
        Thread.sleep(2000);
        assertEquals(expected, outboxLines.size());
    }
}
