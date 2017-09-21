/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.JetTestSupport;
import com.hazelcast.jet.Outbox;
import com.hazelcast.jet.Processor.Context;
import com.hazelcast.jet.ProcessorSupplier;
import com.hazelcast.jet.SnapshotOutbox;
import com.hazelcast.logging.Log4jFactory;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.test.HazelcastParallelClassRunner;
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
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.nio.file.Files;

import static com.hazelcast.jet.processor.SourceProcessors.streamFiles;
import static java.lang.Thread.interrupted;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class StreamFilesPTest extends JetTestSupport {

    private static final long LINE_COUNT = 1_000;
    private static final int ASSERT_COUNT_TIMEOUT_SECONDS = 10;

    @Rule public TestName testName = new TestName();

    private File workDir;
    private StreamFilesP processor;

    private Thread driverThread;
    private long emittedCount;

    private volatile int fileOffsetsSize;
    private volatile boolean completedNormally;

    @Before
    public void before() throws Exception {
        workDir = Files.createTempDirectory("jet-test-streamFilesPTest").toFile();
        driverThread = new Thread(this::driveProcessor,
                "Driving StreamFileP (" + testName.getMethodName() + ')');
    }

    @After
    public void after() throws InterruptedException {
        driverThread.interrupt();
        driverThread.join();
        IOUtil.delete(workDir);
    }

    @Test
    public void supplier() {
        ProcessorSupplier supplier = streamFiles(workDir.getAbsolutePath(), UTF_8, "*");
        assertEquals(1, supplier.get(1).size());
        supplier.complete(null);
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
        assertEquals(0, emittedCount);
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

    private void driveProcessor() {
        while (!completedNormally && !interrupted()) {
            completedNormally = processor.complete();
            updateFileOffsetsSize();
        }
        completedNormally = true;
    }

    private void updateFileOffsetsSize() {
        fileOffsetsSize = processor.fileOffsets.size();
    }

    private void initializeProcessor(String glob) {
        if (glob == null) {
            glob = "*";
        }
        processor = new StreamFilesP(workDir.getAbsolutePath(), UTF_8, glob, 1, 0);
        Outbox outbox = mock(Outbox.class);
        SnapshotOutbox ssOutbox = mock(SnapshotOutbox.class);
        when(outbox.offer(any())).thenAnswer(item -> {
            emittedCount++;
            return true;
        });
        Context ctx = mock(Context.class);
        when(ctx.logger()).thenReturn(new Log4jFactory().getLogger("testing"));
        processor.init(outbox, ssOutbox, ctx);
    }

    // Asserts the eventual stable state of the item counter as follows:
    // 1. Wait for the count to reach at least the expected value
    // 2. Ensure the value is exactly as expceted
    // 3. Wait a bit more
    // 4. Ensure the value hasn't increased
    private void assertEmittedCountEventually(long expected) throws Exception {
        assertTrueEventually(() -> assertTrue("emittedCount=" + emittedCount + ", expected=" + expected,
                emittedCount >= expected), ASSERT_COUNT_TIMEOUT_SECONDS);
        assertEquals(expected, emittedCount);
        Thread.sleep(2000);
        assertEquals(expected, emittedCount);
    }
}
