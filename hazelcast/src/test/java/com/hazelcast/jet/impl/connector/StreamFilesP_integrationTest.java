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

import com.hazelcast.collection.IList;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeListP;
import static com.hazelcast.jet.core.processor.SourceProcessors.streamFilesP;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
@Ignore
public class StreamFilesP_integrationTest extends JetTestSupport {

    private HazelcastInstance instance;
    private File directory;
    private IList<Entry<String, String>> list;

    @Before
    public void setup() throws Exception {
        instance = createHazelcastInstance();
        directory = createTempDirectory();
        list = instance.getList("writer");
    }

    @Test
    public void when_appendingToPreexisting_then_pickupNewLines() throws Exception {
        DAG dag = buildDag();

        // this is a pre-existing file, should not be picked up
        File file = createNewFile();
        appendToFile(file, "hello", "pre-existing");
        sleepAtLeastMillis(50);

        Future<Void> jobFuture = instance.getJet().newJob(dag).getFuture();
        // wait for the processor to initialize
        sleepAtLeastSeconds(2);

        // pre-existing file should not be picked up
        assertEquals(0, list.size());
        appendToFile(file, "third line");
        // now, all three lines are picked up
        assertTrueEventually(() -> assertEquals(1, list.size()));

        finishDirectory(jobFuture, file);
    }

    @Test
    public void when_appendingToPreexistingIncompleteLine_then_pickupCompleteLines() throws Exception {
        DAG dag = buildDag();

        // this is a pre-existing file, should not be picked up
        File file = createNewFile();
        try (PrintWriter writer = new PrintWriter(new FileOutputStream(file, true))) {
            // note: no newline appended
            writer.write("hello");
        }
        sleepAtLeastMillis(50);

        Future<Void> jobFuture = instance.getJet().newJob(dag).getFuture();
        // wait for the processor to initialize
        sleepAtLeastSeconds(2);

        // pre-existing file should not be picked up
        assertEquals(0, list.size());
        // this completes the first line and a second one - only the second one should be picked
        appendToFile(file, "world", "second line");
        // now, all three lines are picked up
        assertTrueEventually(() -> assertEquals(1, list.size()));
        assertEquals(entry(file.getName(), "second line"), list.get(0));

        finishDirectory(jobFuture, file);
    }

    @Test
    public void when_withCrlf_then_pickupCompleteLines() throws Exception {
        DAG dag = buildDag();

        // this is a pre-existing file, should not be picked up
        File file = createNewFile();
        try (PrintWriter writer = new PrintWriter(new FileOutputStream(file, true))) {
            // note: no newline appended
            writer.write("hello world\r");
        }
        sleepAtLeastMillis(50);

        Future<Void> jobFuture = instance.getJet().newJob(dag).getFuture();
        // wait for the processor to initialize
        sleepAtLeastSeconds(2);

        // pre-existing file should not be picked up
        assertEquals(0, list.size());
        // this completes the first line and a second one - only the second one should be picked
        try (PrintWriter writer = new PrintWriter(new FileOutputStream(file, true))) {
            // note: no newline appended
            writer.write("\nsecond line\r\n");
        }
        // now, all three lines are picked up
        assertTrueEventually(() -> assertEquals(1, list.size()));
        assertEquals(entry(file.getName(), "second line"), list.get(0));

        finishDirectory(jobFuture, file);
    }

    @Test
    @Ignore
    public void when_newAndModified_then_pickupAddition() throws Exception {
        DAG dag = buildDag();

        Future<Void> jobFuture = instance.getJet().newJob(dag).getFuture();
        // wait for the processor to initialize
        sleepAtLeastSeconds(2);

        assertEquals(0, list.size());
        File file = new File(directory, randomName());
        appendToFile(file, "hello", "world");
        assertTrueEventually(() -> assertEquals(2, list.size()));

        // now add one more line, only this line should be picked up
        appendToFile(file, "third line");
        assertTrueEventually(() -> assertEquals(3, list.size()));

        finishDirectory(jobFuture, file);
    }

    @Test
    public void when_fileWithManyLines_then_emitCooperatively() throws Exception {
        DAG dag = buildDag();

        Future<Void> jobFuture = instance.getJet().newJob(dag).getFuture();
        // wait for the processor to initialize
        sleepAtLeastSeconds(2);

        assertEquals(0, list.size());
        File subdir = new File(directory, "subdir");
        assertTrue(subdir.mkdir());
        File fileInSubdir = new File(subdir, randomName());
        appendToFile(fileInSubdir, IntStream.range(0, 5000).mapToObj(String::valueOf).toArray(String[]::new));

        // move the file to watchedDirectory
        File file = new File(directory, fileInSubdir.getName());
        assertTrue(fileInSubdir.renameTo(file));

        assertTrueEventually(() -> assertEquals(5000, list.size()));

        finishDirectory(jobFuture, file, subdir);
    }

    @Test
    @Ignore
    public void stressTest() throws Exception {
        // Here, I will start the job and in two separate threads I'll write fixed number of lines
        // to two different log files, with few ms hiccups between each line.
        // At the end, I'll check, if all the contents matches.
        DAG dag = buildDag();

        Future<Void> jobFuture = instance.getJet().newJob(dag).getFuture();
        // wait for the processor to initialize
        sleepAtLeastSeconds(2);
        int numLines = 10_000;
        File file1 = new File(directory, "file1.log");
        File file2 = new File(directory, "file2.log");
        Thread t1 = new Thread(new RandomWriter(file1, numLines, "file0"));
        Thread t2 = new Thread(new RandomWriter(file2, numLines, "file1"));
        t1.start();
        t2.start();
        t1.join();
        t2.join();

        // wait for the list to be full
        assertTrueEventually(() -> assertEquals(2 * numLines, list.size()));

        // check the list
        Set<Integer>[] expectedNumbers = new Set[] {
                IntStream.range(0, numLines).boxed().collect(Collectors.toSet()),
                IntStream.range(0, numLines).boxed().collect(Collectors.toSet())};

        for (Entry<String, String> logLine : list) {
            // logLine has the form "fileN M ...", N is fileIndex, M is line number
            int fileIndex = logLine.getValue().charAt(4) - '0';
            int lineIndex = Integer.parseInt(logLine.getValue().split(" ", 3)[1]);
            assertTrue(expectedNumbers[fileIndex].remove(lineIndex));
        }

        assertEquals(Collections.emptySet(), expectedNumbers[0]);
        assertEquals(Collections.emptySet(), expectedNumbers[1]);

        finishDirectory(jobFuture, file1, file2);
    }

    private static class RandomWriter implements Runnable {

        private final File file;
        private final int numLines;
        private final String prefix;
        private final Random random = new Random();

        RandomWriter(File file, int numLines, String prefix) {
            this.file = file;
            this.numLines = numLines;
            this.prefix = prefix;
        }

        @Override
        public void run() {
            try (FileOutputStream fos = new FileOutputStream(file);
                    OutputStreamWriter osw = new OutputStreamWriter(fos, UTF_8);
                    BufferedWriter bw = new BufferedWriter(osw)
            ) {
                for (int i = 0; i < numLines; i++) {
                    bw.write(prefix + ' ' + i +
                            " Lorem ipsum dolor sit amet, consectetur adipiscing elit," +
                            " sed do eiusmod tempor incididunt ut labore et dolore magna aliqua\n");
                    bw.flush();
                    osw.flush();
                    fos.flush();
                    if (random.nextInt(100) < 5) {
                        LockSupport.parkNanos(MILLISECONDS.toNanos(1));
                    }
                }
            } catch (IOException e) {
                throw sneakyThrow(e);
            }
        }
    }

    private DAG buildDag() {
        DAG dag = new DAG();
        Vertex reader = dag.newVertex("reader", streamFilesP(directory.getPath(), UTF_8, "*", false, Util::entry))
                           .localParallelism(1);
        Vertex writer = dag.newVertex("writer", writeListP(list.getName())).localParallelism(1);
        dag.edge(between(reader, writer));
        return dag;
    }

    private File createNewFile() {
        File file = new File(directory, randomName());
        assertTrueEventually(() -> assertTrue(file.createNewFile()));
        return file;
    }

    private void finishDirectory(Future<Void> jobFuture, File ... files) throws Exception {
        for (File file : files) {
            logger.info("deleting " + file + "...");
            assertTrueEventually(() -> assertTrue("Failed to delete " + file, file.delete()));
            logger.info("deleted " + file);
        }
        assertTrueEventually(() -> assertTrue("Failed to delete " + directory, directory.delete()));
        assertTrueEventually(() -> assertTrue("job should complete eventually", jobFuture.isDone()));
        // called for side-effect of throwing exception, if the job failed
        jobFuture.get();
    }
}
