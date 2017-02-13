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

import com.hazelcast.jet.DAG;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetTestSupport;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.impl.connector.ReadFileStreamP.WatchType;
import com.hazelcast.jet.stream.IStreamList;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.Processors.writeList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
@Ignore
public class ReadFileStreamPTest extends JetTestSupport {

    private JetInstance instance;
    private File directory;
    private IStreamList<String> list;

    @Before
    public void setup() throws IOException {
        instance = createJetMember();
        directory = createTempDirectory();
        list = instance.getList("writer");
    }

    @Test
    public void when_watchType_new_then_ignorePreexisting() throws IOException, InterruptedException, ExecutionException {
        DAG dag = buildDag(WatchType.NEW);

        // this is a pre-existing file, should not be picked up
        File file = createNewFile();
        appendToFile(file,   "hello", "pre-existing");
        sleepAtLeastMillis(50);

        Future<Void> jobFuture = instance.newJob(dag).execute();
        // wait for the processor to initialize and pickup what it wants to
        sleepAtLeastSeconds(2);

        // check that nothing is picked up
        assertEquals(0, list.size());

        finishDirectory(jobFuture, file);
    }

    @Test
    @Ignore //this does not work reliably due to ENTRY_CREATE might be reported before contents is written
    public void when_watchType_new_then_pickupNew() throws IOException, InterruptedException, ExecutionException {
        DAG dag = buildDag(WatchType.NEW);

        Future<Void> jobFuture = instance.newJob(dag).execute();
        // wait for the processor to initialize
        sleepAtLeastSeconds(2);

        File file = new File(directory, randomName());
        appendToFile(file, "hello");

        assertTrueEventually(() -> assertEquals(1, list.size()), 2);

        finishDirectory(jobFuture, file);
    }

    @Test
    @Ignore //this does not work reliably due to ENTRY_CREATE might be reported before contents is written
    public void when_watchType_new_then_doNotPickupModifications() throws IOException, InterruptedException, ExecutionException {
        DAG dag = buildDag(WatchType.NEW);

        Future<Void> jobFuture = instance.newJob(dag).execute();
        // wait for the processor to initialize
        sleepAtLeastSeconds(2);

        File file = new File(directory, randomName());
        appendToFile(file, "hello", "pre-existing");

        assertTrueEventually(() -> assertEquals(2, list.size()), 2);

        appendToFile(file, "third line");

        // wait for the processor to pickup what it wants to
        sleepAtLeastSeconds(1);

        // the list should not have the third line
        assertEquals(2, list.size());

        finishDirectory(jobFuture, file);
    }

    @Test
    public void when_watchType_reProcess_then_pickupPreexistingAfterModify() throws IOException, InterruptedException, ExecutionException {
        DAG dag = buildDag(WatchType.REPROCESS);

        // this is a pre-existing file, should not be picked up
        File file = createNewFile();
        appendToFile(file, "hello", "pre-existing");
        sleepAtLeastMillis(50);

        Future<Void> jobFuture = instance.newJob(dag).execute();
        // wait for the processor to initialize
        sleepAtLeastSeconds(2);
        // check that nothing was picked up
        assertEquals(0, list.size());

        // append more lines to the file, these should not be picked up
        appendToFile(file, "third line");
        assertTrueEventually(() -> assertCountDistinct(3, list), 2);

        finishDirectory(jobFuture, file);
    }

    @Test
    public void when_watchType_reProcess_then_pickupNewAndModified() throws IOException, InterruptedException, ExecutionException {
        DAG dag = buildDag(WatchType.REPROCESS);

        Future<Void> jobFuture = instance.newJob(dag).execute();
        // wait for the processor to initialize
        sleepAtLeastSeconds(2);

        File file = new File(directory, randomName());
        appendToFile(file, "hello", "pre-existing");
        assertTrueEventually(() -> assertCountDistinct(2, list), 2);

        // append third line to the file, now all three lines should be picked again
        appendToFile(file, "third line");
        assertTrueEventually(() -> assertCountDistinct(3, list), 2);
        // the size should be at least five, because the first two appended lines must have been picked up at least twice
        assertTrueEventually(() -> assertTrue(list.size() >= 5), 2);

        finishDirectory(jobFuture, file);
    }

    @Test
    public void when_watchType_appendedOnly_appendingToPreexisting_then_pickupEntirely() throws IOException, InterruptedException, ExecutionException {
        DAG dag = buildDag(WatchType.APPENDED_ONLY);

        // this is a pre-existing file, should not be picked up
        File file = createNewFile();
        appendToFile(file, "hello", "pre-existing");
        sleepAtLeastMillis(50);

        Future<Void> jobFuture = instance.newJob(dag).execute();
        // wait for the processor to initialize
        sleepAtLeastSeconds(2);

        // pre-existing file should not be picked up
        assertEquals(0, list.size());
        appendToFile(file, "third line");
        // now, all three lines are picked up
        assertTrueEventually(() -> assertEquals(3, list.size()), 2);

        finishDirectory(jobFuture, file);
    }

    @Test
    public void when_watchType_appendedOnly_newAndModified_then_pickupAddition() throws IOException, InterruptedException, ExecutionException {
        DAG dag = buildDag(WatchType.APPENDED_ONLY);

        Future<Void> jobFuture = instance.newJob(dag).execute();
        // wait for the processor to initialize
        sleepAtLeastSeconds(2);

        assertEquals(0, list.size());
        File file = new File(directory, randomName());
        appendToFile(file, "hello", "world");
        assertTrueEventually(() -> assertEquals(2, list.size()), 2);

        // now add one more line, only this line should be picked up
        appendToFile(file, "third line");
        assertTrueEventually(() -> assertEquals(3, list.size()), 2);

        finishDirectory(jobFuture, file);
    }

    private DAG buildDag(WatchType type) {
        DAG dag = new DAG();
        Vertex reader = dag.newVertex("reader", ReadFileStreamP.supplier(directory.getPath(), type))
                           .localParallelism(1);
        Vertex writer = dag.newVertex("writer", writeList(list.getName())).localParallelism(1);
        dag.edge(between(reader, writer));
        return dag;
    }

    private File createNewFile() {
        File file = new File(directory, randomName());
        assertTrueEventually(() -> assertTrue(file.createNewFile()), 2);
        return file;
    }

    private static void appendToFile(File file, String... lines) throws IOException {
        try (PrintWriter writer = new PrintWriter(new FileOutputStream(file, true))) {
            for (String payload : lines) {
                writer.write(payload + '\n');
            }
        }
    }

    private static File createTempDirectory() throws IOException {
        Path directory = Files.createTempDirectory("read-file-stream-p");
        File file = directory.toFile();
        file.deleteOnExit();
        return file;
    }

    private void finishDirectory(Future<Void> jobFuture, File ... files) throws InterruptedException, ExecutionException {
        for (File file : files) {
            assertTrue(file.delete());
        }
        assertTrue(directory.delete());
        assertTrueEventually(() -> assertTrue("job should complete eventually", jobFuture.isDone()), 5);
        // called for side-effect of throwing exception, if the job failed
        jobFuture.get();
    }

    /**
     * Asserts the count of distinct elements. We use this, because reprocess might pick-up single line
     * multiple times due to multiple {@link java.nio.file.StandardWatchEventKinds#ENTRY_MODIFY}
     * events fired during writing to the file.
     */
    private <T> void assertCountDistinct(int expected, IStreamList<T> list) {
        Set<T> set = new HashSet<>(list);
        assertEquals(expected, set.size());
    }
}
