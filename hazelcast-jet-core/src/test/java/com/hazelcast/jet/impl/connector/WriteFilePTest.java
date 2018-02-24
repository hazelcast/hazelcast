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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.IListJet;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.test.HazelcastParallelClassRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeFileP;
import static com.hazelcast.jet.core.processor.SourceProcessors.readListP;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
public class WriteFilePTest extends JetTestSupport {

    // only used in when_slowSource_then_fileFlushedAfterEachItem
    private static final Semaphore semaphore = new Semaphore(0);

    private JetInstance instance;
    private Path directory;
    private Path file;
    private IListJet<String> list;

    @Before
    public void setup() throws IOException {
        instance = createJetMember();
        directory = Files.createTempDirectory("write-file-p");
        file = directory.resolve("0");
        list = instance.getList("sourceList");
    }

    @After
    public void tearDown() throws Exception {
        IOUtil.delete(directory.toFile());
    }

    @Test
    public void when_localParallelismMoreThan1_then_multipleFiles() throws Exception {
        // Given
        DAG dag = buildDag(null, null, false);
        dag.getVertex("writer").localParallelism(2);
        addItemsToList(0, 10);

        // When
        instance.newJob(dag).join();

        // Then
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory)) {
            int[] count = {0};
            stream.forEach(p -> count[0]++);
            assertEquals(2, count[0]);
        }
    }

    @Test
    @Ignore // the test keeps failing on Jenkins, even though it runs without failure hundreds of times locally
    public void when_twoMembers_then_twoFiles() throws Exception {
        // Given
        DAG dag = buildDag(null, null, false);
        addItemsToList(0, 10);
        createJetMember();

        // When
        instance.newJob(dag).join();

        // Then
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory)) {
            int[] count = {0};
            stream.forEach(p -> count[0]++);
            assertEquals(2, count[0]);
        }
    }

    @Test
    public void smokeTest_smallFile() throws Exception {
        // Given
        DAG dag = buildDag(null, null, false);
        addItemsToList(0, 10);

        // When
        instance.newJob(dag).join();

        // Then
        checkFileContents(StandardCharsets.UTF_8, 10);
    }

    @Test
    public void smokeTest_bigFile() throws Exception {
        // Given
        DAG dag = buildDag(null, null, false);
        addItemsToList(0, 100_000);

        // When
        instance.newJob(dag).join();

        // Then
        checkFileContents(StandardCharsets.UTF_8, 100_000);
    }

    @Test
    public void when_append_then_previousContentsOfFileIsKept() throws Exception {
        // Given
        DAG dag = buildDag(null, null, true);
        addItemsToList(1, 10);
        try (BufferedWriter writer = Files.newBufferedWriter(file)) {
            writer.write("0");
            writer.newLine();
        }

        // When
        instance.newJob(dag).join();

        // Then
        checkFileContents(StandardCharsets.UTF_8, 10);
    }

    @Test
    public void when_overwrite_then_previousContentsOverwritten() throws Exception {
        // Given
        DAG dag = buildDag(null, null, false);
        addItemsToList(0, 10);
        try (BufferedWriter writer = Files.newBufferedWriter(file)) {
            writer.write("bla bla");
            writer.newLine();
        }

        // When
        instance.newJob(dag).join();

        // Then
        checkFileContents(StandardCharsets.UTF_8, 10);
    }

    @Test
    public void when_slowSource_then_fileFlushedAfterEachItem() throws Exception {
        // Given
        int numItems = 10;

        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", () -> new SlowSourceP(semaphore, numItems))
                           .localParallelism(1);
        Vertex sink = dag.newVertex("sink",
                writeFileP(directory.toString(), Object::toString, StandardCharsets.UTF_8, false))
                         .localParallelism(1);
        dag.edge(between(source, sink));

        Job job = instance.newJob(dag);
        // wait, until the file is created
        assertTrueEventually(() -> assertTrue(Files.exists(file)));
        for (int i = 0; i < numItems; i++) {
            // When
            semaphore.release();
            int finalI = i;
            // Then
            assertTrueEventually(() -> checkFileContents(StandardCharsets.UTF_8, finalI + 1), 5);
        }

        // wait for the job to finish
        job.join();
    }

    @Test
    public void testCharset() throws ExecutionException, InterruptedException, IOException {
        // Given
        Charset charset = Charset.forName("iso-8859-2");
        DAG dag = buildDag(null, charset, true);
        String text = "ľščťž";
        list.add(text);

        // When
        instance.newJob(dag).join();

        // Then
        assertEquals(text + System.getProperty("line.separator"), new String(Files.readAllBytes(file), charset));
    }

    @Test
    public void test_createDirectories() throws Exception {
        // Given
        Path myFile = directory.resolve("subdir1/subdir2/" + file.getFileName());

        DAG dag = new DAG();
        Vertex reader = dag.newVertex("reader", readListP(list.getName()))
                           .localParallelism(1);
        Vertex writer = dag.newVertex("writer",
                writeFileP(myFile.toString(), Object::toString, StandardCharsets.UTF_8, false))
                           .localParallelism(1);
        dag.edge(between(reader, writer));
        addItemsToList(0, 10);

        // When
        instance.newJob(dag).join();

        // Then
        assertTrue(Files.exists(directory.resolve("subdir1")));
        assertTrue(Files.exists(directory.resolve("subdir1/subdir2")));
    }

    @Test
    public void when_toStringF_then_used() throws Exception {
        // Given
        DAG dag = buildDag(val -> Integer.toString(Integer.parseInt(val) - 1), null, false);
        addItemsToList(1, 11);

        // When
        instance.newJob(dag).join();

        // Then
        checkFileContents(StandardCharsets.UTF_8, 10);
    }

    private static class SlowSourceP extends AbstractProcessor {

        private final Semaphore semaphore;
        private final int limit;

        SlowSourceP(Semaphore semaphore, int limit) {
            this.semaphore = semaphore;
            this.limit = limit;
        }


        @Override
        public boolean complete() {
            int number = 0;
            while (number < limit) {
                uncheckRun(semaphore::acquire);
                assertTrue(tryEmit(String.valueOf(number)));
                number++;
            }
            return true;
        }

        @Override
        public boolean isCooperative() {
            return false;
        }
    }

    private void checkFileContents(Charset charset, int numTo) throws IOException {
        String actual = new String(Files.readAllBytes(file), charset);

        StringBuilder expected = new StringBuilder();
        for (int i = 0; i < numTo; i++) {
            expected.append(i).append(System.getProperty("line.separator"));
        }

        assertEquals(expected.toString(), actual);
    }

    private void addItemsToList(int from, int to) {
        for (int i = from; i < to; i++) {
            list.add(String.valueOf(i));
        }
    }

    private DAG buildDag(DistributedFunction<String, String> toStringFn, Charset charset, boolean append) {
        if (toStringFn == null) {
            toStringFn = Object::toString;
        }
        if (charset == null) {
            charset = StandardCharsets.UTF_8;
        }
        DAG dag = new DAG();
        Vertex reader = dag.newVertex("reader", readListP(list.getName()))
                           .localParallelism(1);
        Vertex writer = dag.newVertex("writer", writeFileP(directory.toString(), toStringFn, charset, append))
                           .localParallelism(1);
        dag.edge(between(reader, writer));
        return dag;
    }

}
