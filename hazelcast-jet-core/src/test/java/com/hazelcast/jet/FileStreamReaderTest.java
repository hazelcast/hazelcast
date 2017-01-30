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

package com.hazelcast.jet;

import com.hazelcast.jet.impl.connector.FileStreamReader;
import com.hazelcast.jet.impl.connector.FileStreamReader.WatchType;
import com.hazelcast.jet.impl.connector.IListWriter;
import com.hazelcast.jet.stream.IStreamList;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Future;

import static com.hazelcast.jet.Edge.between;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class FileStreamReaderTest extends JetTestSupport {

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
    public void when_watchType_new() throws IOException, InterruptedException {
        DAG dag = buildDag(WatchType.NEW);

        File file = createNewFile();

        Future<Void> jobFuture = instance.newJob(dag).execute();

        sleepAtLeastSeconds(2);
        appendToFile(file, "hello", "world");
        assertTrueEventually(() -> assertEquals(2, list.size()));

        assertTrue(file.delete());
        assertTrue(directory.delete());
        assertTrueEventually(() -> assertTrue("job should be completed eventually", jobFuture.isDone()));
    }

    @Test
    public void when_watchType_reProcess() throws IOException, InterruptedException {
        DAG dag = buildDag(WatchType.REPROCESS);

        File file = createNewFile();

        Future<Void> jobFuture = instance.newJob(dag).execute();

        sleepAtLeastSeconds(2);
        appendToFile(file, "hello", "world");
        assertTrueEventually(() -> assertEquals(2, list.size()));

        sleepAtLeastSeconds(2);
        appendToFile(file, "jet");
        assertTrueEventually(() -> assertEquals(5, list.size()));

        assertTrue(file.delete());
        assertTrue(directory.delete());
        assertTrueEventually(() -> assertTrue("job should be completed eventually", jobFuture.isDone()));
    }

    @Test
    public void when_watchType_appendedOnly() throws IOException, InterruptedException {
        DAG dag = buildDag(WatchType.APPENDED_ONLY);
        File file = createNewFile();

        appendToFile(file, "init");

        Future<Void> jobFuture = instance.newJob(dag).execute();

        sleepAtLeastSeconds(2);

        appendToFile(file, "hello", "world");
        assertTrueEventually(() -> assertEquals(3, list.size()));

        sleepAtLeastSeconds(2);

        appendToFile(file, "append", "only");
        assertTrueEventually(() -> assertEquals(5, list.size()));

        assertTrue(file.delete());
        assertTrue(directory.delete());
        assertTrueEventually(() -> assertTrue("job should be completed eventually", jobFuture.isDone()));
    }

    private DAG buildDag(WatchType type) {
        DAG dag = new DAG();
        Vertex reader = dag.newVertex("reader", FileStreamReader.supplier(directory.getPath(), type))
                           .localParallelism(1);
        Vertex writer = dag.newVertex("writer", IListWriter.supplier(list.getName())).localParallelism(1);
        dag.edge(between(reader, writer));
        return dag;
    }

    private File createNewFile() {
        File file = new File(directory, randomName());
        assertTrueEventually(() -> assertTrue(file.createNewFile()));
        return file;
    }

    private static void appendToFile(File file, String... lines) throws IOException {
        try (PrintWriter writer = new PrintWriter(new FileOutputStream(file, true))) {
            for (String payload : lines) {
                writer.write(payload + "\n");
            }
        }
    }

    private File createTempDirectory() throws IOException {
        Path directory = Files.createTempDirectory("file-stream-reader");
        File file = directory.toFile();
        file.deleteOnExit();
        return file;
    }


}
