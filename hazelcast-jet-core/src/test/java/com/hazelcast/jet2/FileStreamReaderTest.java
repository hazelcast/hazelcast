/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet2;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.jet2.impl.FileStreamReader;
import com.hazelcast.jet2.impl.IListWriter;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class FileStreamReaderTest extends HazelcastTestSupport {

    @Test
    public void testFileStreamReader_new() throws IOException, InterruptedException {
        HazelcastInstance instance = createHazelcastInstance();
        JetEngine jetEngine = JetEngine.get(instance, randomName());
        File directory = createTempFileDirectory();
        DAG dag = new DAG();
        Vertex producer = new Vertex("producer", FileStreamReader.supplier(directory.getPath(),
                FileStreamReader.WatchType.NEW))
                .parallelism(4);

        Vertex consumer = new Vertex("consumer", IListWriter.supplier("consumer"))
                .parallelism(1);

        dag.addVertex(producer)
                .addVertex(consumer)
                .addEdge(new Edge(producer, consumer));

        jetEngine.newJob(dag).execute();
        sleepAtLeastSeconds(3);

        File file = new File(directory, randomName());
        writeNewLine(file, "hello", "world");
        IList<Object> list = instance.getList("consumer");
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(2, list.size());
            }
        });
    }

    @Test
    @Ignore
    public void testFileStreamReader_reprocess() throws IOException, InterruptedException {
        HazelcastInstance instance = createHazelcastInstance();
        JetEngine jetEngine = JetEngine.get(instance, randomName());
        File directory = createTempFileDirectory();
        DAG dag = new DAG();
        Vertex producer = new Vertex("producer", FileStreamReader.supplier(directory.getPath(),
                FileStreamReader.WatchType.REPROCESS))
                .parallelism(4);

        Vertex consumer = new Vertex("consumer", IListWriter.supplier("consumer"))
                .parallelism(1);

        dag.addVertex(producer)
                .addVertex(consumer)
                .addEdge(new Edge(producer, consumer));

        jetEngine.newJob(dag).execute();
        sleepAtLeastSeconds(3);

        File file = new File(directory, randomName());
        writeNewLine(file, "hello", "world");
        IList<Object> list = instance.getList("consumer");
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(2, list.size());

            }
        });
        writeNewLine(file, "jet");
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(5, list.size());
            }
        });
    }

    @Test
    public void testFileStreamReader_appendOnly() throws IOException, InterruptedException {
        HazelcastInstance instance = createHazelcastInstance();
        JetEngine jetEngine = JetEngine.get(instance, randomName());
        File directory = createTempFileDirectory();
        File file = new File(directory, randomName());
        writeNewLine(file, "init");
        DAG dag = new DAG();
        Vertex producer = new Vertex("producer", FileStreamReader.supplier(directory.getPath(),
                FileStreamReader.WatchType.APPENDED_ONLY))
                .parallelism(4);

        Vertex consumer = new Vertex("consumer", IListWriter.supplier("consumer"))
                .parallelism(1);

        dag.addVertex(producer)
                .addVertex(consumer)
                .addEdge(new Edge(producer, consumer));

        jetEngine.newJob(dag).execute();
        sleepAtLeastSeconds(3);

        writeNewLine(file, "hello", "world");
        IList<Object> list = instance.getList("consumer");
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(3, list.size());
            }
        });
        writeNewLine(file, "append", "only");
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(5, list.size());
            }
        });
    }

    public void writeNewLine(File file, String... payloads) throws IOException {
        FileOutputStream outputStream = new FileOutputStream(file, true);
        PrintWriter writer = new PrintWriter(outputStream);
        for (String payload : payloads) {
            writer.write(payload + "\n");
        }
        writer.flush();
        writer.close();
        outputStream.close();
    }

    private File createTempFileDirectory() throws IOException {
        Path directory = Files.createTempDirectory(randomName());
        File file = directory.toFile();
        file.deleteOnExit();
        return file;
    }


}
