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

import com.hazelcast.core.IList;
import com.hazelcast.jet.impl.connector.FileStreamReader;
import com.hazelcast.jet.impl.connector.IListWriter;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
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

import static com.hazelcast.jet.Edge.between;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class FileStreamReaderTest extends JetTestSupport {

    private JetTestInstanceFactory factory;
    private JetInstance instance;

    @Before
    public void setup() {
        factory = new JetTestInstanceFactory();
        instance = factory.newMember();
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void testFileStreamReader_new() throws IOException, InterruptedException {
        File directory = createTempFileDirectory();
        DAG dag = new DAG();
        Vertex producer = new Vertex("producer", FileStreamReader.supplier(directory.getPath(),
                FileStreamReader.WatchType.NEW))
                .localParallelism(4);

        Vertex consumer = new Vertex("consumer", IListWriter.supplier("consumer"))
                .localParallelism(1);

        dag.vertex(producer)
           .vertex(consumer)
           .edge(between(producer, consumer));

        instance.newJob(dag).execute();
        sleepAtLeastSeconds(10);

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
    public void testFileStreamReader_reprocess() throws IOException, InterruptedException {
        File directory = createTempFileDirectory();
        File file = new File(directory, randomName());
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertTrue(file.createNewFile());
            }
        });
        DAG dag = new DAG();
        Vertex producer = new Vertex("producer", FileStreamReader.supplier(directory.getPath(),
                FileStreamReader.WatchType.REPROCESS))
                .localParallelism(4);

        Vertex consumer = new Vertex("consumer", IListWriter.supplier("consumer"))
                .localParallelism(1);

        dag.vertex(producer)
           .vertex(consumer)
           .edge(between(producer, consumer));

        instance.newJob(dag).execute();
        sleepAtLeastSeconds(10);

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
        File directory = createTempFileDirectory();
        File file = new File(directory, randomName());
        writeNewLine(file, "init");
        DAG dag = new DAG();
        Vertex producer = new Vertex("producer", FileStreamReader.supplier(directory.getPath(),
                FileStreamReader.WatchType.APPENDED_ONLY))
                .localParallelism(4);

        Vertex consumer = new Vertex("consumer", IListWriter.supplier("consumer"))
                .localParallelism(1);

        dag.vertex(producer)
           .vertex(consumer)
           .edge(between(producer, consumer));

        instance.newJob(dag).execute();
        sleepAtLeastSeconds(10);

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

    private static void writeNewLine(File file, String... payloads) throws IOException {
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
