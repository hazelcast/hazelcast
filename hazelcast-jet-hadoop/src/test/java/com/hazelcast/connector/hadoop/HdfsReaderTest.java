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

package com.hazelcast.connector.hadoop;

import com.hazelcast.core.IList;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetTestSupport;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.impl.connector.IListWriter;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.concurrent.Future;

import static com.hazelcast.jet.Edge.between;
import static org.junit.Assert.assertEquals;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class HdfsReaderTest extends JetTestSupport {

    @Test
    public void testReadFile() throws Exception {
        Path path = writeToFile("hello 1\n", "world 2\n", "hello 3\n", "world 4\n");

        JetInstance instance = createJetInstance();
        createJetInstance();
        DAG dag = new DAG();
        Vertex producer = new Vertex("producer", HdfsReader.supplier(path.toString()))
                .localParallelism(4);

        Vertex consumer = new Vertex("consumer", IListWriter.supplier("consumer"))
                .localParallelism(1);

        dag.vertex(producer)
           .vertex(consumer)
           .edge(between(producer, consumer));

        Future<Void> future = instance.newJob(dag).execute();
        assertCompletesEventually(future);


        IList<Object> list = instance.getList("consumer");
        assertEquals(4, list.size());
    }

    private static Path writeToFile(String... values) throws IOException {
        LocalFileSystem local = FileSystem.getLocal(new Configuration());
        Path path = new Path(randomString());
        local.createNewFile(path);
        FSDataOutputStream outputStream = local.create(path);
        local.deleteOnExit(path);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream));
        for (String value : values) {
            writer.write(value);
        }
        writer.flush();
        writer.close();
        outputStream.close();
        return path;
    }
}
