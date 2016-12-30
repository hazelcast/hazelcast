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

package com.hazelcast.connector.hadoop;

import com.hazelcast.core.IList;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Edge;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetTestSupport;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.impl.connector.IListWriter;
import com.hazelcast.jet.impl.connector.IMapReader;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class HdfsWriterTest extends JetTestSupport {

    @Test
    public void testWriteFile() throws Exception {
        int messageCount = 20;
        String mapName = randomMapName();
        JetInstance instance = createJetInstance();
        createJetInstance();

        Map<Integer, Integer> map = IntStream.range(0, messageCount).boxed().collect(Collectors.toMap(m -> m, m -> m));
        instance.getMap(mapName).putAll(map);

        DAG dag = new DAG();
        Vertex producer = new Vertex("producer", IMapReader.supplier(mapName))
                .parallelism(1);

        Path path = getPath();
        Vertex consumer = new Vertex("consumer", HdfsWriter.supplier(path.toString()))
                .parallelism(4);

        dag.addVertex(producer)
           .addVertex(consumer)
           .addEdge(new Edge(producer, consumer));

        Future<Void> future = instance.newJob(dag).execute();
        assertCompletesEventually(future);


        dag = new DAG();
        producer = new Vertex("producer", HdfsReader.supplier(path.toString()))
                .parallelism(8);

        consumer = new Vertex("consumer", IListWriter.supplier("results"))
                .parallelism(1);

        dag.addVertex(producer)
           .addVertex(consumer)
           .addEdge(new Edge(producer, consumer));
        future = instance.newJob(dag).execute();
        assertCompletesEventually(future);


        IList<Object> results = instance.getList("results");
        assertEquals(messageCount, results.size());
    }

    public Path getPath() throws IOException {
        LocalFileSystem local = FileSystem.getLocal(new Configuration());
        Path path = new Path(randomString());
        local.mkdirs(path);
        local.deleteOnExit(path);
        return path;
    }
}
