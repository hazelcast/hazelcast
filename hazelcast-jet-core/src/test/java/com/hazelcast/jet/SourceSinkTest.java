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

package com.hazelcast.jet;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.runtime.JetPair;
import com.hazelcast.jet.sink.FileSink;
import com.hazelcast.jet.sink.ListSink;
import com.hazelcast.jet.sink.MapSink;
import com.hazelcast.jet.source.FileSource;
import com.hazelcast.jet.source.ListSource;
import com.hazelcast.jet.source.MapSource;
import com.hazelcast.jet.runtime.OutputCollector;
import com.hazelcast.jet.runtime.InputChunk;
import com.hazelcast.jet.io.Pair;
import com.hazelcast.jet.strategy.MemberDistributionStrategy;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.strategy.MemberDistributionStrategy.singleMember;
import static org.junit.Assert.assertEquals;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class SourceSinkTest extends JetTestSupport {

    private static final int COUNT = 10_000;
    private static final int NODE_COUNT = 3;

    private static HazelcastInstance instance;

    @BeforeClass
    public static void initCluster() {
        instance = createCluster(NODE_COUNT);
    }

    @Test
    public void testMapToList() throws Exception {
        IMap<Integer, Integer> sourceMap = getMap(instance);
        IList<Integer> sinkList = getList(instance);
        fillMapWithInts(sourceMap, COUNT);

        DAG dag = new DAG();
        Vertex vertex = createVertex("Dummy", TestProcessors.Noop.class);
        vertex.addSource(new MapSource(sourceMap));
        vertex.addSink(new ListSink(sinkList));
        dag.addVertex(vertex);
        Job job = JetEngine.getJob(instance, "mapToList", dag);

        execute(job);

        assertEquals(COUNT, sinkList.size());
    }

    @Test
    public void testListToList() throws Exception {

        IList<Integer> sourceList = getList(instance);
        fillListWithInts(sourceList, COUNT);

        IList<Integer> targetList = getList(instance);

        DAG dag = new DAG();
        Vertex vertex = createVertex("vertex", TestProcessors.Noop.class);
        vertex.addSource(new ListSource(sourceList));
        vertex.addSink(new ListSink(targetList));
        dag.addVertex(vertex);

        Job job = JetEngine.getJob(instance, "listToList", dag);

        execute(job);
        assertEquals(COUNT, targetList.size());
    }

    @Test
    public void testMapToMultipleSinks() throws Exception {

        IMap<Integer, Integer> sourceMap = instance.getMap("sourceMap");

        fillMapWithInts(sourceMap, COUNT);

        IList<Integer> sinkList = instance.getList("sinkList");
        IMap<Integer, Integer> sinkMap = instance.getMap("sinkMap");

        DAG dag = new DAG();
        Vertex vertex1 = createVertex("vertex1", TestProcessors.Noop.class);
        Vertex vertex2 = createVertex("vertex2", TestProcessors.Noop.class);

        vertex1.addSource(new MapSource(sourceMap));
        vertex1.addSink(new ListSink(sinkList));
        vertex2.addSink(new MapSink(sinkMap));

        dag.addVertex(vertex1);
        dag.addVertex(vertex2);
        dag.addEdge(new Edge("edge", vertex1, vertex2));
        Job job = JetEngine.getJob(instance, "multipleSinks", dag);

        execute(job);

        assertEquals(COUNT, sinkList.size());
        assertEquals(COUNT, sinkMap.size());
    }

    @Test
    public void testFileToFile() throws Exception {

        File input = createInputFile();
        File output = File.createTempFile("output", null);

        DAG dag = new DAG();

        Vertex vertex1 = createVertex("vertex1", Parser.class);
        vertex1.addSource(new FileSource(input.getPath()));

        Vertex vertex2 = createVertex("vertex2", TestProcessors.Noop.class, 1);
        vertex2.addSink(new FileSink(output.getPath()));

        dag.addVertex(vertex1);
        dag.addVertex(vertex2);

        Edge edge = new Edge("edge", vertex1, vertex2)
                .distributed(singleMember(instance.getCluster().getLocalMember()));
        dag.addEdge(edge);

        Job job = JetEngine.getJob(instance, "fileToFile", dag);
        execute(job);

        List<String> files = Files.readAllLines(output.toPath());
        assertEquals(COUNT * NODE_COUNT, files.size());
    }

    private File createInputFile() throws IOException {
        File input = File.createTempFile("input", null);
        try (FileWriter writer = new FileWriter(input)) {
            for (int i = 0; i < COUNT; i++) {
                writer.write(Integer.toString(i));
                writer.write("\n");
            }
        }
        return input;
    }

    public static class Parser implements Processor<Pair<Integer, String>, Pair<Integer, Integer>> {
        @Override
        public boolean process(InputChunk<Pair<Integer, String>> input,
                               OutputCollector<Pair<Integer, Integer>> output,
                               String sourceName) throws Exception {
            for (Pair<Integer, String> pair : input) {
                int val = Integer.parseInt(pair.getValue());
                output.collect(new JetPair<>(val, val));
            }
            return true;
        }
    }
}
