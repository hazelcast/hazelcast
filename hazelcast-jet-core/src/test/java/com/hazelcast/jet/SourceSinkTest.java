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
import com.hazelcast.jet.container.ProcessorContext;
import com.hazelcast.jet.dag.DAG;
import com.hazelcast.jet.dag.Edge;
import com.hazelcast.jet.dag.Vertex;
import com.hazelcast.jet.dag.sink.FileSink;
import com.hazelcast.jet.dag.sink.ListSink;
import com.hazelcast.jet.dag.sink.MapSink;
import com.hazelcast.jet.dag.source.FileSource;
import com.hazelcast.jet.dag.source.ListSource;
import com.hazelcast.jet.dag.source.MapSource;
import com.hazelcast.jet.data.io.ConsumerOutputStream;
import com.hazelcast.jet.data.io.ProducerInputStream;
import com.hazelcast.jet.data.tuple.JetTuple2;
import com.hazelcast.jet.job.Job;
import com.hazelcast.jet.io.tuple.Tuple2;
import com.hazelcast.jet.processor.ContainerProcessor;
import com.hazelcast.jet.strategy.SingleNodeShufflingStrategy;
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
        Job job = JetEngine.getJob(instance, "mapToList");
        IMap<Integer, Integer> sourceMap = getMap(instance);
        IList<Integer> sinkList = getList(instance);
        fillMapWithInts(sourceMap, COUNT);

        DAG dag = new DAG();
        Vertex vertex = createVertex("Dummy", TestProcessors.Noop.class);
        vertex.addSource(new MapSource(sourceMap));
        vertex.addSink(new ListSink(sinkList));
        dag.addVertex(vertex);
        job.submit(dag);

        execute(job);

        assertEquals(COUNT, sinkList.size());
    }

    @Test
    public void testListToList() throws Exception {
        Job job = JetEngine.getJob(instance, "listToList");

        IList<Integer> sourceList = getList(instance);
        fillListWithInts(sourceList, COUNT);

        IList<Integer> targetList = getList(instance);

        DAG dag = new DAG();
        Vertex vertex = createVertex("vertex", TestProcessors.Noop.class);
        vertex.addSource(new ListSource(sourceList));
        vertex.addSink(new ListSink(targetList));
        dag.addVertex(vertex);

        job.submit(dag);

        execute(job);
        assertEquals(COUNT, targetList.size());
    }

    @Test
    public void testMapToMultipleSinks() throws Exception {
        Job job = JetEngine.getJob(instance, "multipleSinks");

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
        job.submit(dag);

        execute(job);

        assertEquals(COUNT, sinkList.size());
        assertEquals(COUNT, sinkMap.size());
    }

    @Test
    public void testFileToFile() throws Exception {
        Job job = JetEngine.getJob(instance, "fileToFile");

        File input = createInputFile();
        File output = File.createTempFile("output", null);

        DAG dag = new DAG();

        Vertex vertex1 = createVertex("vertex1", Parser.class);
        vertex1.addSource(new FileSource(input.getPath()));

        Vertex vertex2 = createVertex("vertex2", TestProcessors.Noop.class, 1);
        vertex2.addSink(new FileSink(output.getPath()));

        dag.addVertex(vertex1);
        dag.addVertex(vertex2);

        Edge edge = new Edge.EdgeBuilder("edge", vertex1, vertex2)
                .shuffling(true)
                .shufflingStrategy(new SingleNodeShufflingStrategy(
                        instance.getCluster().getLocalMember().getAddress()))
                .build();
        dag.addEdge(edge);

        job.submit(dag);
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

    public static class Parser implements ContainerProcessor<Tuple2<Integer, String>, Tuple2<Integer, Integer>> {
        @Override
        public boolean process(ProducerInputStream<Tuple2<Integer, String>> inputStream,
                               ConsumerOutputStream<Tuple2<Integer, Integer>> outputStream,
                               String sourceName, ProcessorContext processorContext) throws Exception {
            for (Tuple2<Integer, String> tuple : inputStream) {
                int val = Integer.parseInt(tuple.get1());
                outputStream.consume(new JetTuple2<>(val, val));
            }
            return true;
        }
    }
}
