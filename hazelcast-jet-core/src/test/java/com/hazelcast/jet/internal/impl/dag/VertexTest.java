package com.hazelcast.jet.internal.impl.dag;

import com.hazelcast.jet.dag.Edge;
import com.hazelcast.jet.processors.DummyProcessor;
import com.hazelcast.jet.container.ContainerDescriptor;
import com.hazelcast.jet.dag.Vertex;
import com.hazelcast.jet.dag.tap.SinkOutputStream;
import com.hazelcast.jet.dag.tap.SinkTap;
import com.hazelcast.jet.dag.tap.SinkTapWriteStrategy;
import com.hazelcast.jet.dag.tap.SourceTap;
import com.hazelcast.jet.dag.tap.TapType;
import com.hazelcast.jet.data.DataReader;
import com.hazelcast.jet.data.DataWriter;
import com.hazelcast.jet.data.tuple.JetTupleFactory;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.jet.base.JetBaseTest.createVertex;
import static com.hazelcast.jet.dag.tap.SinkTapWriteStrategy.APPEND;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class VertexTest {

    @Test
    public void testVertexNameAndProcessorFactory() throws Exception {
        String name = "v1";
        Class<DummyProcessor.Factory> factoryClass = DummyProcessor.Factory.class;
        Vertex v1 = createVertex(name, factoryClass);
        assertEquals(name, v1.getName());
        assertEquals(factoryClass.getName(), v1.getDescriptor().getContainerProcessorFactoryClazz());
    }

    @Test
    public void testVertexInput() throws Exception {
        Vertex v1 = createVertex("v1", DummyProcessor.Factory.class);
        Vertex input = createVertex("input", DummyProcessor.Factory.class);

        Edge edge = new Edge("e1", input, v1);
        v1.addInputVertex(input, edge);

        List<Vertex> inputVertices = v1.getInputVertices();
        List<Edge> inputEdges = v1.getInputEdges();
        assertEquals(1, inputVertices.size());
        assertEquals(1, inputEdges.size());
        assertEquals(input, inputVertices.iterator().next());
        assertEquals(edge, inputEdges.iterator().next());
    }

    @Test
    public void testVertexOutput() throws Exception {
        Vertex v1 = createVertex("v1", DummyProcessor.Factory.class);
        Vertex output = createVertex("output", DummyProcessor.Factory.class);

        Edge edge = new Edge("e1", v1, output);
        v1.addOutputVertex(output, edge);

        List<Vertex> outputVertices = v1.getOutputVertices();
        List<Edge> outputEdges = v1.getOutputEdges();
        assertEquals(1, outputVertices.size());
        assertEquals(1, outputEdges.size());
        assertEquals(output, outputVertices.iterator().next());
        assertEquals(edge, outputEdges.iterator().next());

    }

    @Test
    public void testVertexOutputShuffler() throws Exception {
        Vertex v1 = createVertex("v1", DummyProcessor.Factory.class);
        Vertex output = createVertex("output", DummyProcessor.Factory.class);

        Edge edge = new Edge.EdgeBuilder("edge", v1, output)
                .shuffling(true)
                .build();
        v1.addOutputVertex(output, edge);

        assertTrue(v1.hasOutputShuffler());
    }

    @Test
    public void testEmptyVertexHasNotOutputShuffler() throws Exception {
        Vertex v1 = createVertex("v1", DummyProcessor.Factory.class);
        assertFalse(v1.hasOutputShuffler());
    }

    @Test
    public void testVertexSources() throws Exception {
        Vertex v1 = createVertex("v1", DummyProcessor.Factory.class);

        ArrayList<String> sources = new ArrayList<String>();

        String sourceFileName = "sourceFileName";
        v1.addSourceFile(sourceFileName);
        sources.add(sourceFileName);

        String sourceListName = "sourceListName";
        v1.addSourceList(sourceListName);
        sources.add(sourceListName);

        String sourceMapName = "sourceMapName";
        v1.addSourceMap(sourceMapName);
        sources.add(sourceMapName);

        String sourceMultiMapName = "sourceMultiMapName";
        v1.addSourceMultiMap(sourceMultiMapName);
        sources.add(sourceMultiMapName);

        final String sourceTapName = "sourceTapName";
        SourceTap sourceTap = new SourceTap() {
            @Override
            public DataReader[] getReaders(ContainerDescriptor containerDescriptor, Vertex vertex, JetTupleFactory tupleFactory) {
                return new DataReader[0];
            }

            @Override
            public String getName() {
                return sourceTapName;
            }

            @Override
            public TapType getType() {
                return null;
            }
        };
        v1.addSourceTap(sourceTap);
        sources.add(sourceTapName);

        List<SourceTap> sourcesFromVertex = v1.getSources();
        assertEquals(5, sourcesFromVertex.size());
        for (SourceTap source : sourcesFromVertex) {
            String name = source.getName();
            assertTrue("Vertex should contain the source : " + name, sources.contains(name));
        }
    }

    @Test
    public void testVertexSinks() throws Exception {
        Vertex v1 = createVertex("v1", DummyProcessor.Factory.class);

        ArrayList<String> sinks = new ArrayList<String>();

        String sinkFileName = "sinkFileName";
        v1.addSinkFile(sinkFileName);
        sinks.add(sinkFileName);

        String sinkFileWithWriterStrategyName = "sinkFileWithWriterStrategyName";
        v1.addSinkFile(sinkFileWithWriterStrategyName, APPEND);
        sinks.add(sinkFileWithWriterStrategyName);

        String sinkListName = "sinkListName";
        v1.addSinkList(sinkListName);
        sinks.add(sinkListName);

        String sinkListWithWriterStrategyName = "sinkListWithWriterStrategyName";
        v1.addSinkList(sinkListWithWriterStrategyName, APPEND);
        sinks.add(sinkListWithWriterStrategyName);

        String sinkMapName = "sinkMapName";
        v1.addSinkMap(sinkMapName);
        sinks.add(sinkMapName);

        String sinkMapWithWriterStrategyName = "sinkMapWithWriterStrategyName";
        v1.addSinkMap(sinkMapWithWriterStrategyName, APPEND);
        sinks.add(sinkMapWithWriterStrategyName);

        String sinkMultiMapName = "sinkMultiMapName";
        v1.addSinkMultiMap(sinkMultiMapName);
        sinks.add(sinkMultiMapName);

        String sinkMultiMapWithWriterStrategyName = "sinkMultiMapWithWriterStrategyName";
        v1.addSinkMultiMap(sinkMultiMapWithWriterStrategyName, APPEND);
        sinks.add(sinkMultiMapWithWriterStrategyName);


        final String sinkTapName = "sinkTapWithWriterStrategyName";
        SinkTap sinkTap = new SinkTap() {
            @Override
            public DataWriter[] getWriters(NodeEngine nodeEngine, ContainerDescriptor containerDescriptor) {
                return new DataWriter[0];
            }

            @Override
            public SinkTapWriteStrategy getTapStrategy() {
                return APPEND;
            }

            @Override
            public SinkOutputStream getSinkOutputStream() {
                return null;
            }

            @Override
            public String getName() {
                return sinkTapName;
            }

            @Override
            public TapType getType() {
                return null;
            }
        };
        v1.addSinkTap(sinkTap);
        sinks.add(sinkTapName);

        List<SinkTap> sinksFromVertex = v1.getSinks();
        assertEquals(9, sinksFromVertex.size());
        for (SinkTap sink : sinksFromVertex) {
            String name = sink.getName();
            SinkTapWriteStrategy tapStrategy = sink.getTapStrategy();
            assertTrue("Vertex should contain the sink : " + name, sinks.contains(name));
            if (name.contains("WithWriterStrategy")) {
                assertEquals(APPEND, tapStrategy);
            }
        }
    }
}
