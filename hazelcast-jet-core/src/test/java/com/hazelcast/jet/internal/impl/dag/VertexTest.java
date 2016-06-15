package com.hazelcast.jet.internal.impl.dag;

import com.hazelcast.jet.container.ContainerDescriptor;
import com.hazelcast.jet.dag.Edge;
import com.hazelcast.jet.dag.Vertex;
import com.hazelcast.jet.dag.tap.SinkTap;
import com.hazelcast.jet.dag.tap.SourceTap;
import com.hazelcast.jet.data.DataReader;
import com.hazelcast.jet.data.DataWriter;
import com.hazelcast.jet.data.tuple.JetTupleFactory;
import com.hazelcast.jet.processors.DummyProcessor;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

import static com.hazelcast.jet.base.JetBaseTest.createVertex;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class VertexTest {

    @Test
    public void testVertexNameAndProcessorFactory() throws Exception {
        String name = "v1";
        Class<DummyProcessor> procesorClass = DummyProcessor.class;
        Vertex v1 = createVertex(name, procesorClass);
        assertEquals(name, v1.getName());
        assertEquals(procesorClass.getName(), v1.getDescriptor().getContainerProcessorClazz());
    }

    @Test
    public void testVertexInput() throws Exception {
        Vertex v1 = createVertex("v1", DummyProcessor.class);
        Vertex input = createVertex("input", DummyProcessor.class);

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
        Vertex v1 = createVertex("v1", DummyProcessor.class);
        Vertex output = createVertex("output", DummyProcessor.class);

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
        Vertex v1 = createVertex("v1", DummyProcessor.class);
        Vertex output = createVertex("output", DummyProcessor.class);

        Edge edge = new Edge.EdgeBuilder("edge", v1, output)
                .shuffling(true)
                .build();
        v1.addOutputVertex(output, edge);

        assertTrue(v1.hasOutputShuffler());
    }

    @Test
    public void testEmptyVertexHasNotOutputShuffler() throws Exception {
        Vertex v1 = createVertex("v1", DummyProcessor.class);
        assertFalse(v1.hasOutputShuffler());
    }

    @Test
    public void testVertexSources() throws Exception {
        Vertex vertex = createVertex("vertex", DummyProcessor.class);

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
        };
        vertex.addSource(sourceTap);

        assertEquals(1, vertex.getSources().size());
        assertEquals(sourceTap, vertex.getSources().get(0));
    }

    @Test
    public void testVertexSinks() throws Exception {
        Vertex vertex = createVertex("vertex", DummyProcessor.class);

        final String sinkTapName = "sinkTapWithWriterStrategyName";
        SinkTap sinkTap = new SinkTap() {
            @Override
            public DataWriter[] getWriters(NodeEngine nodeEngine, ContainerDescriptor containerDescriptor) {
                return new DataWriter[0];
            }

            @Override
            public boolean isPartitioned() {
                return false;
            }

            @Override
            public String getName() {
                return sinkTapName;
            }
        };
        vertex.addSink(sinkTap);

        assertEquals(1, vertex.getSinks().size());
        assertEquals(sinkTap, vertex.getSinks().get(0));
    }
}
