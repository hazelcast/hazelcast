package com.hazelcast.jet.internal.impl.dag;

import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.jet.dag.DAGImpl;
import com.hazelcast.jet.dag.EdgeImpl;
import com.hazelcast.jet.impl.actor.ByReferenceDataTransferringStrategy;
import com.hazelcast.jet.impl.strategy.DefaultHashingStrategy;
import com.hazelcast.jet.strategy.IListBasedShufflingStrategy;
import com.hazelcast.jet.processors.DummyProcessor;
import com.hazelcast.jet.dag.Edge;
import com.hazelcast.jet.dag.Vertex;
import com.hazelcast.jet.strategy.ProcessingStrategy;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.strategy.StringAndPartitionAwarePartitioningStrategy;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import static com.hazelcast.jet.base.JetBaseTest.createVertex;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;


@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class DagImplTest {

    @Test(expected = IllegalArgumentException.class)
    public void test_add_same_vertex_multiple_times_throws_exception() throws Exception {
        DAGImpl dag = new DAGImpl();
        Vertex v1 = createVertex("v1", DummyProcessor.Factory.class);
        dag.addVertex(v1);
        dag.addVertex(v1);
    }

    @Test
    public void test_dag_should_contains_vertex() throws Exception {
        DAGImpl dag = new DAGImpl();
        Vertex v1 = createVertex("v1", DummyProcessor.Factory.class);
        dag.addVertex(v1);
        assertTrue("DAG should contain the added vertex", dag.containsVertex(v1));
    }

    @Test
    public void test_empty_dag_should_not_contain_vertex() throws Exception {
        DAGImpl dag = new DAGImpl();
        Vertex v1 = createVertex("v1", DummyProcessor.Factory.class);
        assertFalse("DAG should not contain any vertex", dag.containsVertex(v1));
    }

    @Test(expected = IllegalStateException.class)
    public void test_validate_empty_dag_throws_exception() throws Exception {
        DAGImpl dag = new DAGImpl();
        dag.validate();
    }

    @Test(expected = IllegalStateException.class)
    public void test_validate_cyclic_graph_throws_exception() throws Exception {
        DAGImpl dag = new DAGImpl();
        Vertex v1 = createVertex("v1", DummyProcessor.Factory.class);
        Vertex v2 = createVertex("v2", DummyProcessor.Factory.class);
        dag.addVertex(v1);
        dag.addVertex(v2);
        dag.addEdge(new EdgeImpl("e1", v1, v2));
        dag.addEdge(new EdgeImpl("e2", v2, v1));
        dag.validate();
    }

    @Test(expected = IllegalStateException.class)
    public void test_validate_self_cycle_on_vertex_throws_exception() throws Exception {
        DAGImpl dag = new DAGImpl();
        Vertex vertex = createVertex("v1", DummyProcessor.Factory.class);
        dag.addVertex(vertex);
        dag.addEdge(new EdgeImpl("e1", vertex, vertex));
        dag.validate();
    }

    @Test(expected = IllegalStateException.class)
    public void test_validate_same_output_and_vertex_name_throws_exception() throws Exception {
        DAGImpl dag = new DAGImpl();
        Vertex vertex = createVertex("v1", DummyProcessor.Factory.class);
        Vertex output = createVertex("output", DummyProcessor.Factory.class);
        vertex.addOutputVertex(output, new EdgeImpl("e1", vertex, output));
        vertex.addSinkFile("output");
        dag.addVertex(vertex);
        dag.addVertex(output);
        dag.validate();
    }

    @Test(expected = IllegalStateException.class)
    public void test_validate_same_input_and_vertex_name_throws_exception() throws Exception {
        DAGImpl dag = new DAGImpl();
        Vertex vertex = createVertex("v1", DummyProcessor.Factory.class);
        Vertex input = createVertex("input", DummyProcessor.Factory.class);
        vertex.addInputVertex(input, new EdgeImpl("e1", input, vertex));
        vertex.addSourceFile("input");
        dag.addVertex(vertex);
        dag.addVertex(input);
        dag.validate();
    }


    @Test(expected = IllegalStateException.class)
    public void test_validate_same_source_and_vertex_name_throws_exception() throws Exception {
        DAGImpl dag = new DAGImpl();
        Vertex vertex = createVertex("v1", DummyProcessor.Factory.class);
        vertex.addSourceFile("v1");
        dag.addVertex(vertex);
        dag.validate();
    }

    @Test(expected = IllegalStateException.class)
    public void test_validate_same_sink_and_vertex_name_throws_exception() throws Exception {
        DAGImpl dag = new DAGImpl();
        Vertex vertex = createVertex("v1", DummyProcessor.Factory.class);
        vertex.addSinkFile("v1");
        dag.addVertex(vertex);
        dag.validate();
    }


    @Test(expected = IllegalStateException.class)
    public void testGetTopologicalVertexIterator_throwsException_whenRemoveIsCalled() throws Exception {
        DAGImpl dag = new DAGImpl();
        Vertex v1 = createVertex("v1", DummyProcessor.Factory.class);
        dag.addVertex(v1);
        dag.validate();
        Iterator<Vertex> iterator = dag.getTopologicalVertexIterator();
        iterator.remove();
    }

    @Test(expected = IllegalStateException.class)
    public void testGetTopologicalVertexIterator_throwsException_whenDagIsNotValidated() throws Exception {
        DAGImpl dag = new DAGImpl();
        dag.getTopologicalVertexIterator();
    }


    @Test
    public void testGetTopologicalVertexIterator() throws Exception {
        DAGImpl dag = new DAGImpl();
        Vertex v1 = createVertex("v1", DummyProcessor.Factory.class);
        Vertex v2 = createVertex("v2", DummyProcessor.Factory.class);
        Vertex v3 = createVertex("v3", DummyProcessor.Factory.class);

        dag.addVertex(v1);
        dag.addVertex(v2);
        dag.addVertex(v3);

        EdgeImpl e1 = new EdgeImpl("e1", v1, v2);
        EdgeImpl e2 = new EdgeImpl("e2", v2, v3);

        dag.addEdge(e1);
        dag.addEdge(e2);

        dag.validate();

        ArrayList<Vertex> vertices = new ArrayList<Vertex>();
        vertices.add(v1);
        vertices.add(v2);
        vertices.add(v3);

        ArrayList<Vertex> verticesFromIterator = new ArrayList<Vertex>();
        Iterator<Vertex> iterator = dag.getTopologicalVertexIterator();
        while (iterator.hasNext()) {
            Vertex next = iterator.next();
            verticesFromIterator.add(next);
        }

        assertEquals(vertices, verticesFromIterator);
    }

    @Test(expected = IllegalStateException.class)
    public void testGetRevertedTopologicalVertexIterator_throwsException_whenDagIsNotValidated() throws Exception {
        DAGImpl dag = new DAGImpl();
        dag.getRevertedTopologicalVertexIterator();
    }


    @Test(expected = IllegalStateException.class)
    public void testGetRevertedTopologicalVertexIterator_throwsException_whenRemoveIsCalled() throws Exception {
        DAGImpl dag = new DAGImpl();
        Vertex v1 = createVertex("v1", DummyProcessor.Factory.class);
        dag.addVertex(v1);
        dag.validate();
        Iterator<Vertex> iterator = dag.getRevertedTopologicalVertexIterator();
        iterator.remove();
    }

    @Test
    public void testGetRevertedTopologicalVertexIterator() throws Exception {
        DAGImpl dag = new DAGImpl();
        Vertex v1 = createVertex("v1", DummyProcessor.Factory.class);
        Vertex v2 = createVertex("v2", DummyProcessor.Factory.class);
        Vertex v3 = createVertex("v3", DummyProcessor.Factory.class);

        dag.addVertex(v1);
        dag.addVertex(v2);
        dag.addVertex(v3);

        EdgeImpl e1 = new EdgeImpl("e1", v1, v2);
        EdgeImpl e2 = new EdgeImpl("e2", v2, v3);

        dag.addEdge(e1);
        dag.addEdge(e2);

        dag.validate();

        ArrayList<Vertex> vertices = new ArrayList<Vertex>();
        vertices.add(v3);
        vertices.add(v2);
        vertices.add(v1);

        ArrayList<Vertex> verticesFromIterator = new ArrayList<Vertex>();
        Iterator<Vertex> iterator = dag.getRevertedTopologicalVertexIterator();
        while (iterator.hasNext()) {
            Vertex next = iterator.next();
            verticesFromIterator.add(next);
        }

        assertEquals(vertices, verticesFromIterator);
    }

    @Test
    public void testDAG_Serialization_Deserialization() throws Exception {
        DAGImpl dag = new DAGImpl("dag");
        Vertex v1 = createVertex("v1", DummyProcessor.Factory.class);
        Vertex v2 = createVertex("v2", DummyProcessor.Factory.class);
        Vertex v3 = createVertex("v3", DummyProcessor.Factory.class);

        Edge e1 = new EdgeImpl.EdgeBuilder("e1", v1, v2)
                .dataTransferringStrategy(ByReferenceDataTransferringStrategy.INSTANCE)
                .hashingStrategy(DefaultHashingStrategy.INSTANCE)
                .partitioningStrategy(StringAndPartitionAwarePartitioningStrategy.INSTANCE)
                .processingStrategy(ProcessingStrategy.BROADCAST)
                .shufflingStrategy(new IListBasedShufflingStrategy("e1"))
                .build();
        Edge e2 = new EdgeImpl.EdgeBuilder("e2", v2, v3)
                .dataTransferringStrategy(ByReferenceDataTransferringStrategy.INSTANCE)
                .hashingStrategy(DefaultHashingStrategy.INSTANCE)
                .partitioningStrategy(StringPartitioningStrategy.INSTANCE)
                .processingStrategy(ProcessingStrategy.PARTITIONING)
                .shufflingStrategy(new IListBasedShufflingStrategy("e2"))
                .build();

        dag.addVertex(v1);
        dag.addVertex(v2);
        dag.addVertex(v3);

        dag.addEdge(e1);
        dag.addEdge(e2);

        DefaultSerializationServiceBuilder builder = new DefaultSerializationServiceBuilder();
        SerializationService serializationService = builder.build();
        Data data = serializationService.toData(dag);
        DAGImpl deSerializedDag = serializationService.toObject(data);

        assertEquals("dag", deSerializedDag.getName());
        assertEquals(v1, deSerializedDag.getVertex("v1"));
        assertEquals(v2, deSerializedDag.getVertex("v2"));
        assertEquals(v3, deSerializedDag.getVertex("v3"));
        assertTrue(deSerializedDag.containsEdge(e1));
        assertTrue(deSerializedDag.containsEdge(e2));

    }

    @Test
    public void testDAG_GetVertices() throws Exception {
        DAGImpl dag = new DAGImpl("dag");
        Vertex v1 = createVertex("v1", DummyProcessor.Factory.class);
        Vertex v2 = createVertex("v2", DummyProcessor.Factory.class);
        Vertex v3 = createVertex("v3", DummyProcessor.Factory.class);

        dag.addVertex(v1);
        dag.addVertex(v2);
        dag.addVertex(v3);

        Collection<Vertex> vertices = dag.getVertices();
        assertEquals(3, vertices.size());
        assertTrue(vertices.contains(v1));
        assertTrue(vertices.contains(v2));
        assertTrue(vertices.contains(v3));

    }

    @Test(expected = IllegalArgumentException.class)
    public void testDAG_addEdgeMultipleTimes_throwsException() throws Exception {
        DAGImpl dag = new DAGImpl("dag");
        Vertex v1 = createVertex("v1", DummyProcessor.Factory.class);
        Vertex v2 = createVertex("v2", DummyProcessor.Factory.class);
        EdgeImpl e1 = new EdgeImpl("e1", v1, v2);
        dag.addVertex(v1);
        dag.addVertex(v2);

        dag.addEdge(e1);
        dag.addEdge(e1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDAG_addEdge_withoutCorrespondingInput_throwsException() throws Exception {
        DAGImpl dag = new DAGImpl("dag");
        Vertex v1 = createVertex("v1", DummyProcessor.Factory.class);
        Vertex v2 = createVertex("v2", DummyProcessor.Factory.class);
        EdgeImpl e1 = new EdgeImpl("e1", v1, v2);
        dag.addVertex(v2);

        dag.addEdge(e1);
        dag.addEdge(e1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDAG_addEdge_withoutCorrespondingOutput_throwsException() throws Exception {
        DAGImpl dag = new DAGImpl("dag");
        Vertex v1 = createVertex("v1", DummyProcessor.Factory.class);
        Vertex v2 = createVertex("v2", DummyProcessor.Factory.class);
        EdgeImpl e1 = new EdgeImpl("e1", v1, v2);
        dag.addVertex(v1);
        dag.addEdge(e1);
    }

}
