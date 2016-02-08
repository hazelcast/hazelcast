package com.hazelcast.jet;

import com.hazelcast.jet.impl.dag.DAGImpl;
import com.hazelcast.jet.impl.dag.EdgeImpl;
import com.hazelcast.jet.impl.dag.VertexImpl;
import com.hazelcast.jet.processors.DummyProcessor;
import com.hazelcast.jet.spi.processor.ProcessorDescriptor;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;


@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class DagTest {

    @Test(expected = IllegalStateException.class)
    public void test_add_same_vertex_multiple_times_throws_exception() throws Exception {
        DAGImpl dag = new DAGImpl();
        VertexImpl vertex = new VertexImpl("v1", getDummyProcessor());
        dag.addVertex(vertex);
        dag.addVertex(vertex);
    }

    @Test
    public void test_dag_should_contains_vertex() throws Exception {
        DAGImpl dag = new DAGImpl();
        VertexImpl vertex = new VertexImpl("v1", getDummyProcessor());
        dag.addVertex(vertex);
        assertTrue("DAG should contain the added vertex", dag.containsVertex(vertex));
    }

    @Test
    public void test_empty_dag_should_not_contain_vertex() throws Exception {
        DAGImpl dag = new DAGImpl();
        VertexImpl vertex = new VertexImpl("v1", getDummyProcessor());
        assertFalse("DAG should not contain any vertex", dag.containsVertex(vertex));
    }

    @Test(expected = IllegalStateException.class)
    public void test_validate_empty_dag_throws_exception() throws Exception {
        DAGImpl dag = new DAGImpl();
        dag.validate();
    }

    @Test(expected = IllegalStateException.class)
    public void test_validate_cyclic_graph_throws_exception() throws Exception {
        DAGImpl dag = new DAGImpl();
        VertexImpl v1 = new VertexImpl("v1", getDummyProcessor());
        VertexImpl v2 = new VertexImpl("v2", getDummyProcessor());
        dag.addVertex(v1);
        dag.addVertex(v2);
        dag.addEdge(new EdgeImpl("e1", v1, v2));
        dag.addEdge(new EdgeImpl("e2", v2, v1));
        dag.validate();
    }

    @Test(expected = IllegalStateException.class)
    public void test_validate_self_cycle_on_vertex_throws_exception() throws Exception {
        DAGImpl dag = new DAGImpl();
        VertexImpl vertex = new VertexImpl("v1", getDummyProcessor());
        dag.addVertex(vertex);
        dag.addEdge(new EdgeImpl("e1", vertex, vertex));
        dag.validate();
    }

    @Test(expected = IllegalStateException.class)
    public void test_validate_same_output_and_vertex_name_throws_exception() throws Exception {
        DAGImpl dag = new DAGImpl();
        VertexImpl vertex = new VertexImpl("v1", getDummyProcessor());
        VertexImpl output = new VertexImpl("output", getDummyProcessor());
        vertex.addOutputVertex(output, new EdgeImpl("e1", vertex, output));
        vertex.addSinkFile("output");
        dag.addVertex(vertex);
        dag.addVertex(output);
        dag.validate();
    }

    @Test(expected = IllegalStateException.class)
    public void test_validate_same_input_and_vertex_name_throws_exception() throws Exception {
        DAGImpl dag = new DAGImpl();
        VertexImpl vertex = new VertexImpl("v1", getDummyProcessor());
        VertexImpl input = new VertexImpl("input", getDummyProcessor());
        vertex.addInputVertex(input, new EdgeImpl("e1", input, vertex));
        vertex.addSourceFile("input");
        dag.addVertex(vertex);
        dag.addVertex(input);
        dag.validate();
    }


    @Test(expected = IllegalStateException.class)
    public void test_validate_same_source_and_vertex_name_throws_exception() throws Exception {
        DAGImpl dag = new DAGImpl();
        VertexImpl vertex = new VertexImpl("v1", getDummyProcessor());
        vertex.addSourceFile("v1");
        dag.addVertex(vertex);
        dag.validate();
    }

    @Test(expected = IllegalStateException.class)
    public void test_validate_same_sink_and_vertex_name_throws_exception() throws Exception {
        DAGImpl dag = new DAGImpl();
        VertexImpl vertex = new VertexImpl("v1", getDummyProcessor());
        vertex.addSinkFile("v1");
        dag.addVertex(vertex);
        dag.validate();
    }

    private ProcessorDescriptor getDummyProcessor() {
        return ProcessorDescriptor.
                builder(DummyProcessor.Factory.class).
                build();
    }

}
