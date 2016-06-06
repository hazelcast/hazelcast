package com.hazelcast.jet.internal.impl.dag;

import com.hazelcast.jet.dag.Edge;
import com.hazelcast.jet.impl.actor.ByReferenceDataTransferringStrategy;
import com.hazelcast.jet.impl.strategy.DefaultHashingStrategy;
import com.hazelcast.jet.strategy.IListBasedShufflingStrategy;
import com.hazelcast.jet.processors.DummyProcessor;
import com.hazelcast.jet.dag.Vertex;
import com.hazelcast.jet.strategy.ProcessingStrategy;
import com.hazelcast.partition.strategy.StringAndPartitionAwarePartitioningStrategy;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.base.JetBaseTest.createVertex;
import static org.junit.Assert.assertEquals;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class EdgeTest {

    @Test
    public void testEdgeName() throws Exception {
        Vertex v1 = createVertex("v1", DummyProcessor.Factory.class);
        Vertex v2 = createVertex("v2", DummyProcessor.Factory.class);
        String name = "edge";
        Edge edge = new Edge(name, v1, v2);
        assertEquals(name, edge.getName());
    }

    @Test
    public void testEdgeInputOutputVertexes() throws Exception {
        Vertex v1 = createVertex("v1", DummyProcessor.Factory.class);
        Vertex v2 = createVertex("v2", DummyProcessor.Factory.class);
        Edge edge = new Edge("edge", v1, v2);
        assertEquals(v1, edge.getInputVertex());
        assertEquals(v2, edge.getOutputVertex());
    }

    @Test
    public void testDefaultStrategies() throws Exception {
        Vertex v1 = createVertex("v1", DummyProcessor.Factory.class);
        Vertex v2 = createVertex("v2", DummyProcessor.Factory.class);
        Edge edge = new Edge("edge", v1, v2);
        assertEquals(ByReferenceDataTransferringStrategy.INSTANCE, edge.getDataTransferringStrategy());
        assertEquals(DefaultHashingStrategy.INSTANCE, edge.getHashingStrategy());
        assertEquals(StringPartitioningStrategy.INSTANCE, edge.getPartitioningStrategy());
        assertEquals(ProcessingStrategy.ROUND_ROBIN, edge.getProcessingStrategy());
    }


    @Test
    public void testEdgeBuilder() throws Exception {
        Vertex v1 = createVertex("v1", DummyProcessor.Factory.class);
        Vertex v2 = createVertex("v2", DummyProcessor.Factory.class);
        IListBasedShufflingStrategy shufflingStrategy = new IListBasedShufflingStrategy("test");
        Edge edge = new Edge.EdgeBuilder("edge", v1, v2)
                .dataTransferringStrategy(ByReferenceDataTransferringStrategy.INSTANCE)
                .hashingStrategy(DefaultHashingStrategy.INSTANCE)
                .partitioningStrategy(StringAndPartitionAwarePartitioningStrategy.INSTANCE)
                .processingStrategy(ProcessingStrategy.BROADCAST)
                .shuffling(true)
                .shufflingStrategy(shufflingStrategy)
                .build();
        assertEquals(ByReferenceDataTransferringStrategy.INSTANCE, edge.getDataTransferringStrategy());
        assertEquals(DefaultHashingStrategy.INSTANCE, edge.getHashingStrategy());
        assertEquals(StringAndPartitionAwarePartitioningStrategy.INSTANCE, edge.getPartitioningStrategy());
        assertEquals(ProcessingStrategy.BROADCAST, edge.getProcessingStrategy());
        assertEquals(true, edge.isShuffled());
        assertEquals(shufflingStrategy, edge.getShufflingStrategy());
    }

    @Test(expected = IllegalStateException.class)
    public void testEdgeBuilder_multipleCallToBuild_throwsException() throws Exception {
        Vertex v1 = createVertex("v1", DummyProcessor.Factory.class);
        Vertex v2 = createVertex("v2", DummyProcessor.Factory.class);
        Edge.EdgeBuilder edgeBuilder = new Edge.EdgeBuilder("edge", v1, v2);
        edgeBuilder.build();
        edgeBuilder.build();
    }

}
