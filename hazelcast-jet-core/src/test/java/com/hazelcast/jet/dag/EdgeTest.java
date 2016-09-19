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

package com.hazelcast.jet.dag;

import com.hazelcast.jet.Edge;
import com.hazelcast.jet.TestProcessors;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.strategy.SerializedHashingStrategy;
import com.hazelcast.jet.strategy.SinglePartitionDistributionStrategy;
import com.hazelcast.jet.strategy.RoutingStrategy;
import com.hazelcast.partition.strategy.StringAndPartitionAwarePartitioningStrategy;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.JetTestSupport.createVertex;
import static org.junit.Assert.assertEquals;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class EdgeTest {

    @Test
    public void testEdgeName() throws Exception {
        Vertex v1 = createVertex("v1", TestProcessors.Noop.class);
        Vertex v2 = createVertex("v2", TestProcessors.Noop.class);
        String name = "edge";
        Edge edge = new Edge(name, v1, v2);
        assertEquals(name, edge.getName());
    }

    @Test
    public void testEdgeInputOutputVertexes() throws Exception {
        Vertex v1 = createVertex("v1", TestProcessors.Noop.class);
        Vertex v2 = createVertex("v2", TestProcessors.Noop.class);
        Edge edge = new Edge("edge", v1, v2);
        assertEquals(v1, edge.getInputVertex());
        assertEquals(v2, edge.getOutputVertex());
    }

    @Test
    public void testDefaultStrategies() throws Exception {
        Vertex v1 = createVertex("v1", TestProcessors.Noop.class);
        Vertex v2 = createVertex("v2", TestProcessors.Noop.class);
        Edge edge = new Edge("edge", v1, v2);
        assertEquals(SerializedHashingStrategy.INSTANCE, edge.getHashingStrategy());
        assertEquals(StringAndPartitionAwarePartitioningStrategy.INSTANCE, edge.getPartitioningStrategy());
        assertEquals(RoutingStrategy.ROUND_ROBIN, edge.getRoutingStrategy());
    }


    @Test
    public void testEdgeBuilder() throws Exception {
        Vertex v1 = createVertex("v1", TestProcessors.Noop.class);
        Vertex v2 = createVertex("v2", TestProcessors.Noop.class);
        SinglePartitionDistributionStrategy shufflingStrategy = new SinglePartitionDistributionStrategy("test");
        Edge edge = new Edge("edge", v1, v2)
                .distributed(shufflingStrategy)
                .partitioned(StringAndPartitionAwarePartitioningStrategy.INSTANCE, SerializedHashingStrategy.INSTANCE)
                .broadcast();
        assertEquals(SerializedHashingStrategy.INSTANCE, edge.getHashingStrategy());
        assertEquals(StringAndPartitionAwarePartitioningStrategy.INSTANCE, edge.getPartitioningStrategy());
        assertEquals(RoutingStrategy.BROADCAST, edge.getRoutingStrategy());
        assertEquals(false, edge.isLocal());
        assertEquals(shufflingStrategy, edge.getMemberDistributionStrategy());
    }

}
