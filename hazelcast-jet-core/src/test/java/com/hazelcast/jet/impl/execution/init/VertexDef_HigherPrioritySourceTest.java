/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.execution.init;

import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.cluster.impl.MembersView;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetTestInstanceFactory;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.impl.MasterJobContext;
import com.hazelcast.jet.impl.execution.SnapshotContext;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngineImpl;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Map;

import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.Edge.from;
import static com.hazelcast.jet.impl.execution.init.ExecutionPlanBuilder.createExecutionPlans;
import static com.hazelcast.test.HazelcastTestSupport.getNodeEngineImpl;
import static java.util.Collections.nCopies;
import static java.util.stream.Collectors.joining;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class VertexDef_HigherPrioritySourceTest {

    private static final ProcessorMetaSupplier MOCK_PMS =
            addresses -> address -> count -> nCopies(count, new DummyProcessor());
    private static final JetTestInstanceFactory factory = new JetTestInstanceFactory();
    private static NodeEngineImpl nodeEngineImpl;
    private static MembersView membersView;

    private DAG dag = new DAG();
    private Vertex v1 = dag.newVertex("v1", MOCK_PMS);
    private Vertex v2 = dag.newVertex("v2", MOCK_PMS);
    private Vertex v3 = dag.newVertex("v3", MOCK_PMS);
    private Vertex v4 = dag.newVertex("v4", MOCK_PMS);
    private Vertex v5 = dag.newVertex("v5", MOCK_PMS);

    @BeforeClass
    public static void beforeClass() {
        JetInstance inst = factory.newMember();
        nodeEngineImpl = getNodeEngineImpl(inst.getHazelcastInstance());
        ClusterServiceImpl clusterService = (ClusterServiceImpl) nodeEngineImpl.getClusterService();
        membersView = clusterService.getMembershipManager().getMembersView();
    }

    @AfterClass
    public static void afterClass() {
        factory.shutdownAll();
    }

    @Test
    public void test_simple() {
        dag.edge(from(v1, 0).to(v3, 0).priority(-1))
           .edge(from(v2, 0).to(v3, 1));

        assertHigherPriorityVertices(v1);
    }

    @Test
    public void test_nested() {
        dag.edge(between(v1, v2))
           .edge(from(v2, 0).to(v4, 0).priority(-1))
           .edge(from(v3, 0).to(v4, 1));

        assertHigherPriorityVertices(v1, v2);
    }

    @Test
    public void test_nestedMore() {
        dag.edge(from(v1, 0).to(v3, 0))
           .edge(from(v2, 0).to(v3, 1))
           .edge(from(v3, 0).to(v5, 0).priority(-1))
           .edge(from(v4, 0).to(v5, 1));

        assertHigherPriorityVertices(v1, v2, v3);
    }

    @Test
    public void test_noHigherPrioritySource() {
        assertHigherPriorityVertices();
    }

    @Test
    public void test_noHigherPrioritySource_2() {
        dag.edge(between(v1, v2));
        assertHigherPriorityVertices();
    }

    @Test
    public void test_snapshotRestoreEdge() throws Exception {
        Edge edge = between(v1, v2);
        forceSnapshotPriority(edge);
        dag.edge(edge);
        assertHigherPriorityVertices(v1);
    }

    private void forceSnapshotPriority(Edge edge) throws Exception {
        Field field = Edge.class.getDeclaredField("priority");
        field.setAccessible(true);
        field.set(edge, MasterJobContext.SNAPSHOT_RESTORE_EDGE_PRIORITY);
    }

    private void assertHigherPriorityVertices(Vertex... vertices) {
        Map<MemberInfo, ExecutionPlan> executionPlans =
                createExecutionPlans(nodeEngineImpl, membersView, dag, 0, 0, new JobConfig(), 0);
        ExecutionPlan plan = executionPlans.values().iterator().next();
        plan.initialize(nodeEngineImpl, 0, 0, new SnapshotContext(mock(ILogger.class), "job", 0, EXACTLY_ONCE));
        String actualHigherPriorityVertices = plan.getVertices().stream()
                .filter(VertexDef::isHigherPrioritySource)
                .map(VertexDef::name)
                .sorted()
                .collect(joining("\n"));
        String expectedVertices = Arrays.stream(vertices).map(Vertex::getName).sorted().collect(joining("\n"));
        assertEquals(expectedVertices, actualHigherPriorityVertices);
    }

    private static class DummyProcessor implements Processor {
    }
}
