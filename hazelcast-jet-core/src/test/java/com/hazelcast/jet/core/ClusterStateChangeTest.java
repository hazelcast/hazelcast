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

package com.hazelcast.jet.core;

import com.hazelcast.core.Cluster;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.TestProcessors.MockPMS;
import com.hazelcast.jet.core.TestProcessors.MockPS;
import com.hazelcast.jet.core.TestProcessors.NoOutputSourceP;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static com.hazelcast.cluster.ClusterState.ACTIVE;
import static com.hazelcast.cluster.ClusterState.PASSIVE;
import static com.hazelcast.jet.core.JobStatus.NOT_RUNNING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
public class ClusterStateChangeTest extends JetTestSupport {

    private static final int NODE_COUNT = 3;
    private static final int LOCAL_PARALLELISM = 4;
    private static final int TOTAL_PARALLELISM = NODE_COUNT * LOCAL_PARALLELISM;

    private JetInstance[] members;
    private JetInstance jet;
    private Cluster cluster;
    private DAG dag;

    @Before
    public void before() {
        TestProcessors.reset(TOTAL_PARALLELISM);
        JetConfig config = new JetConfig();
        config.getInstanceConfig().setCooperativeThreadCount(LOCAL_PARALLELISM);
        members = createJetMembers(config, NODE_COUNT);
        for (JetInstance member : members) {
            if (!getNodeEngineImpl(member).getClusterService().isMaster()) {
                jet = member;
                break;
            }
        }
        cluster = jet.getCluster();
        dag = new DAG().vertex(new Vertex("test",
                new MockPMS(() -> new MockPS(NoOutputSourceP::new, NODE_COUNT))));
    }

    @Test(expected = IllegalStateException.class)
    public void when_clusterPassive_then_jobSubmissionFails() {
        cluster.changeClusterState(PASSIVE);
        assertEquals("Cluster state", PASSIVE, cluster.getClusterState());
        jet.newJob(dag);
    }

    @Test
    public void when_enterPassiveState_then_executionTerminated() throws Exception {
        // Given
        Job job = jet.newJob(dag);
        NoOutputSourceP.executionStarted.await();

        // When
        cluster.changeClusterState(PASSIVE);

        // Then
        assertEquals("Cluster state", PASSIVE, cluster.getClusterState());
        assertTrue("ProcessorMetaSupplier should get closed", MockPMS.closeCalled.get());
        assertEquals("ProcessorSupplier should get closed on each member", NODE_COUNT, MockPS.closeCount.get());
        assertEquals("Job status", NOT_RUNNING, job.getStatus());
    }

    @Test
    public void when_goPassiveAndBack_then_jobResumes() throws Exception {
        // Given
        jet.newJob(dag);
        NoOutputSourceP.executionStarted.await();

        // When
        cluster.changeClusterState(PASSIVE);
        assertEquals("Cluster state", PASSIVE, cluster.getClusterState());
        TestProcessors.reset(TOTAL_PARALLELISM);
        cluster.changeClusterState(ACTIVE);

        // Then
        assertEquals("Cluster state", ACTIVE, cluster.getClusterState());
        NoOutputSourceP.executionStarted.await();
    }
}
