/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.Cluster;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.TestProcessors.MockPMS;
import com.hazelcast.jet.core.TestProcessors.MockPS;
import com.hazelcast.jet.core.TestProcessors.NoOutputSourceP;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static com.hazelcast.cluster.ClusterState.ACTIVE;
import static com.hazelcast.cluster.ClusterState.PASSIVE;
import static com.hazelcast.jet.core.JobStatus.NOT_RUNNING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClusterStateChangeTest extends JetTestSupport {

    private static final int NODE_COUNT = 3;
    private static final int LOCAL_PARALLELISM = 4;
    private static final int TOTAL_PARALLELISM = NODE_COUNT * LOCAL_PARALLELISM;

    @Rule
    public final ExpectedException thrown = ExpectedException.none();

    private HazelcastInstance[] members;
    private HazelcastInstance hz;
    private Cluster cluster;
    private DAG dag;

    @Before
    public void before() {
        TestProcessors.reset(TOTAL_PARALLELISM);
        Config config = smallInstanceConfig();
        config.getJetConfig().setCooperativeThreadCount(LOCAL_PARALLELISM);
        members = createHazelcastInstances(config, NODE_COUNT);

        assertTrueEventually(() -> {
            for (HazelcastInstance instance : members) {
                assertClusterSizeEventually(NODE_COUNT, instance);
            }
        });

        for (HazelcastInstance member : members) {
            if (!getNodeEngineImpl(member).getClusterService().isMaster()) {
                hz = member;
                break;
            }
        }

        cluster = hz.getCluster();
        dag = new DAG().vertex(new Vertex("test",
                new MockPMS(() -> new MockPS(NoOutputSourceP::new, NODE_COUNT))));
    }

    @Test
    public void when_clusterPassive_then_jobSubmissionFails() {
        cluster.changeClusterState(PASSIVE);
        assertEquals("Cluster state", PASSIVE, cluster.getClusterState());

        thrown.expect(IllegalStateException.class);
        hz.getJet().newJob(dag);
    }

    @Test
    public void when_enterPassiveState_then_executionTerminated() throws Exception {
        // Given
        Job job = hz.getJet().newJob(dag);
        NoOutputSourceP.executionStarted.await();

        // When
        cluster.changeClusterState(PASSIVE);

        // Then
        assertEquals("Cluster state", PASSIVE, cluster.getClusterState());
        assertTrue("ProcessorMetaSupplier should get closed", MockPMS.closeCalled.get());
        assertEquals("ProcessorSupplier should get closed on each member", NODE_COUNT, MockPS.closeCount.get());
        assertEquals("Job status", NOT_RUNNING, job.getStatus());

        // Change back to active, so we can stop jobs and check for leaking resources
        cluster.changeClusterState(ACTIVE);
    }

    @Test
    public void when_goPassiveAndBack_then_jobResumes() throws Exception {
        // Given
        hz.getJet().newJob(dag);
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
