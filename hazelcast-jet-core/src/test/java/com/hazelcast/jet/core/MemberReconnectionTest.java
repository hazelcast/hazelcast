/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.TestProcessors.MockP;
import com.hazelcast.jet.impl.util.ImdgUtil;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastSerialClassRunner.class)
public class MemberReconnectionTest extends JetTestSupport {

    @Test
    public void when_connectionDropped_then_detectedInReceiverTaskletAndFails() {
        // we use real-network instances, closing the mock connection doesn't cause them to reconnect
        JetConfig config = new JetConfig();
        config.getHazelcastConfig().setClusterName(randomName());

        JetInstance inst1 = Jet.newJetInstance(config);
        JetInstance inst2 = Jet.newJetInstance(config);

        try {
            DAG dag = new DAG();
            Vertex v1 = dag.newVertex("v1", () -> new MockP().streaming());
            Vertex v2 = dag.newVertex("v2", () -> new MockP());
            dag.edge(between(v1, v2).distributed());

            Job job = inst1.newJob(dag);
            assertJobStatusEventually(job, RUNNING);

            // Close the connection. Nothing is sent through the SenderTasklet, therefore we won't detect
            // it there. We rely on detecting it in ReceiverTasklet, we assert that it was detected there.
            ImdgUtil.getMemberConnection(getNodeEngineImpl(inst1), getNodeEngineImpl(inst2).getThisAddress())
                    .close("mock close", new Exception("mock close"));

            logger.info("joining...");
            assertThatThrownBy(() -> job.join())
                    .hasMessageContaining("The member was reconnected")
                    .hasMessageContaining("Exception in ReceiverTasklet");
        } finally {
            Jet.shutdownAll();
        }
    }
}
