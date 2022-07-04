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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.collection.IList;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.impl.JetServiceBackend;
import com.hazelcast.jet.impl.JobExecutionService;
import com.hazelcast.jet.impl.execution.ExecutionContext;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.SimpleEvent;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HazelcastConnector_RestartTest extends JetTestSupport {

    private HazelcastInstance instance1;
    private HazelcastInstance instance2;

    @Before
    public void setup() {
        instance1 = createHazelcastInstance();
        instance2 = createHazelcastInstance();
    }

    @Test
    public void when_iListWrittenAndMemberShutdown_then_jobRestarts() {
        IList<SimpleEvent> sinkList = instance1.getList("list");

        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.itemStream(10))
                .withoutTimestamps()
                .writeTo(Sinks.list(sinkList));

        Job job = instance1.getJet().newJob(p);
        assertTrueEventually(() -> assertTrue("no output to sink", sinkList.size() > 0), 10);

        Long executionId = executionId(instance1, job);
        if (executionId == null) {
            executionId = executionId(instance2, job);
        }
        instance2.shutdown();

        // Then - assert that the job stopped producing output
        waitExecutionDoneOnMember(instance1, executionId);
        int sizeAfterShutdown = sinkList.size();

        // Then2 - job restarts and continues production
        assertTrueEventually(() ->
                assertTrue("no output after migration completed", sinkList.size() > sizeAfterShutdown), 20);
    }

    private Long executionId(HazelcastInstance instance, Job job) {
        JetServiceBackend jetServiceBackend = getJetServiceBackend(instance);
        JobExecutionService executionService = jetServiceBackend.getJobExecutionService();
        return executionService.getExecutionIdForJobId(job.getId());
    }

    private void waitExecutionDoneOnMember(HazelcastInstance instance, long executionId) {
        JetServiceBackend jetServiceBackend = getJetServiceBackend(instance);
        JobExecutionService executionService = jetServiceBackend.getJobExecutionService();
        ExecutionContext execCtx = executionService.getExecutionContext(executionId);
        assertTrueEventually(() -> assertTrue(execCtx == null || execCtx.getExecutionFuture().isDone()));
    }
}
