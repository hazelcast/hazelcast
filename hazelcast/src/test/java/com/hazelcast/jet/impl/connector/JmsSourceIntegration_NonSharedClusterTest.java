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

import com.hazelcast.client.map.helpers.AMapStore;
import com.hazelcast.collection.IList;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.junit.EmbeddedActiveMQResource;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.jms.ConnectionFactory;
import javax.jms.TextMessage;
import java.io.Serializable;

import static com.hazelcast.jet.config.ProcessingGuarantee.AT_LEAST_ONCE;
import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.config.ProcessingGuarantee.NONE;
import static com.hazelcast.jet.core.JobStatus.NOT_RUNNING;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.JobStatus.STARTING;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.DAYS;
import static org.hamcrest.Matchers.isIn;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class JmsSourceIntegration_NonSharedClusterTest extends JetTestSupport {

    @ClassRule
    public static EmbeddedActiveMQResource realBroker = new EmbeddedActiveMQResource();

    private static final int MESSAGE_COUNT = 10_000;

    private static volatile boolean storeFailed;

    @Test
    public void when_memberTerminated_then_transactionsRolledBack() throws Exception {
        HazelcastInstance instance1 = createHazelcastInstance();
        HazelcastInstance instance2 = createHazelcastInstance();

        // use higher number of messages so that each of the parallel processors gets some
        JmsTestUtil.sendMessages(getConnectionFactory(), "queue", true, MESSAGE_COUNT);

        Pipeline p = Pipeline.create();
        IList<String> sinkList = instance1.getList("sinkList");
        p.readFrom(Sources.jmsQueueBuilder(JmsSourceIntegration_NonSharedClusterTest::getConnectionFactory)
                          .destinationName("queue")
                          .build(msg -> ((TextMessage) msg).getText()))
         .withoutTimestamps()
         .writeTo(Sinks.list(sinkList));

        instance1.getJet().newJob(p, new JobConfig()
                .setProcessingGuarantee(EXACTLY_ONCE)
                .setSnapshotIntervalMillis(DAYS.toMillis(1)));

        assertTrueEventually(() -> assertEquals("expected items not in sink", MESSAGE_COUNT, sinkList.size()), 20);

        // Now forcefully shut down the second member. The terminated member
        // will NOT roll back its transaction. We'll assert that the
        // transactions with processorIndex beyond the current total
        // parallelism are rolled back. We assert that each item is emitted
        // twice, if this was wrong, the items in the non-rolled-back
        // transaction will be stalled and only emitted once, they will be
        // emitted after the default Artemis timeout of 5 minutes.
        instance2.getLifecycleService().terminate();
        assertTrueEventually(() -> assertEquals("items should be emitted twice", MESSAGE_COUNT * 2, sinkList.size()), 30);
    }

    @Test
    public void when_snapshotFails_exactlyOnce_then_jobRestarts() {
        when_snapshotFails(EXACTLY_ONCE, true);
    }

    @Test
    public void when_snapshotFails_atLeastOnce_then_ignored() {
        when_snapshotFails(AT_LEAST_ONCE, false);
    }

    @Test
    public void when_snapshotFails_noGuarantee_then_ignored() {
        when_snapshotFails(NONE, false);
    }

    private void when_snapshotFails(ProcessingGuarantee guarantee, boolean expectFailure) {
        storeFailed = false;
        // force snapshots to fail by adding a failing map store configuration for snapshot data maps
        Config config = smallInstanceConfig();
        MapConfig mapConfig = new MapConfig(JobRepository.SNAPSHOT_DATA_MAP_PREFIX + '*');
        MapStoreConfig mapStoreConfig = mapConfig.getMapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(new FailingMapStore());
        config.addMapConfig(mapConfig);

        HazelcastInstance instance = createHazelcastInstance(config);
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.jmsQueue("queue", JmsSourceIntegration_NonSharedClusterTest::getConnectionFactory))
         .withoutTimestamps()
         .writeTo(Sinks.noop());

        Job job = instance.getJet().newJob(p, new JobConfig()
                .setProcessingGuarantee(guarantee)
                .setSnapshotIntervalMillis(100));

        assertJobStatusEventually(job, RUNNING);
        if (expectFailure) {
            // the job should be repeatedly restarted due to the snapshot failure. It goes through NOT_RUNNING and STARTING
            // states, we should observe one of those eventually
            assertTrueEventually(() -> assertThat(job.getStatus(), isIn(asList(NOT_RUNNING, STARTING))));
            assertTrue(storeFailed);
        } else {
            assertTrueAllTheTime(() -> assertEquals(RUNNING, job.getStatus()), 3);
        }
    }

    private static ConnectionFactory getConnectionFactory() {
        return new ActiveMQConnectionFactory(realBroker.getVmURL());
    }

    private static class FailingMapStore extends AMapStore implements Serializable {
        @Override
        public void store(Object o, Object o2) {
            storeFailed = true;
            throw new UnsupportedOperationException("failing map store");
        }
    }
}
