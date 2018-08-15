/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.map.helpers.AMapStore;
import com.hazelcast.config.MapConfig;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JobRestartWithSnapshotTest.SequencesInPartitionsGeneratorP;
import com.hazelcast.jet.core.TestProcessors.MockPS;
import com.hazelcast.jet.core.TestProcessors.StuckForeverSourceP;
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.jet.impl.SnapshotRepository;
import com.hazelcast.jet.impl.execution.SnapshotRecord;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.CancellationException;
import java.util.stream.Collectors;

import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.TestUtil.throttle;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeListP;
import static com.hazelcast.test.PacketFiltersUtil.rejectOperationsBetween;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
public class ManualRestartTest extends JetTestSupport {

    private static final int NODE_COUNT = 2;
    private static final int LOCAL_PARALLELISM = 1;

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private DAG dag;
    private JetInstance[] instances;

    @Before
    public void setup() {
        TestProcessors.reset(NODE_COUNT * LOCAL_PARALLELISM);

        dag = new DAG().vertex(new Vertex("test", new MockPS(StuckForeverSourceP::new, NODE_COUNT)));
        instances = createJetMembers(new JetConfig(), NODE_COUNT);
    }

    @Test
    public void when_jobIsRunning_then_itRestarts() {
        testJobRestartWhenJobIsRunning(true);
    }

    @Test
    public void when_autoRestartOnMemberFailureDisabled_then_jobRestarts() {
        testJobRestartWhenJobIsRunning(false);
    }

    private void testJobRestartWhenJobIsRunning(boolean autoRestartOnMemberFailureEnabled) {
        // Given that the job is running
        JetInstance client = createJetClient();
        Job job = client.newJob(dag, new JobConfig().setAutoScaling(autoRestartOnMemberFailureEnabled));

        assertTrueEventually(() -> assertEquals(NODE_COUNT, MockPS.initCount.get()), 10);

        // When the job is restarted after new members join to the cluster
        int newMemberCount = 2;
        for (int i = 0; i < newMemberCount; i++) {
            createJetMember();
        }

        assertTrueAllTheTime(() -> assertEquals(NODE_COUNT, MockPS.initCount.get()), 3);

        job.restart();

        // Then, the job restarts
        int initCount = NODE_COUNT * 2 + newMemberCount;
        assertTrueEventually(() -> assertEquals(initCount, MockPS.initCount.get()), 10);
    }

    @Test
    public void when_jobIsNotBeingExecuted_then_itCannotBeRestarted() {
        // Given that the job execution has not started
        rejectOperationsBetween(instances[0].getHazelcastInstance(), instances[1].getHazelcastInstance(),
                JetInitDataSerializerHook.FACTORY_ID, singletonList(JetInitDataSerializerHook.INIT_EXECUTION_OP));

        JetInstance client = createJetClient();
        Job job = client.newJob(dag);

        assertTrueEventually(() -> assertSame(job.getStatus(), JobStatus.STARTING), 10);

        // Then, the job cannot restart
        exception.expect(IllegalStateException.class);
        exception.expectMessage("Cannot RESTART");
        job.restart();
    }

    @Test
    public void when_jobIsCompleted_then_itCannotBeRestarted() {
        // Given that the job is completed
        JetInstance client = createJetClient();
        Job job = client.newJob(dag);
        job.cancel();

        try {
            job.join();
            fail();
        } catch (CancellationException ignored) {
            logger.info("Cancellation exception caught");
        }

        // Then, the job cannot restart
        exception.expect(IllegalStateException.class);
        exception.expectMessage("Cannot RESTART");
        job.restart();
    }

    @Test
    public void when_terminalSnapshotFails_then_previousSnapshotUsed() {
        MapConfig mapConfig = new MapConfig(SnapshotRepository.SNAPSHOT_DATA_NAME_PREFIX + "*");
        mapConfig.getMapStoreConfig()
                 .setClassName(FailingMapStore.class.getName())
                 .setEnabled(true);
        instances[0].getConfig().getHazelcastConfig().addMapConfig(mapConfig);
        FailingMapStore.fail = false;
        FailingMapStore.failed = false;

        DAG dag = new DAG();
        Vertex source = dag.newVertex("source",
                throttle(() -> new SequencesInPartitionsGeneratorP(2, 10000, true), 1000));
        Vertex sink = dag.newVertex("sink", writeListP("sink"));
        dag.edge(between(source, sink));
        source.localParallelism(1);
        Job job = instances[0].newJob(dag, new JobConfig()
                .setProcessingGuarantee(EXACTLY_ONCE)
                .setSnapshotIntervalMillis(2000));

        // wait for the first snapshot
        JetService jetService = getNode(instances[0]).nodeEngine.getService(JetService.SERVICE_NAME);
        SnapshotRepository snapshotRepository = jetService.getJobCoordinationService().snapshotRepository();
        assertTrueEventually(() -> assertTrue(
                snapshotRepository.getAllSnapshotRecords(job.getId()).stream().anyMatch(SnapshotRecord::isSuccessful)));

        // When
        sleepMillis(100);
        FailingMapStore.fail = true;
        job.restart();
        assertTrueEventually(() -> assertTrue(FailingMapStore.failed), 5);
        FailingMapStore.fail = false;

        job.join();

        Map<Integer, Integer> actual = new ArrayList<>(instances[0].<Entry<Integer, Integer>>getList("sink")).stream()
                .filter(e -> e.getKey() == 0)
                .map(Entry::getValue)
                .collect(Collectors.toMap(e -> e, e -> 1, (o, n) -> o + n, TreeMap::new));

        assertEquals(actual.toString(), (Integer) 1, actual.get(0));
        assertEquals(actual.toString(), (Integer) 1, actual.get(9999));
        // the result should be some ones, then some twos and then some ones. The ones should be from the time
        // when the source was replayed from last snapshot.
        boolean sawTwo = false;
        boolean sawOneAgain = false;
        for (Integer v : actual.values()) {
            if (v == 1) {
                if (sawTwo) {
                    sawOneAgain = true;
                }
            } else if (v == 2) {
                assertFalse("got a 2 in another group", sawOneAgain);
                sawTwo = true;
            } else {
                fail("v=" + v);
            }
        }
        assertTrue("didn't see any 2s", sawTwo);
    }

    public static class FailingMapStore extends AMapStore implements Serializable {
        private static volatile boolean fail;
        private static volatile boolean failed;

        @Override
        public void store(Object o, Object o2) {
            if (fail) {
                failed = true;
                throw new RuntimeException();
            }
        }
    }
}
