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

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.TestProcessors.MockPS;
import com.hazelcast.jet.core.TestProcessors.StuckProcessor;
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.jet.impl.JobRecord;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.impl.MasterContext;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static com.hazelcast.jet.core.JobStatus.COMPLETED;
import static com.hazelcast.jet.core.JobStatus.RESTARTING;
import static com.hazelcast.jet.core.JobStatus.STARTING;
import static com.hazelcast.spi.partition.IPartition.MAX_BACKUP_COUNT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
public class SplitBrainTest extends JetSplitBrainTestSupport {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();


    @Override
    protected void onBeforeSetup() {
        MockPS.closeCount.set(0);
        MockPS.initCount.set(0);
        MockPS.receivedCloseErrors.clear();

        StuckProcessor.proceedLatch = new CountDownLatch(1);
    }

    @Override
    protected void onJetConfigCreated(JetConfig jetConfig) {
        jetConfig.getInstanceConfig().setBackupCount(MAX_BACKUP_COUNT);
    }

    @Test
    public void when_quorumIsLostOnMinority_then_jobRestartsUntilMerge() {
        int firstSubClusterSize = 3;
        int secondSubClusterSize = 2;
        int clusterSize = firstSubClusterSize + secondSubClusterSize;
        StuckProcessor.executionStarted = new CountDownLatch(clusterSize * PARALLELISM);
        Job[] jobRef = new Job[1];

        Consumer<JetInstance[]> beforeSplit = instances -> {
            MockPS processorSupplier = new MockPS(StuckProcessor::new, clusterSize);
            DAG dag = new DAG().vertex(new Vertex("test", processorSupplier));
            jobRef[0] = instances[0].newJob(dag, new JobConfig().setSplitBrainProtection(true));
            assertOpenEventually(StuckProcessor.executionStarted);
        };

        Future[] minorityJobFutureRef = new Future[1];

        BiConsumer<JetInstance[], JetInstance[]> onSplit = (firstSubCluster, secondSubCluster) -> {
            StuckProcessor.proceedLatch.countDown();

            assertTrueEventually(() ->
                    assertEquals(clusterSize + firstSubClusterSize, MockPS.initCount.get()));

            long jobId = jobRef[0].getId();

            assertTrueEventually(() -> {
                JetService service = getJetService(firstSubCluster[0]);
                assertEquals(COMPLETED, service.getJobCoordinationService().getJobStatus(jobId));
            });

            JetService service2 = getJetService(secondSubCluster[0]);

            assertTrueEventually(() -> {
                assertEquals(STARTING, service2.getJobCoordinationService().getJobStatus(jobId));
            });

            MasterContext masterContext = service2.getJobCoordinationService().getMasterContext(jobId);
            assertNotNull(masterContext);
            minorityJobFutureRef[0] = masterContext.completionFuture();

            assertTrueAllTheTime(() -> {
                assertEquals(STARTING, service2.getJobCoordinationService().getJobStatus(jobId));
            }, 20);
        };

        Consumer<JetInstance[]> afterMerge = instances -> {
            assertTrueEventually(() -> {
                assertEquals(clusterSize + firstSubClusterSize, MockPS.initCount.get());
                assertEquals(clusterSize + firstSubClusterSize, MockPS.closeCount.get());
            });

            assertEquals(clusterSize, MockPS.receivedCloseErrors.size());
            MockPS.receivedCloseErrors.forEach(t -> assertTrue(t instanceof TopologyChangedException));

            try {
                minorityJobFutureRef[0].get();
                fail();
            } catch (CancellationException ignored) {
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        };

        testSplitBrain(firstSubClusterSize, secondSubClusterSize, beforeSplit, onSplit, afterMerge);
    }

    @Test
    public void when_quorumIsLostOnBothSides_then_jobRestartsUntilMerge() {
        int firstSubClusterSize = 2;
        int secondSubClusterSize = 2;
        int clusterSize = firstSubClusterSize + secondSubClusterSize;
        StuckProcessor.executionStarted = new CountDownLatch(clusterSize * PARALLELISM);
        Job[] jobRef = new Job[1];

        Consumer<JetInstance[]> beforeSplit = instances -> {
            MockPS processorSupplier = new MockPS(StuckProcessor::new, clusterSize);
            DAG dag = new DAG().vertex(new Vertex("test", processorSupplier));
            jobRef[0] = instances[0].newJob(dag, new JobConfig().setSplitBrainProtection(true));
            assertOpenEventually(StuckProcessor.executionStarted);
        };

        BiConsumer<JetInstance[], JetInstance[]> onSplit = (firstSubCluster, secondSubCluster) -> {
            StuckProcessor.proceedLatch.countDown();

            long jobId = jobRef[0].getId();

            assertTrueEventually(() -> {
                JetService service1 = getJetService(firstSubCluster[0]);
                JetService service2 = getJetService(secondSubCluster[0]);
                assertEquals(RESTARTING, service1.getJobCoordinationService().getJobStatus(jobId));
                assertEquals(STARTING, service2.getJobCoordinationService().getJobStatus(jobId));
            });

            assertTrueAllTheTime(() -> {
                JetService service1 = getJetService(firstSubCluster[0]);
                JetService service2 = getJetService(secondSubCluster[0]);
                assertEquals(RESTARTING, service1.getJobCoordinationService().getJobStatus(jobId));
                assertEquals(STARTING, service2.getJobCoordinationService().getJobStatus(jobId));
            }, 20);
        };

        Consumer<JetInstance[]> afterMerge = instances -> {
            assertTrueEventually(() -> {
                assertEquals(clusterSize * 2, MockPS.initCount.get());
                assertEquals(clusterSize * 2, MockPS.closeCount.get());
            });

            assertEquals(clusterSize, MockPS.receivedCloseErrors.size());
            MockPS.receivedCloseErrors.forEach(t -> assertTrue(t instanceof TopologyChangedException));
        };

        testSplitBrain(firstSubClusterSize, secondSubClusterSize, beforeSplit, onSplit, afterMerge);
    }

    @Test
    public void when_splitBrainProtectionIsDisabled_then_jobCompletesOnBothSides() {
        int firstSubClusterSize = 2;
        int secondSubClusterSize = 2;
        int clusterSize = firstSubClusterSize + secondSubClusterSize;
        StuckProcessor.executionStarted = new CountDownLatch(clusterSize * PARALLELISM);
        Job[] jobRef = new Job[1];

        Consumer<JetInstance[]> beforeSplit = instances -> {
            MockPS processorSupplier = new MockPS(StuckProcessor::new, clusterSize);
            DAG dag = new DAG().vertex(new Vertex("test", processorSupplier));
            jobRef[0] = instances[0].newJob(dag);
            assertOpenEventually(StuckProcessor.executionStarted);
        };

        BiConsumer<JetInstance[], JetInstance[]> onSplit = (firstSubCluster, secondSubCluster) -> {
            StuckProcessor.proceedLatch.countDown();

            long jobId = jobRef[0].getId();

            assertTrueEventually(() -> {
                JetService service1 = getJetService(firstSubCluster[0]);
                JetService service2 = getJetService(secondSubCluster[0]);
                assertEquals(COMPLETED, service1.getJobCoordinationService().getJobStatus(jobId));
                assertEquals(COMPLETED, service2.getJobCoordinationService().getJobStatus(jobId));
            });
        };

        Consumer<JetInstance[]> afterMerge = instances -> {
            assertTrueEventually(() -> {
                assertEquals(clusterSize * 2, MockPS.initCount.get());
                assertEquals(clusterSize * 2, MockPS.closeCount.get());
            });

            assertEquals(clusterSize, MockPS.receivedCloseErrors.size());
            MockPS.receivedCloseErrors.forEach(t -> assertTrue(t instanceof TopologyChangedException));
        };

        testSplitBrain(firstSubClusterSize, secondSubClusterSize, beforeSplit, onSplit, afterMerge);
    }

    @Test
    public void when_jobIsSubmittedToMinoritySide_then_jobIsCancelledDuringMerge() {
        int firstSubClusterSize = 3;
        int secondSubClusterSize = 2;
        int clusterSize = firstSubClusterSize + secondSubClusterSize;
        StuckProcessor.executionStarted = new CountDownLatch(secondSubClusterSize * PARALLELISM);
        Job[] jobRef = new Job[1];

        BiConsumer<JetInstance[], JetInstance[]> onSplit = (firstSubCluster, secondSubCluster) -> {
            MockPS processorSupplier = new MockPS(StuckProcessor::new, clusterSize);
            DAG dag = new DAG().vertex(new Vertex("test", processorSupplier));
            jobRef[0] = secondSubCluster[1].newJob(dag, new JobConfig().setSplitBrainProtection(true));
            assertOpenEventually(StuckProcessor.executionStarted);
        };

        Consumer<JetInstance[]> afterMerge = instances -> {
            assertEquals(secondSubClusterSize, MockPS.receivedCloseErrors.size());
            MockPS.receivedCloseErrors.forEach(t -> assertTrue(t instanceof TopologyChangedException));

            try {
                jobRef[0].getFuture().get(30, TimeUnit.SECONDS);
                fail();
            } catch (CancellationException ignored) {
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        };

        testSplitBrain(firstSubClusterSize, secondSubClusterSize, null, onSplit, afterMerge);
    }

    @Test
    public void when_newMemberJoinsToCluster_then_jobQuorumSizeIsUpdated() {
        int clusterSize = 3;
        JetConfig jetConfig = new JetConfig();
        JetInstance[] instances = new JetInstance[clusterSize];
        for (int i = 0; i < clusterSize; i++) {
            instances[i] = createJetMember(jetConfig);
        }

        StuckProcessor.executionStarted = new CountDownLatch(clusterSize * PARALLELISM);
        MockPS processorSupplier = new MockPS(StuckProcessor::new, clusterSize);
        DAG dag = new DAG().vertex(new Vertex("test", processorSupplier));
        Job job = instances[0].newJob(dag, new JobConfig().setSplitBrainProtection(true));
        assertOpenEventually(StuckProcessor.executionStarted);

        createJetMember(jetConfig);

        assertTrueEventually(() -> {
            JobRepository jobRepository = getJetService(instances[0]).getJobRepository();
            JobRecord jobRecord = jobRepository.getJobRecord(job.getId());
            assertEquals(3, jobRecord.getQuorumSize());
        });

        StuckProcessor.proceedLatch.countDown();
    }

    @Test
    public void when_newMemberIsAddedAfterClusterSizeFallsBelowQuorumSize_then_jobRestartDoesNotSucceed() {
        int clusterSize = 5;
        JetConfig jetConfig = new JetConfig();
        JetInstance[] instances = new JetInstance[clusterSize];
        for (int i = 0; i < clusterSize; i++) {
            instances[i] = createJetMember(jetConfig);
        }

        StuckProcessor.executionStarted = new CountDownLatch(clusterSize * PARALLELISM);
        MockPS processorSupplier = new MockPS(StuckProcessor::new, clusterSize);
        DAG dag = new DAG().vertex(new Vertex("test", processorSupplier));
        Job job = instances[0].newJob(dag, new JobConfig().setSplitBrainProtection(true));
        assertOpenEventually(StuckProcessor.executionStarted);

        for (int i = 1; i < clusterSize; i++) {
            instances[i].getHazelcastInstance().getLifecycleService().terminate();
        }

        StuckProcessor.proceedLatch.countDown();

        assertTrueEventually(() -> assertEquals(RESTARTING, job.getStatus()));

        createJetMember(jetConfig);

        assertTrueAllTheTime(() -> assertEquals(RESTARTING, job.getStatus()), 5);
    }

}
