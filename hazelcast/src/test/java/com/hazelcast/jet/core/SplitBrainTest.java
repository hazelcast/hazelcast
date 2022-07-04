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

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.TestProcessors.MockPS;
import com.hazelcast.jet.core.TestProcessors.NoOutputSourceP;
import com.hazelcast.jet.impl.JetServiceBackend;
import com.hazelcast.jet.impl.JobExecutionRecord;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.impl.MasterContext;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static com.hazelcast.internal.partition.IPartition.MAX_BACKUP_COUNT;
import static com.hazelcast.jet.core.JobStatus.COMPLETED;
import static com.hazelcast.jet.core.JobStatus.NOT_RUNNING;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.JobStatus.STARTING;
import static org.assertj.core.util.Lists.newArrayList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({NightlyTest.class})
public class SplitBrainTest extends JetSplitBrainTestSupport {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Override
    protected void onBeforeSetup() {
        TestProcessors.reset(1);
    }

    @Override
    protected void onConfigCreated(Config config) {
        config.getJetConfig().setBackupCount(MAX_BACKUP_COUNT);
        config.getJetConfig().setScaleUpDelayMillis(3000);
    }

    @Test
    public void when_quorumIsLostOnMinority_then_jobDoesNotRestartOnMinorityAndCancelledAfterMerge() {
        int firstSubClusterSize = 3;
        int secondSubClusterSize = 2;
        int clusterSize = firstSubClusterSize + secondSubClusterSize;
        NoOutputSourceP.executionStarted = new CountDownLatch(clusterSize * PARALLELISM);
        Job[] jobRef = new Job[1];

        Consumer<HazelcastInstance[]> beforeSplit = instances -> {
            MockPS processorSupplier = new MockPS(NoOutputSourceP::new, clusterSize);
            DAG dag = new DAG().vertex(new Vertex("test", processorSupplier));
            jobRef[0] = instances[0].getJet().newJob(dag, new JobConfig().setSplitBrainProtection(true));
            assertOpenEventually(NoOutputSourceP.executionStarted);
        };

        Future[] minorityJobFutureRef = new Future[1];

        BiConsumer<HazelcastInstance[], HazelcastInstance[]> onSplit = (firstSubCluster, secondSubCluster) -> {
            NoOutputSourceP.proceedLatch.countDown();

            assertTrueEventually(() ->
                    assertEquals(clusterSize + firstSubClusterSize, MockPS.initCount.get()));

            long jobId = jobRef[0].getId();

            assertTrueEventually(() -> {
                JetServiceBackend service = getJetServiceBackend(firstSubCluster[0]);
                assertEquals(COMPLETED, service.getJobCoordinationService().getJobStatus(jobId).get());
            });

            JetServiceBackend service2 = getJetServiceBackend(secondSubCluster[0]);

            assertTrueEventually(() -> {
                MasterContext masterContext = service2.getJobCoordinationService().getMasterContext(jobId);
                assertNotNull(masterContext);
                minorityJobFutureRef[0] = masterContext.jobContext().jobCompletionFuture();
            });

            assertTrueAllTheTime(() -> {
                assertStatusNotRunningOrStarting(service2.getJobCoordinationService().getJobStatus(jobId).get());
            }, 20);
        };

        Consumer<HazelcastInstance[]> afterMerge = instances -> {
            assertTrueEventually(() -> {
                assertEquals(clusterSize + firstSubClusterSize, MockPS.initCount.get());
                assertEquals(clusterSize + firstSubClusterSize, MockPS.closeCount.get());
            });

            assertEquals(clusterSize, MockPS.receivedCloseErrors.size());
            MockPS.receivedCloseErrors.forEach(t -> assertTrue("received " + t, t instanceof CancellationException));

            try {
                minorityJobFutureRef[0].get();
                fail();
            } catch (CancellationException expected) {
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        };

        testSplitBrain(firstSubClusterSize, secondSubClusterSize, beforeSplit, onSplit, afterMerge);
    }

    @Test
    public void when_quorumIsLostOnBothSides_then_jobRestartsAfterMerge() {
        int firstSubClusterSize = 2;
        int secondSubClusterSize = 2;
        int clusterSize = firstSubClusterSize + secondSubClusterSize;
        NoOutputSourceP.executionStarted = new CountDownLatch(clusterSize * PARALLELISM);
        Job[] jobRef = new Job[1];

        Consumer<HazelcastInstance[]> beforeSplit = instances -> {
            MockPS processorSupplier = new MockPS(NoOutputSourceP::new, clusterSize);
            DAG dag = new DAG().vertex(new Vertex("test", processorSupplier));
            jobRef[0] = instances[0].getJet().newJob(dag, new JobConfig().setSplitBrainProtection(true));
            assertOpenEventually(NoOutputSourceP.executionStarted);
        };

        BiConsumer<HazelcastInstance[], HazelcastInstance[]> onSplit = (firstSubCluster, secondSubCluster) -> {
            NoOutputSourceP.proceedLatch.countDown();

            long jobId = jobRef[0].getId();

            assertTrueEventually(() -> {
                JetServiceBackend service1 = getJetServiceBackend(firstSubCluster[0]);
                JetServiceBackend service2 = getJetServiceBackend(secondSubCluster[0]);
                MasterContext masterContext = service1.getJobCoordinationService().getMasterContext(jobId);
                assertNotNull(masterContext);
                masterContext = service2.getJobCoordinationService().getMasterContext(jobId);
                assertNotNull(masterContext);
            });

            assertTrueAllTheTime(() -> {
                JetServiceBackend service1 = getJetServiceBackend(firstSubCluster[0]);
                JetServiceBackend service2 = getJetServiceBackend(secondSubCluster[0]);
                JobStatus status1 = service1.getJobCoordinationService().getJobStatus(jobId).get();
                JobStatus status2 = service2.getJobCoordinationService().getJobStatus(jobId).get();
                assertStatusNotRunningOrStarting(status1);
                assertStatusNotRunningOrStarting(status2);
            }, 20);
        };

        Consumer<HazelcastInstance[]> afterMerge = instances -> {
            assertTrueEventually(() -> {
                assertEquals(clusterSize * 2, MockPS.initCount.get());
                assertEquals(clusterSize * 2, MockPS.closeCount.get());
            });

            assertEquals(clusterSize, MockPS.receivedCloseErrors.size());
            MockPS.receivedCloseErrors.forEach(t -> assertTrue("received " + t, t instanceof CancellationException));
        };

        testSplitBrain(firstSubClusterSize, secondSubClusterSize, beforeSplit, onSplit, afterMerge);
    }

    @Test
    public void when_splitBrainProtectionIsDisabled_then_jobCompletesOnBothSides() {
        int firstSubClusterSize = 2;
        int secondSubClusterSize = 2;
        int clusterSize = firstSubClusterSize + secondSubClusterSize;
        NoOutputSourceP.executionStarted = new CountDownLatch(clusterSize * PARALLELISM);
        Job[] jobRef = new Job[1];

        Consumer<HazelcastInstance[]> beforeSplit = instances -> {
            MockPS processorSupplier = new MockPS(NoOutputSourceP::new, clusterSize);
            DAG dag = new DAG().vertex(new Vertex("test", processorSupplier));
            jobRef[0] = instances[0].getJet().newJob(dag);
            assertOpenEventually(NoOutputSourceP.executionStarted);
        };

        BiConsumer<HazelcastInstance[], HazelcastInstance[]> onSplit = (firstSubCluster, secondSubCluster) -> {
            NoOutputSourceP.proceedLatch.countDown();

            long jobId = jobRef[0].getId();

            assertTrueEventually(() -> {
                JetServiceBackend service1 = getJetServiceBackend(firstSubCluster[0]);
                JetServiceBackend service2 = getJetServiceBackend(secondSubCluster[0]);
                assertEquals(COMPLETED, service1.getJobCoordinationService().getJobStatus(jobId).get());
                assertEquals(COMPLETED, service2.getJobCoordinationService().getJobStatus(jobId).get());
            });
        };

        Consumer<HazelcastInstance[]> afterMerge = instances -> {
            assertTrueEventually(() -> {
                assertEquals("init count", clusterSize * 2, MockPS.initCount.get());
                assertEquals("close count", clusterSize * 2, MockPS.closeCount.get());
            });

            assertEquals(clusterSize, MockPS.receivedCloseErrors.size());
            MockPS.receivedCloseErrors.forEach(t -> assertTrue("received " + t, t instanceof CancellationException));
        };

        testSplitBrain(firstSubClusterSize, secondSubClusterSize, beforeSplit, onSplit, afterMerge);
    }

    @Test
    public void when_jobIsSubmittedToMinoritySide_then_jobIsCancelledDuringMerge() {
        int firstSubClusterSize = 3;
        int secondSubClusterSize = 2;
        NoOutputSourceP.executionStarted = new CountDownLatch(secondSubClusterSize * PARALLELISM);
        Job[] jobRef = new Job[1];

        BiConsumer<HazelcastInstance[], HazelcastInstance[]> onSplit = (firstSubCluster, secondSubCluster) -> {
            MockPS processorSupplier = new MockPS(NoOutputSourceP::new, secondSubClusterSize);
            DAG dag = new DAG().vertex(new Vertex("test", processorSupplier));
            jobRef[0] = secondSubCluster[0].getJet().newJob(dag, new JobConfig().setSplitBrainProtection(true));
            assertOpenEventually(NoOutputSourceP.executionStarted);
        };

        Consumer<HazelcastInstance[]> afterMerge = instances -> {
            assertTrueEventually(() -> assertEquals(secondSubClusterSize, MockPS.receivedCloseErrors.size()), 20);
            MockPS.receivedCloseErrors.forEach(t -> assertTrue("received: " + t, t instanceof CancellationException));

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
        HazelcastInstance[] instances = new HazelcastInstance[clusterSize];
        for (int i = 0; i < clusterSize; i++) {
            instances[i] = createHazelcastInstance(createConfig());
        }

        NoOutputSourceP.executionStarted = new CountDownLatch(clusterSize * PARALLELISM);
        MockPS processorSupplier = new MockPS(NoOutputSourceP::new, clusterSize);
        DAG dag = new DAG().vertex(new Vertex("test", processorSupplier).localParallelism(PARALLELISM));
        Job job = instances[0].getJet().newJob(dag, new JobConfig().setSplitBrainProtection(true));
        assertOpenEventually(NoOutputSourceP.executionStarted);

        createHazelcastInstance(createConfig());

        assertTrueEventually(() -> {
            JetServiceBackend service = getJetServiceBackend(instances[0]);
            JobRepository jobRepository = service.getJobRepository();
            JobExecutionRecord record = jobRepository.getJobExecutionRecord(job.getId());
            assertEquals(3, record.getQuorumSize());
            MasterContext masterContext = service.getJobCoordinationService().getMasterContext(job.getId());
            assertEquals(3, masterContext.jobExecutionRecord().getQuorumSize());
        });

        NoOutputSourceP.proceedLatch.countDown();
    }

    @Test
    public void when_newMemberIsAddedAfterClusterSizeFallsBelowQuorumSize_then_jobRestartDoesNotSucceed() {
        int clusterSize = 5;
        HazelcastInstance[] instances = new HazelcastInstance[clusterSize];
        for (int i = 0; i < clusterSize; i++) {
            instances[i] = createHazelcastInstance(createConfig());
        }

        NoOutputSourceP.executionStarted = new CountDownLatch(clusterSize * PARALLELISM);
        MockPS processorSupplier = new MockPS(NoOutputSourceP::new, clusterSize);
        DAG dag = new DAG().vertex(new Vertex("test", processorSupplier).localParallelism(PARALLELISM));
        Job job = instances[0].getJet().newJob(dag, new JobConfig().setSplitBrainProtection(true));
        assertOpenEventually(NoOutputSourceP.executionStarted);

        for (int i = 1; i < clusterSize; i++) {
            instances[i].shutdown();
        }
        NoOutputSourceP.proceedLatch.countDown();
        assertJobStatusEventually(job, NOT_RUNNING, 10);
        HazelcastInstance instance6 = createHazelcastInstance(createConfig());
        assertTrueAllTheTime(() -> assertStatusNotRunningOrStarting(job.getStatus()), 5);

        // The test ends with a cluster size 2, which is below quorum
        // Start another instance so the job can restart and be cleaned up correctly
        HazelcastInstance instance7 = createHazelcastInstance(createConfig());
        waitAllForSafeState(newArrayList(instances[0], instance6, instance7));
        assertTrueEventually(() -> assertStatusRunningOrCompleted(job.getStatus()), 5);
    }

    private void assertStatusNotRunningOrStarting(JobStatus status) {
        assertTrue("status=" + status, status == NOT_RUNNING || status == STARTING);
    }

    private void assertStatusRunningOrCompleted(JobStatus status) {
        assertTrue("status=" + status, status == RUNNING || status == COMPLETED);
    }

    @Test
    public void when_minorityMasterBecomesMajorityMaster_then_jobKeepsRunning() {
        int firstSubClusterSize = 2;
        int secondSubClusterSize = 1;
        int clusterSize = firstSubClusterSize + secondSubClusterSize;
        NoOutputSourceP.executionStarted = new CountDownLatch(secondSubClusterSize * PARALLELISM);
        Job[] jobRef = new Job[1];

        Consumer<HazelcastInstance[]> beforeSplit = instances -> {
            MockPS processorSupplier = new MockPS(NoOutputSourceP::new, clusterSize);
            DAG dag = new DAG().vertex(new Vertex("test", processorSupplier));
            jobRef[0] = instances[2].getJet().newJob(dag);
            assertOpenEventually(NoOutputSourceP.executionStarted);
        };

        Consumer<HazelcastInstance[]> afterMerge = instances -> {
            assertEquals(clusterSize, instances.length);

            logger.info("Shutting down 1st instance");
            instances[0].shutdown();
            logger.info("1st instance down, starting another instance");
            createHazelcastInstance(createConfig());

            logger.info("Shutting down 2nd instance");
            instances[1].shutdown();

            assertTrue(((ClusterService) instances[2].getCluster()).isMaster());

            assertJobStatusEventually(jobRef[0], RUNNING, 10);
            assertTrueAllTheTime(() -> assertEquals(RUNNING, jobRef[0].getStatus()), 5);
        };

        testSplitBrain(firstSubClusterSize, secondSubClusterSize, beforeSplit, null, afterMerge);
    }

    @Test
    public void when_splitBrainProtectionDisabled_then_jobRunsTwiceAndAgainOnceAfterHeal() {
        int firstSubClusterSize = 3;
        int secondSubClusterSize = 2;
        int clusterSize = firstSubClusterSize + secondSubClusterSize;
        NoOutputSourceP.executionStarted = new CountDownLatch(secondSubClusterSize * PARALLELISM);
        Job[] jobRef = new Job[1];

        Consumer<HazelcastInstance[]> beforeSplit = instances -> {
            MockPS processorSupplier = new MockPS(NoOutputSourceP::new, clusterSize);
            DAG dag = new DAG().vertex(new Vertex("test", processorSupplier));
            jobRef[0] = instances[0].getJet().newJob(dag, new JobConfig().setSplitBrainProtection(false));
            assertTrueEventually(() -> assertEquals("initCount", clusterSize, MockPS.initCount.get()), 10);
            assertOpenEventually("executionStarted", NoOutputSourceP.executionStarted);
        };

        BiConsumer<HazelcastInstance[], HazelcastInstance[]> onSplit = (firstSubCluster, secondSubCluster) -> {
            Job jobRef1 = firstSubCluster[0].getJet().getJob(jobRef[0].getId());
            Job jobRef2 = secondSubCluster[0].getJet().getJob(jobRef[0].getId());
            assertNotNull("jobRef1", jobRef1);
            assertNotNull("jobRef2", jobRef2);
            assertTrueEventually(() -> assertEquals("job not running on subcluster 1", RUNNING, jobRef1.getStatus()));
            assertTrueEventually(() -> assertEquals("job not running on subcluster 2", RUNNING, jobRef2.getStatus()));
            // we need assert-eventually here because we might observe RUNNING state from an execution before the split
            assertTrueEventually(() -> assertEquals("initCount", clusterSize * 2, MockPS.initCount.get()));
        };

        Consumer<HazelcastInstance[]> afterMerge = instances -> {
            // this assert will hold after the job scales up
            assertTrueEventually(() -> assertEquals(clusterSize * 3, MockPS.initCount.get()), 20);
        };

        testSplitBrain(firstSubClusterSize, secondSubClusterSize, beforeSplit, onSplit, afterMerge);
    }
}
