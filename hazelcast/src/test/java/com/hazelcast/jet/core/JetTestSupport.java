/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.cluster.Address;
import com.hazelcast.collection.IList;
import com.hazelcast.config.Config;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetTestInstanceFactory;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.function.RunnableEx;
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.jet.impl.JobExecutionRecord;
import com.hazelcast.jet.impl.JobExecutionService;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.impl.pipeline.transform.BatchSourceTransform;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.Accessors;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.rules.Timeout;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class JetTestSupport extends HazelcastTestSupport {

    /**
     * This is needed to finish tests which got stuck in @Before, @BeforeClass, @After or @AfterClass method.
     */
    @ClassRule
    public static Timeout globalTimeout = Timeout.seconds(15 * 60);

    private static final ILogger SUPPORT_LOGGER = Logger.getLogger(JetTestSupport.class);

    protected ILogger logger = Logger.getLogger(getClass());
    private JetTestInstanceFactory instanceFactory;

    @After
    public void shutdownFactory() throws Exception {
        if (instanceFactory != null) {
            SUPPORT_LOGGER.info("Terminating instanceFactory in JetTestSupport.@After");
            spawn(() -> instanceFactory.terminateAll())
                    .get(1, TimeUnit.MINUTES);
        }
    }

    protected JetInstance createJetClient() {
        return instanceFactory.newClient();
    }

    protected JetInstance createJetClient(ClientConfig config) {
        return instanceFactory.newClient(config);
    }

    protected JetInstance createJetMember() {
        return this.createJetMember(smallInstanceConfig());
    }

    protected JetInstance createJetMember(Config config) {
        if (instanceFactory == null) {
            instanceFactory = new JetTestInstanceFactory();
        }
        return instanceFactory.newMember(config);
    }

    protected JetInstance[] createJetMembers(int nodeCount) {
        return createJetMembers(smallInstanceConfig(), nodeCount);
    }

    protected JetInstance[] createJetMembers(Config config, int nodeCount) {
        if (instanceFactory == null) {
            instanceFactory = new JetTestInstanceFactory();
        }
        return instanceFactory.newMembers(config, nodeCount);
    }

    protected JetInstance createJetMember(Config config, Address[] blockedAddress) {
        if (instanceFactory == null) {
            instanceFactory = new JetTestInstanceFactory();
        }
        return instanceFactory.newMember(config, blockedAddress);
    }

    protected static <K, V> IMap<K, V> getMap(JetInstance instance) {
        return instance.getMap(randomName());
    }

    protected static void fillListWithInts(IList<Integer> list, int count) {
        for (int i = 0; i < count; i++) {
            list.add(i);
        }
    }

    protected static <E> IList<E> getList(JetInstance instance) {
        return instance.getList(randomName());
    }

    protected static void appendToFile(File file, String... lines) throws IOException {
        try (PrintWriter writer = new PrintWriter(new FileOutputStream(file, true))) {
            for (String payload : lines) {
                writer.write(payload + '\n');
            }
        }
    }

    protected static File createTempDirectory() throws IOException {
        Path directory = Files.createTempDirectory("jet-test-temp");
        File file = directory.toFile();
        file.deleteOnExit();
        return file;
    }

    public static void assertJobStatusEventually(Job job, JobStatus expected) {
        assertJobStatusEventually(job, expected, ASSERT_TRUE_EVENTUALLY_TIMEOUT);
    }

    /**
     * Asserts that a job status is eventually RUNNING. When it's running,
     * checks that the execution ID is different from the given {@code
     * ignoredExecutionId}, if not, tries again.
     * <p>
     * This is useful when checking that the job is running after a restart:
     * <pre>{@code
     *     job.restart();
     *     // This is racy, we might see the previous execution running.
     *     // Subsequent steps can fail because the job is restarting.
     *     assertJobStatusEventually(job, RUNNING);
     * }</pre>
     *
     * This method allows an equivalent code:
     * <pre>{@code
     *     long oldExecutionId = assertJobRunningEventually(instance, job, null);
     *     // now we're sure the job is safe to restart - restart fails if the job isn't running
     *     job.restart();
     *     assertJobRunningEventually(instance, job, oldExecutionId);
     *     // now we're sure that a new execution is running
     * }</pre>
     *
     * @param ignoredExecutionId If job is running and has this execution ID,
     *      wait longer. If null, no execution ID is ignored.
     * @return the execution ID of the new execution or 0 if {@code
     *      ignoredExecutionId == null}
     */
    public static long assertJobRunningEventually(JetInstance instance, Job job, Long ignoredExecutionId) {
        Long executionId;
        JobExecutionService service = getNodeEngineImpl(instance)
                .<JetService>getService(JetService.SERVICE_NAME)
                .getJobExecutionService();
        do {
            assertJobStatusEventually(job, RUNNING);
            // executionId can be null if the execution just terminated
            executionId = service.getExecutionIdForJobId(job.getId());
        } while (executionId == null || executionId.equals(ignoredExecutionId));
        return executionId;
    }

    public static void assertJobStatusEventually(Job job, JobStatus expected, int timeoutSeconds) {
        assertNotNull(job);
        assertTrueEventually(() ->
                assertEquals("jobId=" + idToString(job.getId()), expected, job.getStatus()), timeoutSeconds);
    }

    public static void assertClusterSizeEventually(int size, JetInstance jetInstance) {
        HazelcastTestSupport.assertClusterSizeEventually(size, jetInstance.getHazelcastInstance());
    }

    public static JetService getJetService(JetInstance jetInstance) {
        return getNodeEngineImpl(jetInstance).getService(JetService.SERVICE_NAME);
    }

    public static HazelcastInstance hz(JetInstance instance) {
        return instance.getHazelcastInstance();
    }

    public static Address getAddress(JetInstance instance) {
        return Accessors.getAddress(hz(instance));
    }

    public static Node getNode(JetInstance instance) {
        return Accessors.getNode(hz(instance));
    }

    public static NodeEngineImpl getNodeEngineImpl(JetInstance instance) {
        return Accessors.getNodeEngineImpl(hz(instance));
    }

    public Address nextAddress() {
        return instanceFactory.nextAddress();
    }

    protected void terminateInstance(JetInstance instance) {
        instanceFactory.terminate(instance);
    }

    /**
     * Runs the given Runnable in a new thread, if you're not interested in the
     * execution failure, any errors are logged at WARN level.
     */
    public Future spawnSafe(RunnableEx r) {
        return spawn(() -> {
            try {
                r.runEx();
            } catch (Throwable e) {
                SUPPORT_LOGGER.warning("Spawned Runnable failed", e);
            }
        });
    }

    public static Watermark wm(long timestamp) {
        return new Watermark(timestamp);
    }

    public void waitForFirstSnapshot(JobRepository jr, long jobId, int timeoutSeconds, boolean allowEmptySnapshot) {
        long[] snapshotId = {-1};
        assertTrueEventually(() -> {
            JobExecutionRecord record = jr.getJobExecutionRecord(jobId);
            assertNotNull("null JobExecutionRecord", record);
            assertTrue("No snapshot produced",
                    record.dataMapIndex() >= 0 && record.snapshotId() >= 0);
            assertTrue("stats are 0", allowEmptySnapshot || record.snapshotStats().numBytes() > 0);
            snapshotId[0] = record.snapshotId();
        }, timeoutSeconds);
        SUPPORT_LOGGER.info("First snapshot found (id=" + snapshotId[0] + ")");
    }

    public void waitForNextSnapshot(JobRepository jr, long jobId, int timeoutSeconds, boolean allowEmptySnapshot) {
        long originalSnapshotId = jr.getJobExecutionRecord(jobId).snapshotId();
        // wait until there is at least one more snapshot
        long[] snapshotId = {-1};
        long start = System.nanoTime();
        assertTrueEventually(() -> {
            JobExecutionRecord record = jr.getJobExecutionRecord(jobId);
            assertNotNull("jobExecutionRecord is null", record);
            snapshotId[0] = record.snapshotId();
            assertTrue("No more snapshots produced in " + timeoutSeconds + " seconds",
                    snapshotId[0] > originalSnapshotId);
            assertTrue("stats are 0", allowEmptySnapshot || record.snapshotStats().numBytes() > 0);
        }, timeoutSeconds);
        SUPPORT_LOGGER.info("Next snapshot found after " + NANOSECONDS.toMillis(System.nanoTime() - start) + " ms (id="
                + snapshotId[0] + ", previous id=" + originalSnapshotId + ")");
    }

    /**
     * Clean up the cluster and make it ready to run a next test. If we fail
     * to, shut it down so that next tests don't run on a messed-up cluster.
     *
     * @param instances cluster instances, must contain at least
     *                            one instance
     */
    public void cleanUpCluster(JetInstance ... instances) {
        for (Job job : instances[0].getJobs()) {
            ditchJob(job, instances);
        }
        for (DistributedObject o : instances[0].getHazelcastInstance().getDistributedObjects()) {
            o.destroy();
        }
    }

    /**
     * Give this method a job and it will ensure it's no longer running. It
     * will ignore if it's not running. If the cancellation fails, it will
     * retry.
     */
    public void ditchJob(@Nonnull Job job, @Nonnull JetInstance... instancesToShutDown) {
        int numAttempts;
        for (numAttempts = 0; numAttempts < 10; numAttempts++) {
            JobStatus status = null;
            try {
                status = job.getStatus();
                if (status == JobStatus.FAILED || status == JobStatus.COMPLETED) {
                    return;
                }
            } catch (Exception e) {
                SUPPORT_LOGGER.warning("Failure to read job status: " + e, e);
            }

            Exception cancellationFailure;
            try {
                job.cancel();
                try {
                    job.join();
                } catch (Exception ignored) {
                    // This can be CancellationException or any other job failure. We don't care,
                    // we're supposed to rid the cluster of the job and that's what we have.
                }
                return;
            } catch (Exception e) {
                cancellationFailure = e;
            }

            sleepMillis(500);
            SUPPORT_LOGGER.warning("Failed to cancel the job and it is " + status + ", retrying. Failure: "
                            + cancellationFailure, cancellationFailure);
        }
        // if we got here, 10 attempts to cancel the job have failed. Cluster is in bad shape probably, shut it down
        try {
            for (JetInstance instance : instancesToShutDown) {
                instance.getHazelcastInstance().getLifecycleService().terminate();
            }
        } catch (Exception e) {
            // ignore, proceed to throwing RuntimeException
        }
        throw new RuntimeException(numAttempts + " attempts to cancel the job failed" +
                (instancesToShutDown.length > 0 ? ", shut down the cluster" : ""));
    }

    /**
     * Cancel the job and wait until it cancels using Job.join(), ignoring the
     * CancellationException.
     */
    public static void cancelAndJoin(@Nonnull Job job) {
        job.cancel();
        try {
            job.join();
            fail("join didn't fail with CancellationException");
        } catch (CancellationException ignored) {
        }
    }

    public static <T> void assertCollection(Collection<T> expected, Collection<T> actual) {
        assertEquals(String.format("Expected collection: `%s`, actual collection: `%s`", expected, actual),
                expected.size(), actual.size());
        assertContainsAll(expected, actual);
    }

    public static <T> ProcessorMetaSupplier processorFromPipelineSource(BatchSource<T> source) {
        return ((BatchSourceTransform<T>) source).metaSupplier;
    }
}
