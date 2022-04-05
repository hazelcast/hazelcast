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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.collection.IList;
import com.hazelcast.config.Config;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.function.RunnableEx;
import com.hazelcast.jet.impl.JetServiceBackend;
import com.hazelcast.jet.impl.JobClassLoaderService;
import com.hazelcast.jet.impl.JobExecutionRecord;
import com.hazelcast.jet.impl.JobExecutionService;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.impl.execution.ExecutionContext;
import com.hazelcast.jet.impl.pipeline.transform.BatchSourceTransform;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.Accessors;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.OverridePropertyRule;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.impl.JetServiceBackend.SERVICE_NAME;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class JetTestSupport extends HazelcastTestSupport {

    public static final SerializationService TEST_SS = new DefaultSerializationServiceBuilder().build();

    /**
     * This is needed to finish tests which got stuck in @Before, @BeforeClass, @After or @AfterClass method.
     */
    @ClassRule
    public static Timeout globalTimeout = Timeout.seconds(15 * 60);

    @ClassRule
    public static OverridePropertyRule enableJetRule = OverridePropertyRule.set("hz.jet.enabled", "true");

    private static final ILogger SUPPORT_LOGGER = Logger.getLogger(JetTestSupport.class);

    protected ILogger logger = Logger.getLogger(getClass());
    private TestHazelcastFactory instanceFactory;

    @After
    public void shutdownFactory() throws Exception {
        if (instanceFactory != null) {
            Map<Long, String> leakedClassloaders = shutdownJobsAndGetLeakedClassLoaders();

            SUPPORT_LOGGER.info("Terminating instanceFactory in JetTestSupport.@After");
            spawn(() -> instanceFactory.terminateAll())
                    .get(1, TimeUnit.MINUTES);

            if (!leakedClassloaders.isEmpty()) {
                String ids = leakedClassloaders
                        .entrySet().stream()
                        .map(entry -> idToString(entry.getKey()) + "[" + entry.getValue() + "]")
                        .collect(joining(", "));
                fail("There are one or more leaked job classloaders. " +
                        "This is a bug, but it is not necessarily related to this test. " +
                        "The classloader was leaked for the following jobIds: " + ids);
            }
        }
    }

    @Nonnull
    private Map<Long, String> shutdownJobsAndGetLeakedClassLoaders() {
        Map<Long, String> leakedClassloaders = new HashMap<>();
        Collection<HazelcastInstance> instances = instanceFactory.getAllHazelcastInstances();
        for (HazelcastInstance instance : instances) {
            if (instance.getConfig().getJetConfig().isEnabled()) {
                // Some tests leave jobs running, which keeps job classloader, shut down all running/starting jobs
                JetService jet = instance.getJet();
                List<Job> jobs = jet.getJobs();
                for (Job job : jobs) {
                    ditchJob(job, instances.toArray(new HazelcastInstance[instances.size()]));
                }

                JobClassLoaderService jobClassLoaderService = ((HazelcastInstanceImpl) instance).node
                        .getNodeEngine()
                        .<JetServiceBackend>getService(SERVICE_NAME)
                        .getJobClassLoaderService();

                Map<Long, ?> classLoaders = jobClassLoaderService.getClassLoaders();
                // The classloader cleanup is done asynchronously in some cases, wait up to 10s
                for (int i = 0; i < 100 && !classLoaders.isEmpty(); i++) {
                    sleepMillis(100);
                }
                for (Entry<Long, ?> entry : classLoaders.entrySet()) {
                    leakedClassloaders.put(entry.getKey(), entry.toString());
                }
            }
        }
        return leakedClassloaders;
    }

    protected HazelcastInstance createHazelcastClient() {
        return instanceFactory.newHazelcastClient();
    }

    protected HazelcastInstance createHazelcastClient(ClientConfig config) {
        return instanceFactory.newHazelcastClient(config);
    }

    /**
     * Returns config to configure a non-smart client that connects to the
     * given instance only.
     */
    protected ClientConfig configForNonSmartClientConnectingTo(HazelcastInstance targetInstance) {
        ClientConfig clientConfig = new ClientConfig();
        Member coordinator = targetInstance.getCluster().getLocalMember();
        clientConfig.getNetworkConfig()
                .addAddress(coordinator.getAddress().getHost() + ':' + coordinator.getAddress().getPort())
                .setSmartRouting(false);
        return clientConfig;
    }

    protected HazelcastInstance createHazelcastInstance() {
        return createHazelcastInstance(smallInstanceConfig());
    }

    protected HazelcastInstance createHazelcastInstance(Config config) {
        if (instanceFactory == null) {
            instanceFactory = new TestHazelcastFactory();
        }
        return instanceFactory.newHazelcastInstance(config);
    }

    protected HazelcastInstance createHazelcastInstance(Config config, Address[] blockedAddress) {
        if (instanceFactory == null) {
            instanceFactory = new TestHazelcastFactory();
        }
        return instanceFactory.newHazelcastInstance(config, blockedAddress);
    }

    protected HazelcastInstance[] createHazelcastInstances(int nodeCount) {
        return this.createHazelcastInstances(smallInstanceConfig(), nodeCount);
    }

    protected HazelcastInstance[] createHazelcastInstances(Config config, int nodeCount) {
        if (instanceFactory == null) {
            instanceFactory = new TestHazelcastFactory();
        }
        return instanceFactory.newInstances(config, nodeCount);
    }

    protected static <K, V> IMap<K, V> getMap(HazelcastInstance instance) {
        return instance.getMap(randomName());
    }

    protected static <E> IList<E> getList(HazelcastInstance instance) {
        return instance.getList(randomName());
    }

    protected static void fillListWithInts(IList<Integer> list, int count) {
        for (int i = 0; i < count; i++) {
            list.add(i);
        }
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

    public static void assertJobStatusEventually(Job job, @Nonnull JobStatus expected) {
        assertJobStatusEventually(job, expected, ASSERT_TRUE_EVENTUALLY_TIMEOUT);
    }

    public static Config smallInstanceWithResourceUploadConfig() {
        Config config = smallInstanceConfig();
        config.getJetConfig().setResourceUploadEnabled(true);
        return config;
    }

    public static Config defaultInstanceConfigWithJetEnabled() {
        Config config = new Config();
        config.getJetConfig().setEnabled(true);
        return config;
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
     * <p>
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
     *                           wait longer. If null, no execution ID is ignored.
     * @return the execution ID of the new execution or 0 if {@code
     * ignoredExecutionId == null}
     */
    public static long assertJobRunningEventually(HazelcastInstance instance, Job job, Long ignoredExecutionId) {
        Long executionId;
        JobExecutionService service = getJetServiceBackend(instance).getJobExecutionService();
        long nullSince = Long.MIN_VALUE;
        do {
            assertJobStatusEventually(job, RUNNING);
            // executionId can be null if the execution just terminated
            executionId = service.getExecutionIdForJobId(job.getId());
            if (executionId == null) {
                if (nullSince == Long.MIN_VALUE) {
                    nullSince = System.nanoTime();
                } else {
                    if (NANOSECONDS.toSeconds(System.nanoTime() - nullSince) > 10) {
                        // Because we check the execution ID, make sure the execution is running on
                        // the given instance. E.g. a job with a non-distributed source and no
                        // distributed edge will complete on all but one members immediately.
                        throw new RuntimeException("The executionId is null for 10 secs - is the job running on all members?");
                    }
                }
            } else {
                nullSince = Long.MIN_VALUE;
            }
        } while (executionId == null || executionId.equals(ignoredExecutionId));
        return executionId;
    }

    public static void assertJobStatusEventually(Job job, JobStatus expected, int timeoutSeconds) {
        assertNotNull(job);
        assertTrueEventually(() ->
                assertEquals("jobId=" + idToString(job.getId()), expected, job.getStatus()), timeoutSeconds);
    }

    public static JetServiceBackend getJetServiceBackend(HazelcastInstance instance) {
        return getNodeEngineImpl(instance).getService(SERVICE_NAME);
    }

    public static Address getAddress(HazelcastInstance instance) {
        return Accessors.getAddress(instance);
    }

    public static Node getNode(HazelcastInstance instance) {
        return Accessors.getNode(instance);
    }

    public static NodeEngineImpl getNodeEngineImpl(HazelcastInstance instance) {
        return Accessors.getNodeEngineImpl(instance);
    }

    public Address nextAddress() {
        return instanceFactory.nextAddress();
    }

    protected void terminateInstance(HazelcastInstance instance) {
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
     *                  one instance
     */
    public void cleanUpCluster(HazelcastInstance... instances) {
        for (Job job : instances[0].getJet().getJobs()) {
            ditchJob(job, instances);
        }
        for (DistributedObject o : instances[0].getDistributedObjects()) {
            o.destroy();
        }
    }

    /**
     * Give this method a job and it will ensure it's no longer running. It
     * will ignore if it's not running. If the cancellation fails, it will
     * retry.
     */
    public static void ditchJob(@Nonnull Job job, @Nonnull HazelcastInstance... instancesToShutDown) {
        int numAttempts;
        for (numAttempts = 0; numAttempts < 10; numAttempts++) {
            JobStatus status = null;
            try {
                status = job.getStatus();
                if (status == JobStatus.FAILED || status == JobStatus.COMPLETED) {
                    return;
                }
            } catch (JobNotFoundException e) {
                SUPPORT_LOGGER.fine("Job " + job.getIdString() + " is gone.");
                return;
            } catch (Exception e) {
                SUPPORT_LOGGER.warning("Failure to read job status: " + e, e);
            }

            Exception cancellationFailure;
            try {
                job.cancel();
                try {
                    job.join();
                } catch (JobNotFoundException e) {
                    SUPPORT_LOGGER.fine("Job " + job.getIdString() + " is gone.");
                    return;
                } catch (Exception ignored) {
                    // This can be CancellationException or any other job failure. We don't care,
                    // we're supposed to rid the cluster of the job and that's what we have.
                }
                return;
            } catch (JobNotFoundException e) {
                SUPPORT_LOGGER.fine("Job " + job.getIdString() + " is gone.");
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
            for (HazelcastInstance instance : instancesToShutDown) {
                instance.getLifecycleService().terminate();
            }
        } catch (Exception e) {
            // ignore, proceed to throwing RuntimeException
        }
        throw new RuntimeException(numAttempts + " attempts to cancel the job failed" +
                (instancesToShutDown.length > 0 ? ", shut down the cluster" : ""));
    }

    /**
     * Cancel the job and wait until it cancels using {@link Job#join()},
     * ignoring the CancellationException.
     */
    public static void cancelAndJoin(@Nonnull Job job) {
        job.cancel();
        try {
            job.join();
            fail("join didn't fail with CancellationException");
        } catch (CancellationException ignored) {
        }
    }

    public static <T> ProcessorMetaSupplier processorFromPipelineSource(BatchSource<T> source) {
        return ((BatchSourceTransform<T>) source).metaSupplier;
    }

    public static Job awaitSingleRunningJob(HazelcastInstance hz) {
        AtomicReference<Job> job = new AtomicReference<>();
        assertTrueEventually(() -> {
            List<Job> jobs = hz.getJet().getJobs().stream().filter(j -> j.getStatus() == RUNNING).collect(toList());
            assertEquals(1, jobs.size());
            job.set(jobs.get(0));
        });
        return job.get();
    }

    /**
     * Asserts that the {@code job} has an {@link ExecutionContext} on the
     * given {@code instance}.
     */
    public static void assertJobExecuting(Job job, HazelcastInstance instance) {
        ExecutionContext execCtx = getJetServiceBackend(instance).getJobExecutionService().getExecutionContext(job.getId());
        assertNotNull("Job should be executing on member " + instance + ", but is not", execCtx);
    }

    /**
     * Asserts that the {@code job} does not have an {@link ExecutionContext}
     * on the given {@code instance}.
     */
    public static void assertJobNotExecuting(Job job, HazelcastInstance instance) {
        ExecutionContext execCtx = getJetServiceBackend(instance).getJobExecutionService().getExecutionContext(job.getId());
        assertNull("Job should not be executing on member " + instance + ", but is", execCtx);
    }
}
