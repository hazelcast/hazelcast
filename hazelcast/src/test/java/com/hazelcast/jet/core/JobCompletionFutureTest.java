/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.BiConsumerEx;
import com.hazelcast.function.ConsumerEx;
import com.hazelcast.instance.impl.DefaultNodeExtension;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeContext;
import com.hazelcast.instance.impl.NodeExtension;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.TestProcessors.MockPS;
import com.hazelcast.jet.core.TestProcessors.NoOutputSourceP;
import com.hazelcast.jet.impl.JetServiceBackend;
import com.hazelcast.jet.impl.JobCoordinationService;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.impl.MasterContext;
import com.hazelcast.jet.impl.exception.CancellationByUserException;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.TestHazelcastInstanceFactory.DelegatingNodeContext;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.function.Supplier;

import static com.hazelcast.jet.core.JobStatus.COMPLETED;
import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.core.JobStatus.SUSPENDED;
import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JobCompletionFutureTest extends JetTestSupport {
    private static final int NODE_COUNT = 2;
    private static final int LOCAL_PARALLELISM = 1;
    private static final int TOTAL_PARALLELISM = NODE_COUNT * LOCAL_PARALLELISM;

    private static TestHazelcastFactory factory;
    private static final HazelcastInstance[] instances = new HazelcastInstance[NODE_COUNT];
    private static HazelcastInstance client;

    @Parameters(name = "{0}")
    public static Iterable<Object[]> parameters() {
        return parameters(Map.of("useLightJob", true, "useNormalJob", false),
                Map.<String, Supplier<HazelcastInstance>>of(
                        "fromNonMaster", () -> instances[1],
                        "fromClient", () -> client));
    }

    @Parameter(0)
    public String description;

    @Parameter(1)
    public boolean useLightJob;

    @Parameter(2)
    public Supplier<HazelcastInstance> instance;

    private static final Map<String, ConsumerEx<Job>> methods = Map.of(
            "getStatus()", Job::getStatus,
            "getFuture()", Job::getFuture,
            "join()", Job::join,
            "async join()", job -> {
                CompletableFuture<CompletableFuture<Void>> future = future("joinJob");
                spawn(job::join);
                future.join();
            },
            "suspend()", Job::suspend,
            "resume()", Job::resume,
            "restart()", Job::restart,
            "cancel()", Job::cancel,
            "isUserCancelled()", job -> {
                try {
                    job.isUserCancelled();
                } catch (IllegalStateException ignored) { }
            });

    /** Triggers that require special treatment, such as {@code cancel()} are handled separately. */
    private static final List<String> triggers = asList(
            "getStatus()", "getFuture()", "async join()", "suspend()", "resume()", "restart()", "isUserCancelled()");
    private static final List<String> throwers = asList(
            "getStatus()", "getFuture()", "join()", "suspend()", "resume()", "restart()", "cancel()", "isUserCancelled()");

    @BeforeClass
    public static void setup() {
        factory = new TestHazelcastFactory(2);
        factory.delegateNodeContext(MockNodeContext::new);

        Config config = smallInstanceConfig();
        config.getJetConfig().setCooperativeThreadCount(LOCAL_PARALLELISM);

        for (int i = 0; i < NODE_COUNT; i++) {
            instances[i] = factory.newHazelcastInstance(config);
        }
        client = factory.newHazelcastClient();
    }

    @AfterClass
    public static void destroy() {
        factory.shutdownAll();
    }

    @Test
    public void when_completionFutureNotJoined_then_jobNotFound() throws InterruptedException {
        List<String> applicableThrowers = new ArrayList<>(throwers);
        if (useLightJob) {
            applicableThrowers.removeAll(asList("suspend()", "resume()", "restart()"));
        }
        for (int i = 0; i < applicableThrowers.size(); i++) {
            List<String> names = new ArrayList<>(applicableThrowers);
            names.add(0, names.remove(i));

            testTrackedJob((job, trackedJob) -> {
                NoOutputSourceP.proceedLatch.countDown();
                job.join();
                for (int j = 0; j < names.size(); j++) {
                    String name = names.get(j);
                    assertThatThrownBy(() -> methods.get(name).accept(trackedJob),
                                    String.join(" -> ", names.subList(0, j + 1)))
                            .matches(t -> peel(t) instanceof JobNotFoundException);
                }
            });
        }
    }

    @Test
    public void test_jobCompletedDuringJoining() throws InterruptedException {
        testTrackedJob((job, trackedJob) -> {
            Semaphore blockStatus = block("getJobStatus");
            CompletableFuture<CompletableFuture<Void>> future = future("joinJob");
            CompletableFuture<CompletableFuture<JobStatus>> status = future("getJobStatus");

            Future<CompletableFuture<Void>> trackedFuture = spawn(trackedJob::getFuture);
            assertFalse(future.get().isCompletedExceptionally());
            NoOutputSourceP.proceedLatch.countDown();
            job.join();

            blockStatus.release();
            assertThatThrownBy(() -> status.get().get()).hasCauseInstanceOf(JobNotFoundException.class);
            assertTrueAllTheTime(() -> assertThatCode(() -> trackedFuture.get().get()).doesNotThrowAnyException(), 2);
            assertCompletion(trackedJob);
        });
    }

    @Test
    public void test_waitForCompletion() throws InterruptedException {
        for (String name : triggers) {
            assumeFalse(useLightJob && asList("suspend()", "resume()", "restart()").contains(name));
            System.out.println(">> Will call " + name + " to join future and then wait for completion");
            testTrackedJob((job, trackedJob) -> {
                methods.get(name).accept(trackedJob);
                if (name.equals("suspend()")) {
                    assertJobStatusEventually(job, SUSPENDED);
                    job.resume();
                }
                NoOutputSourceP.proceedLatch.countDown();
                job.join();
                assertCompletion(trackedJob);
            });
        }
    }

    @Test
    public void test_cancelJob() throws InterruptedException {
        for (String name : triggers) {
            assumeFalse(useLightJob && asList("suspend()", "resume()", "restart()").contains(name));
            System.out.println(">> Will call " + name + " to join future and then cancel job");
            testTrackedJob((job, trackedJob) -> {
                NoOutputSourceP.executionStarted = new CountDownLatch(1);
                methods.get(name).accept(trackedJob);
                if (name.equals("suspend()")) {
                    assertJobStatusEventually(job, SUSPENDED);
                } else if (name.equals("restart()")) {
                    NoOutputSourceP.executionStarted.await();
                }
                cancelAndJoin(job);
                assertCancellation(trackedJob);
            });
        }
    }

    @Test
    public void test_cancelTrackedJob() throws InterruptedException {
        System.out.println(">> Will call cancel() to join future and then join job");
        testTrackedJob((job, trackedJob) -> {
            trackedJob.cancel();
            assertThatThrownBy(job::join).isInstanceOf(CancellationByUserException.class);
            assertCancellation(trackedJob);
        });
    }

    private void testTrackedJob(BiConsumerEx<Job, Job> test) throws InterruptedException {
        TestProcessors.reset(TOTAL_PARALLELISM);

        DAG dag = new DAG().vertex(new Vertex("v", new MockPS(NoOutputSourceP::new, NODE_COUNT)));
        Job job = useLightJob ? instances[0].getJet().newLightJob(dag) : instances[0].getJet().newJob(dag);

        NoOutputSourceP.executionStarted.await();
        Job trackedJob = assertJobVisibleEventually(instance.get(), job);

        test.accept(job, trackedJob);
    }

    private static void assertCompletion(Job job) {
        assertJobStatusEventually(job, COMPLETED);
        assertThatCode(() -> job.getFuture().get()).doesNotThrowAnyException();
        assertThatCode(job::join).doesNotThrowAnyException();
        assertFalse(job.isUserCancelled());
    }

    private static void assertCancellation(Job job) {
        assertJobStatusEventually(job, FAILED);
        assertThatThrownBy(() -> job.getFuture().get()).isInstanceOf(CancellationByUserException.class);
        assertThatThrownBy(job::join).isInstanceOf(CancellationByUserException.class);
        assertTrue(job.isUserCancelled());
    }

    private static final Map<String, CompletableFuture<?>> futures = new HashMap<>();
    private static final Map<String, Semaphore> semaphores = new HashMap<>();

    private static <T> CompletableFuture<T> future(String key) {
        CompletableFuture<T> future = new CompletableFuture<>();
        futures.put(key, future);
        return future;
    }

    @SuppressWarnings("unchecked")
    private static <T> T resolve(String key, Supplier<T> operation) {
        CompletableFuture<T> future = (CompletableFuture<T>) futures.remove(key);
        if (future != null) {
            try {
                future.complete(operation.get());
            } catch (Exception e) {
                future.completeExceptionally(e);
            }
            return future.getNow(null);
        }
        return operation.get();
    }

    private static Semaphore block(String key) {
        Semaphore semaphore = new Semaphore(0, true);
        semaphores.put(key, semaphore);
        return semaphore;
    }

    private static void waitIfBlocked(String key) {
        Semaphore semaphore = semaphores.remove(key);
        if (semaphore != null) {
            try {
                semaphore.acquire();
            } catch (InterruptedException ignored) { }
        }
    }

    private static class MockJobCoordinationService extends JobCoordinationService {
        MockJobCoordinationService(NodeEngineImpl nodeEngine, JetServiceBackend jetServiceBackend,
                                   JetConfig config, JobRepository jobRepository) {
            super(nodeEngine, jetServiceBackend, config, jobRepository);
        }

        @Override
        public CompletableFuture<Void> joinLightJob(long jobId) {
            return resolve("joinJob", () -> super.joinLightJob(jobId));
        }

        @Override
        public CompletableFuture<Void> joinSubmittedJob(long jobId) {
            return resolve("joinJob", () -> super.joinSubmittedJob(jobId));
        }

        @Override
        public CompletableFuture<JobStatus> getLightJobStatus(long jobId) {
            waitIfBlocked("getJobStatus");
            return resolve("getJobStatus", () -> super.getLightJobStatus(jobId));
        }

        @Override
        public CompletableFuture<JobStatus> getJobStatus(long jobId) {
            waitIfBlocked("getJobStatus");
            return resolve("getJobStatus", () -> super.getJobStatus(jobId));
        }

        @Override
        protected CompletableFuture<Void> completeJob(MasterContext masterContext, Throwable error,
                                                      long completionTime, boolean userCancelled) {
            return super.completeJob(masterContext, error, completionTime, userCancelled)
                    .whenComplete((r, t) -> jobRepository().jobResultsMap().remove(masterContext.jobId()));
        }
    }

    private static class MockJetServiceBackend extends JetServiceBackend {
        MockJetServiceBackend(Node node) {
            super(node);
        }

        @Override
        protected JobCoordinationService createJobCoordinationService() {
            return new MockJobCoordinationService(getNodeEngine(), this, getJetConfig(), getJobRepository());
        }
    }

    private static class MockNodeContext extends DelegatingNodeContext {
        MockNodeContext(NodeContext delegate) {
            super(delegate);
        }

        @Override
        public NodeExtension createNodeExtension(Node node) {
            return new DefaultNodeExtension(node) {
                @Override
                @SuppressWarnings("unchecked")
                public <T> T createService(Class<T> clazz, Object... params) {
                    if (JetServiceBackend.class.isAssignableFrom(clazz)) {
                        return (T) new MockJetServiceBackend(node);
                    }
                    return super.createService(clazz, params);
                }
            };
        }
    }
}
