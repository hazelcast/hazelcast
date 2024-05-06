/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl;

import com.google.common.collect.Collections2;
import com.google.common.collect.Sets;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetMemberSelector;
import com.hazelcast.jet.JetService.JobBuilder;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.JobStateSnapshot;
import com.hazelcast.jet.JobStatusListener;
import com.hazelcast.jet.config.DeltaJobConfig;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.core.JobSuspensionCause;
import com.hazelcast.jet.core.TestProcessors.MockP;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.metrics.JobMetrics;
import com.hazelcast.jet.impl.pipeline.PipelineImpl;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.logging.ILogger;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.security.auth.Subject;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category(QuickTest.class)
public class JobBuilderTest {

    @SuppressWarnings("LeftCurly")
    public enum BuilderSupplier implements Supplier<JobBuilder>, Predicate<MockJobProxy> {
        Dag {
            public JobBuilder get() { return newJobBuilderFromDag(); }
            public boolean test(MockJobProxy job) { return job.jobDefinition instanceof DAG; }
        },
        Pipeline {
            public JobBuilder get() { return newJobBuilderFromPipeline(); }
            public boolean test(MockJobProxy job) { return job.jobDefinition instanceof Pipeline; }
        }
    }

    @SuppressWarnings("LeftCurly")
    public enum BuilderParameter implements UnaryOperator<JobBuilder>, Predicate<MockJobProxy> {
        Config {
            public JobBuilder apply(JobBuilder b) { return b.withConfig(new JobConfig().setMaxProcessorAccumulatedRecords(19)); }
            public boolean test(MockJobProxy job) { return job.getConfig().getMaxProcessorAccumulatedRecords() == 19; }
        },
        MemberSelector {
            public JobBuilder apply(JobBuilder b) { return b.withMemberSelector(JetMemberSelector.ALL_LITE_MEMBERS); }
            public boolean test(MockJobProxy job) { return job.getMemberSelector() != null; }
        },
        LightJob {
            public JobBuilder apply(JobBuilder b) { return b.asLightJob(); }
            public boolean test(MockJobProxy job) { return job.isLightJob(); }
        }
    }

    @Parameters(name = "{0}")
    public static List<Object[]> parameters() {
        return cartesianProduct(" -> ",
                Arrays.stream(BuilderSupplier.values()).collect(toMap(c -> "with" + c, c -> c)),
                pathCoverage(" -> ",
                        Arrays.stream(BuilderParameter.values()).map(p -> Map.entry("with" + p, p))
                )
        );
    }

    @Parameter(0)
    public String description;
    @Parameter(1)
    public BuilderSupplier builderSupplier;
    @Parameter(2)
    public List<BuilderParameter> parameters;

    @SuppressWarnings("rawtypes")
    private static AbstractJetInstance jet;

    @BeforeClass
    public static void setup() {
        jet = new MockJetService(mock(HazelcastInstance.class));
    }

    @Test
    public void test_startJob() {
        test(false);
    }

    @Test
    public void test_startJobIfAbsent() {
        test(true);
    }

    private void test(boolean ifAbsent) {
        JobBuilder builder = builderSupplier.get();
        parameters.forEach(param -> param.apply(builder));
        MockJobProxy job;
        if (ifAbsent) {
            if (parameters.contains(BuilderParameter.LightJob)) {
                assertThatThrownBy(builder::startIfAbsent).isInstanceOf(UnsupportedOperationException.class);
                return;
            }
            job = (MockJobProxy) builder.startIfAbsent();
        } else {
            job = (MockJobProxy) builder.start();
        }

        assertTrue(builderSupplier.test(job));
        EnumSet.allOf(BuilderParameter.class).forEach(param ->
                assertTrue(param.test(job) == parameters.contains(param)));
    }

    private static JobBuilder newJobBuilderFromDag() {
        DAG dag = new DAG().vertex(new Vertex("test", () -> new MockP().streaming()));
        return jet.newJobBuilder(dag);
    }

    private static JobBuilder newJobBuilderFromPipeline() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.itemStream(1))
                .withoutTimestamps().writeTo(Sinks.noop());
        return jet.newJobBuilder(pipeline);
    }

    private static <T> Map<String, List<T>> pathCoverage(String separator, Stream<Entry<String, T>> parameters) {
        return Sets.powerSet(parameters.collect(toSet())).stream()
                .flatMap(arguments ->
                        Collections2.permutations(arguments).stream().map(pairs ->
                                Map.entry(
                                        pairs.stream().map(Entry::getKey).collect(joining(separator)),
                                        pairs.stream().map(Entry::getValue).toList()
                                )))
                .collect(toMap(Entry::getKey, Entry::getValue));
    }

    @SafeVarargs
    private static List<Object[]> cartesianProduct(String separator, Map<String, ?>... arguments) {
        return Sets.cartesianProduct(Arrays.stream(arguments).map(Map::entrySet).toList()).stream()
                .map(pairs ->
                        Stream.concat(
                                Stream.of(pairs.stream().map(Entry::getKey)
                                                        .filter(key -> !key.isEmpty())
                                                        .collect(joining(separator))),
                                pairs.stream().map(Entry::getValue)
                        ).toArray())
                .toList();
    }

    private static RuntimeException uoe() {
        return new UnsupportedOperationException();
    }

    @SuppressWarnings("LeftCurly")
    private record MockJobProxy(long jobId, boolean isLightJob, Object jobDefinition, JobConfig config, Subject subject)
            implements Job {

        @Override
        public long getId() {
            return jobId;
        }

        @Override
        public boolean isLightJob() {
            return isLightJob;
        }

        @Nonnull @Override
        public JobConfig getConfig() {
            return config;
        }

        @Nullable @Override
        public String getName() {
            return config.getName();
        }

        @Nullable
        public JetMemberSelector getMemberSelector() {
            return jobDefinition instanceof DAG dag
                    ? dag.memberSelector()
                    : ((PipelineImpl) jobDefinition).memberSelector();
        }

        @Nonnull public CompletableFuture<Void> getFuture() { throw uoe(); }
        public void cancel() { throw uoe(); }
        public long getSubmissionTime() { throw uoe(); }
        @Nonnull public JobStatus getStatus() { throw uoe(); }
        public boolean isUserCancelled() { throw uoe(); }
        public UUID addStatusListener(@Nonnull JobStatusListener listener) { throw uoe(); }
        public boolean removeStatusListener(@Nonnull UUID id) { throw uoe(); }
        public JobConfig updateConfig(@Nonnull DeltaJobConfig deltaConfig) { throw uoe(); }
        @Nonnull public JobSuspensionCause getSuspensionCause() { throw uoe(); }
        @Nonnull public JobMetrics getMetrics() { throw uoe(); }
        public void restart() { throw uoe(); }
        public void suspend() { throw uoe(); }
        public void resume() { throw uoe(); }
        public JobStateSnapshot cancelAndExportSnapshot(String name) { throw uoe(); }
        public JobStateSnapshot exportSnapshot(String name) { throw uoe(); }
    }

    @SuppressWarnings({"LeftCurly", "rawtypes"})
    private static class MockJetService extends AbstractJetInstance {
        AtomicInteger jobId = new AtomicInteger();

        MockJetService(HazelcastInstance hazelcastInstance) {
            super(hazelcastInstance);
        }

        @Override
        public long newJobId() {
            return jobId.incrementAndGet();
        }

        @Override
        public Job newJobProxy(long jobId, boolean isLightJob, @Nonnull Object jobDefinition,
                               @Nonnull JobConfig config, @Nullable Subject subject) {
            return new MockJobProxy(jobId, isLightJob, jobDefinition, config, subject);
        }

        public Job newJobProxy(long jobId, Object lightJobCoordinator) { throw uoe(); }
        public boolean existsDistributedObject(@Nonnull String serviceName, @Nonnull String objectName) { throw uoe(); }
        public ILogger getLogger() { throw uoe(); }
        public Map getJobsInt(String onlyName, Long onlyJobId) { throw uoe(); }
        public Object getMasterId() { throw uoe(); }
        @Nonnull public JetConfig getConfig() { throw uoe(); }
    }
}
