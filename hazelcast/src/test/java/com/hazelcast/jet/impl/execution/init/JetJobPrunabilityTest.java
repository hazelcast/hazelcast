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

package com.hazelcast.jet.impl.execution.init;

import com.hazelcast.cluster.Address;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.TestProcessors.CollectPerProcessorSink;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.Accessors;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.security.Permission;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.jet.config.JobConfigArguments.KEY_REQUIRED_PARTITIONS;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.ProcessorMetaSupplier.forceTotalParallelismOne;
import static java.util.Collections.singleton;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class JetJobPrunabilityTest extends SimpleTestInClusterSupport {
    private CollectPerProcessorSink consumerPms;
    private int localPartitionId;
    private int remotePartitionId;

    @BeforeClass
    public static void beforeClass() throws Exception {
        initialize(3, null);
    }

    @Before
    public void setUp() throws Exception {
        consumerPms = new CollectPerProcessorSink();
        NodeEngineImpl localNodeEngineImpl = getNodeEngineImpl(instance());
        NodeEngineImpl remoteNodeEngineImpl = getNodeEngineImpl(instances()[1]);
        Map<Address, int[]> ptAssignment = getPartitionAssignment(instance());
        localPartitionId = ptAssignment.get(localNodeEngineImpl.getThisAddress())[0];
        remotePartitionId = ptAssignment.get(remoteNodeEngineImpl.getThisAddress())[0];
    }

    @Test
    public void test_simpleDag_onlyCoordinator() {
        // Given
        int expectedTotalParallelism = 2;
        ProcessorMetaSupplier pmsGen = new ValidatingMetaSupplier(
                ProcessorMetaSupplier.of((ProcessorSupplier) count ->
                        IntStream.range(0, count)
                                .mapToObj(GenP::new)
                                .collect(Collectors.toList())),
                expectedTotalParallelism);


        DAG dag = new DAG();
        Vertex generator = dag.newVertex("Generator", pmsGen);
        Vertex printer = dag.newVertex("Consumer", consumerPms);
        dag.edge(between(generator, printer).distributeTo(localMemberAddress()).allToOne(""));

        var analysisResult = ExecutionPlanBuilder.analyzeDagForPartitionPruning(getNodeEngineImpl(instance()), dag);
        assertThat(analysisResult.allPartitionsRequired).isFalse();
        assertThat(analysisResult.constantPartitionIds).containsExactly(allToOnePartitionId());
        assertThat(analysisResult.requiredAddresses).containsExactly(localMemberAddress());

        JobConfig jobConfig = new JobConfig();
        jobConfig.setArgument(KEY_REQUIRED_PARTITIONS, singleton(localPartitionId));

        Job job = instance().getJet().newJob(dag, jobConfig);
        job.join();

        // should print 0 and 1.
        assertJobStatusEventually(job, JobStatus.COMPLETED);
        List<List<Object>> lists = consumerPms.getLists();
        List<List<Object>> nonEmptyRes = lists.stream().filter(l -> !l.isEmpty()).collect(Collectors.toList());

        // one of the processors should get all data
        assertThat(nonEmptyRes).isNotEmpty();
        assertThat(nonEmptyRes.size()).isOne();

        List<Object> results = nonEmptyRes.get(0);
        assertThat(results).containsExactlyInAnyOrder(0, 1);
    }

    @Test
    public void test_simpleDag_takesMemberAndCoordinator() {
        // Given
        int expectedTotalParallelism = 4;
        ProcessorMetaSupplier pmsGen = new ValidatingMetaSupplier(
                ProcessorMetaSupplier.of((ProcessorSupplier) count ->
                        IntStream.range(0, count)
                                .mapToObj(GenP::new)
                                .collect(Collectors.toList())),
                expectedTotalParallelism);

        DAG dag = new DAG();
        Vertex generator = dag.newVertex("Generator", pmsGen);
        Vertex printer = dag.newVertex("Printer", consumerPms);
        dag.edge(between(generator, printer).distributeTo(localMemberAddress()).allToOne(""));

        JobConfig jobConfig = new JobConfig();
        jobConfig.setArgument(KEY_REQUIRED_PARTITIONS, singleton(remotePartitionId));

        Job job = instance().getJet().newJob(dag, jobConfig);
        job.join();

        assertJobStatusEventually(job, JobStatus.COMPLETED);
        List<List<Object>> lists = consumerPms.getLists();
        List<List<Object>> nonEmptyRes = lists.stream().filter(l -> !l.isEmpty()).collect(Collectors.toList());

        // one of the processors should get all data
        assertThat(nonEmptyRes).isNotEmpty();
        assertThat(nonEmptyRes.size()).isOne();

        List<Object> results = nonEmptyRes.get(0);
        assertThat(results).containsExactlyInAnyOrder(0, 1, 1, 0);
    }

    @Test
    public void test_scanAndAgg() {
        // Given
        Address addr = getAddressForPartitionId(instance(), localPartitionId);

        ProcessorMetaSupplier pmsGen = ProcessorMetaSupplier.of((ProcessorSupplier) count ->
                IntStream.range(0, count).mapToObj(GenP::new).collect(Collectors.toList()));

        // Note: for SQL light jobs we need to use lazyForceTotalParallelismOne.
        ProcessorMetaSupplier pmsAgg = forceTotalParallelismOne(
                ProcessorSupplier.of(
                        Processors.aggregateP(AggregateOperations.counting())),
                addr);

        DAG dag = new DAG();
        Vertex generator = dag.newVertex("Generator", pmsGen);
        Vertex aggregator = dag.newVertex("Aggregator", pmsAgg);
        Vertex printer = dag.newVertex("Printer", consumerPms);

        // generator -> aggregator
        final int partitionId = localPartitionId;
        dag.edge(between(generator, aggregator)
                .distributeTo(addr)
                .partitioned(i -> partitionId));

        // aggregator -> printer
        dag.edge(between(aggregator, printer).isolated());

        var analysisResult = ExecutionPlanBuilder.analyzeDagForPartitionPruning(getNodeEngineImpl(instance()), dag);
        assertThat(analysisResult.allPartitionsRequired).isTrue();
        assertThat(analysisResult.constantPartitionIds).isEmpty();
        assertThat(analysisResult.requiredAddresses).containsExactly(addr);

        JobConfig jobConfig = new JobConfig();
        jobConfig.setArgument(KEY_REQUIRED_PARTITIONS, singleton(localPartitionId));

        Job job = instance().getJet().newJob(dag, jobConfig);
        job.join();

        // should print 2.
        assertJobStatusEventually(job, JobStatus.COMPLETED);
        List<List<Object>> lists = consumerPms.getLists();
        assertContainsAll(lists.get(0), List.of(2L));
    }

    @Test
    public void test_dagWithBranching() {
        // Given
        ProcessorMetaSupplier pmsGen1 = ProcessorMetaSupplier.of((ProcessorSupplier) count ->
                IntStream.range(0, count)
                        .mapToObj(GenP::new)
                        .collect(Collectors.toList()));

        ProcessorMetaSupplier pmsGen2 = ProcessorMetaSupplier.of((ProcessorSupplier) count ->
                IntStream.range(0, count)
                        .mapToObj(GenP::new)
                        .collect(Collectors.toList()));

        DAG dag = new DAG();
        Vertex generatorLeft = dag.newVertex("Generator-Left", pmsGen1);
        Vertex generatorRight = dag.newVertex("Generator-Right", pmsGen2);
        Vertex consumer = dag.newVertex("Consumer", consumerPms);

        dag.edge(Edge.from(generatorLeft).to(consumer, 0).isolated());
        dag.edge(Edge.from(generatorRight).to(consumer, 1).distributed().broadcast());

        var analysisResult = ExecutionPlanBuilder.analyzeDagForPartitionPruning(getNodeEngineImpl(instance()), dag);
        assertThat(analysisResult.allPartitionsRequired).isFalse();
        assertThat(analysisResult.constantPartitionIds).isEmpty();
        assertThat(analysisResult.requiredAddresses).isEmpty();

        JobConfig jobConfig = new JobConfig();
        jobConfig.setArgument(KEY_REQUIRED_PARTITIONS, singleton(localPartitionId));

        Job job = instance().getJet().newJob(dag, jobConfig);
        job.join();

        assertJobStatusEventually(job, JobStatus.COMPLETED);
        List<List<Object>> lists = consumerPms.getLists();
        Set<Object> containerList = new TreeSet<>();
        containerList.addAll(lists.get(0));
        containerList.addAll(lists.get(1));
        assertEquals(containerList, Set.of(0, 1));
    }

    @Nonnull
    private static Address localMemberAddress() {
        return Accessors.getAddress(instance());
    }

    private static int allToOnePartitionId() {
        return instance().getPartitionService().getPartition("").getPartitionId();
    }

    private static class ValidatingMetaSupplier implements ProcessorMetaSupplier {
        private final ProcessorMetaSupplier wrappingPms;
        private final int expectedTotalParallelism;

        private ValidatingMetaSupplier(ProcessorMetaSupplier wrappingPms, int expectedTotalParallelism) {
            this.wrappingPms = wrappingPms;
            this.expectedTotalParallelism = expectedTotalParallelism;
        }

        @Override
        public void init(@NotNull Context context) throws Exception {
            assertEquals(expectedTotalParallelism, context.totalParallelism());
        }

        @Nullable
        @Override
        public Permission getRequiredPermission() {
            return wrappingPms.getRequiredPermission();
        }

        @NotNull
        @Override
        public Map<String, String> getTags() {
            return wrappingPms.getTags();
        }

        @Override
        public int preferredLocalParallelism() {
            return wrappingPms.preferredLocalParallelism();
        }

        @Override
        public boolean initIsCooperative() {
            return wrappingPms.initIsCooperative();
        }

        @Override
        public Function<? super Address, ? extends ProcessorSupplier> get(@NotNull List<Address> addresses) {
            return wrappingPms.get(addresses);
        }

        @Override
        public boolean closeIsCooperative() {
            return wrappingPms.closeIsCooperative();
        }

        @Override
        public void close(@Nullable Throwable error) throws Exception {
            wrappingPms.close(error);
        }

        @Override
        public boolean isReusable() {
            return wrappingPms.isReusable();
        }
    }

    private static class GenP extends AbstractProcessor {
        private final int item;

        GenP(int item) {
            this.item = item;
        }

        @Override
        public boolean complete() {
            return tryEmit(item);
        }

        @Override
        public boolean closeIsCooperative() {
            return true;
        }
    }
}
