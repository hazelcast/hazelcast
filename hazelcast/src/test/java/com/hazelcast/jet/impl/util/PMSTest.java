package com.hazelcast.jet.impl.util;

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
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.datamodel.Tuple2;
import org.jetbrains.annotations.NotNull;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.jet.config.JobConfigArguments.KEY_REQUIRED_PARTITIONS;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.ProcessorMetaSupplier.forceTotalParallelismOne;
import static com.hazelcast.jet.core.ProcessorMetaSupplier.memberPruningMetaSupplier;
import static java.util.Collections.nCopies;
import static java.util.Collections.singleton;

public class PMSTest extends SimpleTestInClusterSupport {
    @BeforeClass
    public static void beforeClass() throws Exception {
        initialize(3, null);
    }

    @Test
    public void test_simpleDag() {
        // Given
        ProcessorMetaSupplier pmsGen = memberPruningMetaSupplier((ProcessorSupplier) count ->
                IntStream.range(0, count)
                        .mapToObj(GenP::new)
                        .collect(Collectors.toList()));

        ProcessorMetaSupplier pmsPrint = memberPruningMetaSupplier(
                (ProcessorSupplier) count -> nCopies(count, new PrintP()));

        DAG dag = new DAG();
        Vertex generator = dag.newVertex("Generator", pmsGen);
        Vertex printer = dag.newVertex("Printer", pmsPrint);
        dag.edge(between(generator, printer));
        dag.markAsPrunable();

        JobConfig jobConfig = new JobConfig();
        jobConfig.setArgument(KEY_REQUIRED_PARTITIONS, singleton(1));

        Job job = instance().getJet().newJob(dag, jobConfig);
        job.join();

        assertJobStatusEventually(job, JobStatus.COMPLETED);
    }

    @Test
    public void test_scanAndAgg() {
        // Given
        final int partitionId = 1;
        Address addr = getAddressForPartitionId(instance(), partitionId);

        ProcessorMetaSupplier pmsGen = memberPruningMetaSupplier((ProcessorSupplier) count ->
                IntStream.range(0, count).mapToObj(GenP::new).collect(Collectors.toList()));

        ProcessorMetaSupplier pmsAgg = forceTotalParallelismOne(
                ProcessorSupplier.of(
                        Processors.aggregateP(AggregateOperations.counting())),
                addr,
                false);

        ProcessorMetaSupplier pmsPrint = memberPruningMetaSupplier(
                (ProcessorSupplier) count -> nCopies(count, new PrintP()));

        DAG dag = new DAG();
        Vertex generator = dag.newVertex("Generator", pmsGen);
        Vertex aggregator = dag.newVertex("Aggregator", pmsAgg);
        Vertex printer = dag.newVertex("Printer", pmsPrint);

        // generator -> aggregator
        dag.edge(between(generator, aggregator)
                .distributeTo(addr)
                .partitioned(i -> partitionId));

        // aggregator -> printer
        dag.edge(between(aggregator, printer).isolated());

        dag.markAsPrunable();


        JobConfig jobConfig = new JobConfig();
        jobConfig.setArgument(KEY_REQUIRED_PARTITIONS, singleton(1));

        Job job = instance().getJet().newJob(dag, jobConfig);
        job.join();

        // should print 2.
        assertJobStatusEventually(job, JobStatus.COMPLETED);
    }

    @Test
    public void test_scanJoin() {
        // Given
        ProcessorMetaSupplier pmsGen1 = memberPruningMetaSupplier((ProcessorSupplier) count ->
                IntStream.range(0, count)
                        .mapToObj(GenP::new)
                        .collect(Collectors.toList()));

        ProcessorMetaSupplier pmsGen2 = memberPruningMetaSupplier((ProcessorSupplier) count ->
                IntStream.range(0, count)
                        .mapToObj(GenP::new)
                        .collect(Collectors.toList()));

        ProcessorMetaSupplier pmsJoin = ProcessorMetaSupplier.of(
                (ProcessorSupplier) count -> nCopies(count, new JoinP()));

        ProcessorMetaSupplier pmsPrint = memberPruningMetaSupplier(
                (ProcessorSupplier) count -> nCopies(count, new PrintP()));

        DAG dag = new DAG();
        Vertex generatorLeft = dag.newVertex("Generator-Left", pmsGen1);
        Vertex generatorRight = dag.newVertex("Generator-Right", pmsGen2);
        Vertex joiner = dag.newVertex("Joiner", pmsJoin);
        Vertex printer = dag.newVertex("Printer", pmsPrint);

        dag.edge(Edge.from(generatorLeft).to(joiner, 0).isolated());
        dag.edge(Edge.from(generatorRight).to(joiner, 1).distributed().broadcast());
        dag.edge(between(joiner, printer).isolated());
        dag.markAsPrunable();

        JobConfig jobConfig = new JobConfig();
        jobConfig.setArgument(KEY_REQUIRED_PARTITIONS, singleton(1));

        Job job = instance().getJet().newJob(dag, jobConfig);
        job.join();

        // Prints
        // (0, 0)
        // (1, 0)
        // (0, 1)
        // (1, 1)
        assertJobStatusEventually(job, JobStatus.COMPLETED);
    }

    @Test
    public void test_memberPruningWithPMS() {
        // Given

        ProcessorMetaSupplier pmsGen = ProcessorMetaSupplier.of(2, (ProcessorSupplier) count ->
                IntStream.range(0, count)
                        .mapToObj(GenP::new)
                        .collect(Collectors.toList()));

        ProcessorMetaSupplier pruningPmsGen = memberPruningMetaSupplier(pmsGen);

        ProcessorMetaSupplier pmsPrint = memberPruningMetaSupplier(
                (ProcessorSupplier) count -> nCopies(count, new PrintP()));

        DAG dag = new DAG();
        Vertex generator = dag.newVertex("Generator", pruningPmsGen);
        Vertex printer = dag.newVertex("Printer", pmsPrint);
        dag.edge(between(generator, printer));
        dag.markAsPrunable();

        JobConfig jobConfig = new JobConfig();
        jobConfig.setArgument(KEY_REQUIRED_PARTITIONS, singleton(1));

        Job job = instance().getJet().newJob(dag, jobConfig);
        job.join();

        assertJobStatusEventually(job, JobStatus.COMPLETED);
    }

    private static class GenP extends AbstractProcessor {
        private final int item;

        public GenP(int item) {
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

    private static class PrintP extends AbstractProcessor {
        @Override
        protected boolean tryProcess0(@NotNull Object item) {
            System.err.println(item);
            return complete();
        }

        @Override
        public boolean closeIsCooperative() {
            return true;
        }
    }

    private static class JoinP extends AbstractProcessor {
        protected Integer leftItem = null;
        protected Integer rightItem = null;

        @Override
        protected boolean tryProcess0(@NotNull Object item) {
            if (rightItem == null) {
                leftItem = (Integer) item;
                return true;
            } else {
                Integer rItem = rightItem;
                rightItem = null;
                return tryEmit(Tuple2.tuple2(item, rItem));
            }
        }

        @Override
        protected boolean tryProcess1(@NotNull Object item) {
            if (leftItem == null) {
                rightItem = (Integer) item;
                return true;
            } else {
                Integer lItem = leftItem;
                leftItem = null;
                return tryEmit(Tuple2.tuple2(lItem, item));
            }
        }

        @Override
        public boolean closeIsCooperative() {
            return true;
        }
    }
}
