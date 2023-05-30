package com.hazelcast.jet.impl.util;

import com.hazelcast.cluster.Address;
import com.hazelcast.function.Functions;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.Processors;
import org.jetbrains.annotations.NotNull;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.jet.config.JobConfigArguments.KEY_REQUIRED_PARTITIONS;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.ProcessorMetaSupplier.forceTotalParallelismOne;
import static com.hazelcast.jet.core.ProcessorMetaSupplier.memberPruningMetaSupplier;
import static java.util.Collections.nCopies;
import static java.util.Collections.singleton;

public class PMSTest extends SimpleTestInClusterSupport {
    private ProcessorMetaSupplier pmsA;
    private ProcessorMetaSupplier pmsB;

    @BeforeClass
    public static void beforeClass() throws Exception {
        initialize(3, null);
    }

    @Test
    public void test_simpleDAG() {
        // Given
        ProcessorMetaSupplier pmsGen = memberPruningMetaSupplier(count -> IntStream.range(0, count)
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
    }

    @Test
    public void test_scanAndAgg() {
        // Given
        final int partitionId = 1;
        ProcessorMetaSupplier pmsGen = memberPruningMetaSupplier((ProcessorSupplier) count ->
                IntStream.range(0, count).mapToObj(GenP::new).collect(Collectors.toList()));

        ProcessorMetaSupplier pmsAgg = forceTotalParallelismOne(
                ProcessorSupplier.of(Processors.aggregateP(AggregateOperations.counting())));

        ProcessorMetaSupplier pmsPrint = memberPruningMetaSupplier(
                (ProcessorSupplier) count -> nCopies(count, new PrintP()));

        Address addr = getAddressForPartitionId(instance(), partitionId);

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
}
