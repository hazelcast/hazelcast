package com.hazelcast.jet.impl.util;

import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.impl.AbstractJobProxy;
import com.hazelcast.jet.impl.JetServiceBackend;
import com.hazelcast.jet.impl.JobRecord;
import com.hazelcast.jet.impl.JobResult;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.jet.config.JobConfigArguments.KEY_REQUIRED_PARTITIONS;
import static com.hazelcast.jet.core.Edge.between;
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

    @Before
    public void setUp() throws Exception {
//        Map<Address, int[]> addressMap = getPartitionAssignment(instance());
        pmsA = memberPruningMetaSupplier(count -> IntStream.range(0, count)
                .mapToObj(GenP::new)
                .collect(Collectors.toList()));
        pmsB = memberPruningMetaSupplier((ProcessorSupplier) count -> nCopies(count, new PrintP()));
    }

    @Test
    public void test() {
        // a --> b

        // Given
        DAG dag = new DAG();
        Vertex a = dag.newVertex("a", pmsA);
        Vertex b = dag.newVertex("b", pmsB);
        dag.edge(between(a, b));
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
