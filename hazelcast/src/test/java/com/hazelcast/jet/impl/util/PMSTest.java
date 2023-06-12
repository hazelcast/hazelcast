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
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.security.Permission;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Scanner;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.jet.config.JobConfigArguments.KEY_REQUIRED_PARTITIONS;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.ProcessorMetaSupplier.forceTotalParallelismOne;
import static java.util.Collections.nCopies;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PMSTest extends SimpleTestInClusterSupport {
    private static final String OUTPUT_FILE = "output.txt";
    private File output;
    private Scanner scanner;

    @BeforeClass
    public static void beforeClass() throws Exception {
        initialize(3, null);
    }

    @Before
    public void setUp() throws Exception {
        output = new File(OUTPUT_FILE);
        assertTrue(output.createNewFile());
        assertTrue(output.exists());
        try {
            scanner = new Scanner(Path.of(OUTPUT_FILE));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @After
    public void tearDown() throws Exception {
        File output = new File(OUTPUT_FILE);
        assertTrue(output.exists());
        assertTrue(output.delete());
    }

    @Test
    public void test_simpleDag() {
        // Given
        int expectedTotalParallelism = 2;
        ProcessorMetaSupplier pmsGen = new ValidatingMetaSupplier(
                ProcessorMetaSupplier.of((ProcessorSupplier) count ->
                        IntStream.range(0, count)
                                .mapToObj(GenP::new)
                                .collect(Collectors.toList())),
                expectedTotalParallelism);

        ProcessorMetaSupplier pmsPrint = ProcessorMetaSupplier.of(
                (ProcessorSupplier) count -> nCopies(count, new PrintP()));

        DAG dag = new DAG();
        Vertex generator = dag.newVertex("Generator", pmsGen);
        Vertex printer = dag.newVertex("Printer", pmsPrint);
        dag.edge(between(generator, printer));

        JobConfig jobConfig = new JobConfig();
        jobConfig.setArgument(KEY_REQUIRED_PARTITIONS, singleton(1));

        Job job = instance().getJet().newJob(dag, jobConfig);
        job.join();

        // should print 0 and 1.
        assertJobStatusEventually(job, JobStatus.COMPLETED);

        Assert.assertEquals("0", scanner.nextLine());
        Assert.assertEquals("1", scanner.nextLine());
        scanner.close();
    }

    @Test
    public void test_simpleDag_takesTwoMembers() {
        // Given
        int expectedTotalParallelism = 4;
        ProcessorMetaSupplier pmsGen = new ValidatingMetaSupplier(
                ProcessorMetaSupplier.of((ProcessorSupplier) count ->
                        IntStream.range(0, count)
                                .mapToObj(GenP::new)
                                .collect(Collectors.toList())),
                expectedTotalParallelism);

        ProcessorMetaSupplier pmsPrint = ProcessorMetaSupplier.of(
                (ProcessorSupplier) count -> nCopies(count, new PrintP()));

        DAG dag = new DAG();
        Vertex generator = dag.newVertex("Generator", pmsGen);
        Vertex printer = dag.newVertex("Printer", pmsPrint);
        dag.edge(between(generator, printer));

        Map<Address, int[]> partitionAssignment = getPartitionAssignment(instance());
        assertEquals(3, partitionAssignment.size());
        Iterator<Address> it = partitionAssignment.keySet().iterator();

        JobConfig jobConfig = new JobConfig();
        jobConfig.setArgument(KEY_REQUIRED_PARTITIONS, new HashSet<>(Arrays.asList(
                partitionAssignment.get(it.next())[0], partitionAssignment.get(it.next())[0])));

        Job job = instance().getJet().newJob(dag, jobConfig);
        job.join();

        // should print 0 and 1.
        assertJobStatusEventually(job, JobStatus.COMPLETED);
        Assert.assertEquals("0", scanner.nextLine());
        Assert.assertEquals("1", scanner.nextLine());
    }

    @Test
    public void test_scanAndAgg() {
        // Given
        final int partitionId = 1;
        Address addr = getAddressForPartitionId(instance(), partitionId);

        ProcessorMetaSupplier pmsGen = ProcessorMetaSupplier.of((ProcessorSupplier) count ->
                IntStream.range(0, count).mapToObj(GenP::new).collect(Collectors.toList()));

        ProcessorMetaSupplier pmsAgg = forceTotalParallelismOne(
                ProcessorSupplier.of(
                        Processors.aggregateP(AggregateOperations.counting())),
                addr,
                false);

        ProcessorMetaSupplier pmsPrint = ProcessorMetaSupplier.of(
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



        JobConfig jobConfig = new JobConfig();
        jobConfig.setArgument(KEY_REQUIRED_PARTITIONS, singleton(1));

        Job job = instance().getJet().newJob(dag, jobConfig);
        job.join();

        // should print 2.
        assertJobStatusEventually(job, JobStatus.COMPLETED);
        Assert.assertEquals("2", scanner.nextLine());
    }

    @Test
    public void test_scanJoin() {
        // Given
        ProcessorMetaSupplier pmsGen1 = ProcessorMetaSupplier.of((ProcessorSupplier) count ->
                IntStream.range(0, count)
                        .mapToObj(GenP::new)
                        .collect(Collectors.toList()));

        ProcessorMetaSupplier pmsGen2 = ProcessorMetaSupplier.of((ProcessorSupplier) count ->
                IntStream.range(0, count)
                        .mapToObj(GenP::new)
                        .collect(Collectors.toList()));

        ProcessorMetaSupplier pmsJoin = new ValidatingMetaSupplier(
                ProcessorMetaSupplier.of(
                        (ProcessorSupplier) count -> nCopies(count, new JoinP())),
                2);

        ProcessorMetaSupplier pmsPrint = ProcessorMetaSupplier.of(
                (ProcessorSupplier) count -> nCopies(count, new PrintP()));

        DAG dag = new DAG();
        Vertex generatorLeft = dag.newVertex("Generator-Left", pmsGen1);
        Vertex generatorRight = dag.newVertex("Generator-Right", pmsGen2);
        Vertex joiner = dag.newVertex("Joiner", pmsJoin);
        Vertex printer = dag.newVertex("Printer", pmsPrint);

        dag.edge(Edge.from(generatorLeft).to(joiner, 0).isolated());
        dag.edge(Edge.from(generatorRight).to(joiner, 1).distributed().broadcast());
        dag.edge(between(joiner, printer).isolated());

        JobConfig jobConfig = new JobConfig();
        jobConfig.setArgument(KEY_REQUIRED_PARTITIONS, singleton(1));

        Job job = instance().getJet().newJob(dag, jobConfig);
        job.join();

        assertJobStatusEventually(job, JobStatus.COMPLETED);
        Set<String> expectedOutput = new HashSet<>(Arrays.asList("(0, 0)", "(1, 0)", "(0, 1)", "(1, 1)"));
        try {
            String nextLine = scanner.nextLine();
            System.err.println(nextLine);
            assertTrue(expectedOutput.remove(nextLine));
        } catch (NoSuchElementException e) {
            fail("Can't find any line in file");
        }
    }

    @Test
    public void test_memberPruningWithPMS() {
        // Given
        ProcessorMetaSupplier pmsGen = ProcessorMetaSupplier.of(2, (ProcessorSupplier) count ->
                IntStream.range(0, count)
                        .mapToObj(GenP::new)
                        .collect(Collectors.toList()));

        ProcessorMetaSupplier pmsPrint = ProcessorMetaSupplier.of(
                (ProcessorSupplier) count -> nCopies(count, new PrintP()));

        DAG dag = new DAG();
        Vertex generator = dag.newVertex("Generator", pmsGen);
        Vertex printer = dag.newVertex("Printer", pmsPrint);
        dag.edge(between(generator, printer));

        JobConfig jobConfig = new JobConfig();
        jobConfig.setArgument(KEY_REQUIRED_PARTITIONS, singleton(1));

        Job job = instance().getJet().newJob(dag, jobConfig);
        job.join();

        assertJobStatusEventually(job, JobStatus.COMPLETED);
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
        private FileWriter fileWriter;

        {
            try {
                fileWriter = new FileWriter(OUTPUT_FILE);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private PrintWriter printWriter = new PrintWriter(fileWriter);

        @Override
        protected boolean tryProcess0(@NotNull Object item) {
            System.err.println(item);
            printWriter.println(item);
            printWriter.flush();
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
