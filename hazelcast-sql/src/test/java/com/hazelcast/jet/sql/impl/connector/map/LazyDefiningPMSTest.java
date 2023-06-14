package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.cluster.Address;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.jet.impl.execution.init.ExecutionPlanBuilder;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.partition.Partition;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.security.Permission;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.jet.core.test.TestSupport.out;
import static com.hazelcast.jet.impl.JetServiceBackend.SQL_ARGUMENTS_KEY_NAME;
import static com.hazelcast.jet.sql.impl.connector.map.LazyDefiningSpecificMemberPms.lazyForceTotalParallelismOne;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toMap;

public class LazyDefiningPMSTest extends SimpleTestInClusterSupport {
    private Map<Address, int[]> partitionAssignment;
    private int pKey;
    private int keyOwnerPartitionId;

    @BeforeClass
    public static void beforeClass() throws Exception {
        initialize(3, null);
    }

    @Before
    public void setUp() throws Exception {
        NodeEngineImpl nodeEngine = getNodeEngineImpl(instance());
        partitionAssignment = ExecutionPlanBuilder.getPartitionAssignment(nodeEngine,
                        Util.getMembersView(nodeEngine).getMembers())
                .entrySet().stream().collect(toMap(en -> en.getKey().getAddress(), Entry::getValue));
        Address address = instance().getCluster().getLocalMember().getAddress();
        for (int i = 1; i < 1000; ++i) {
            int pIdCandidate = instance().getPartitionService().getPartition(i).getPartitionId();
            if (Arrays.binarySearch(partitionAssignment.get(address), pIdCandidate) >= 0) {
                pKey = i;
                keyOwnerPartitionId = pIdCandidate;
                break;
            }
        }
    }

    @Test
    public void test_partitionArgumentIndex() {
        // Given
        LazyDefiningSpecificMemberPms pmsGen = (LazyDefiningSpecificMemberPms) lazyForceTotalParallelismOne(
                (ProcessorSupplier) count ->
                        IntStream.range(0, count)
                                .mapToObj(GenP::new)
                                .collect(Collectors.toList()),
                1);

        JobConfig config = new JobConfig()
                .setArgument(SQL_ARGUMENTS_KEY_NAME, Arrays.asList(0, pKey, 2));

        // When & Then
        TestSupport.verifyProcessor(pmsGen)
                .hazelcastInstance(instance())
                .jobConfig(config)
                .disableSnapshots()
                .disableProgressAssertion()
                .expectExactOutput(out(0));

        Assert.assertEquals(keyOwnerPartitionId, pmsGen.partitionId);
    }

    @Test
    public void test_expressionSupplier() {
        // Given
        LazyDefiningSpecificMemberPms pmsGen = (LazyDefiningSpecificMemberPms) lazyForceTotalParallelismOne(
                (ProcessorSupplier) count ->
                        IntStream.range(0, count)
                                .mapToObj(GenP::new)
                                .collect(Collectors.toList()),
                (SupplierEx<Expression<?>>) () -> ConstantExpression.create(pKey, QueryDataType.INT));

        // When & Then
        TestSupport.verifyProcessor(pmsGen)
                .hazelcastInstance(instance())
                .disableSnapshots()
                .disableProgressAssertion()
                .expectExactOutput(out(0));

        Assert.assertEquals(keyOwnerPartitionId, pmsGen.partitionId);
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
}
