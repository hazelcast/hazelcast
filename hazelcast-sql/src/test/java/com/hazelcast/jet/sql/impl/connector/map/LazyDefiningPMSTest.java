/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.cluster.Address;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.jet.core.test.TestSupport.out;
import static com.hazelcast.jet.impl.JetServiceBackend.SQL_ARGUMENTS_KEY_NAME;
import static com.hazelcast.jet.sql.impl.connector.map.LazyDefiningSpecificMemberPms.lazyForceTotalParallelismOne;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;

public class LazyDefiningPMSTest extends SimpleTestInClusterSupport {
    private static final int ITERATIONS = 1000;

    private int pKey;
    private int keyOwnerPartitionId;
    private Address ownderAddress;

    @BeforeClass
    public static void beforeClass() throws Exception {
        initialize(7, null);
    }

    @Before
    public void setUp() throws Exception {
        NodeEngineImpl nodeEngine = getNodeEngineImpl(instance());
        Map<Address, int[]> partitionAssignment = getPartitionAssignment(instance());
        ownderAddress = instance().getCluster().getLocalMember().getAddress();
        for (int i = 1; i < ITERATIONS; ++i) {
            int pIdCandidate = instance().getPartitionService().getPartition(i).getPartitionId();
            if (Arrays.binarySearch(partitionAssignment.get(ownderAddress), pIdCandidate) >= 0) {
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
                new TestGenPSupplier(),
                1);

        JobConfig config = new JobConfig()
                .setArgument(SQL_ARGUMENTS_KEY_NAME, Arrays.asList(0, pKey, 2));

        // When & Then
        TestSupport.verifyProcessor(pmsGen)
                .hazelcastInstance(instance())
                .jobConfig(config)
                .disableSnapshots()
                .disableProgressAssertion()
                .expectExactOutput(out(ownderAddress));

        Assert.assertEquals(keyOwnerPartitionId, pmsGen.partitionId);
    }

    @Test
    public void test_expressionSupplier() {
        // Given
        LazyDefiningSpecificMemberPms pmsGen = (LazyDefiningSpecificMemberPms) lazyForceTotalParallelismOne(
                new TestGenPSupplier(),
                (SupplierEx<Expression<?>>) () -> ConstantExpression.create(pKey, INT));

        JobConfig config = new JobConfig()
                .setArgument(SQL_ARGUMENTS_KEY_NAME, Arrays.asList(0, pKey, 2));

        // When & Then
        TestSupport.verifyProcessor(pmsGen)
                .hazelcastInstance(instance())
                .jobConfig(config)
                .disableSnapshots()
                .disableProgressAssertion()
                .expectExactOutput(out(ownderAddress));

        Assert.assertEquals(keyOwnerPartitionId, pmsGen.partitionId);
    }

    @Test
    public void test_nonPrunableExpressionSupplier() {
        // Given
        LazyDefiningSpecificMemberPms pmsGen = (LazyDefiningSpecificMemberPms) lazyForceTotalParallelismOne(
                new TestGenPSupplier(),
                (SupplierEx<Expression<?>>) () -> ConstantExpression.create(pKey, INT));

        // When & Then
        TestSupport.verifyProcessor(pmsGen)
                .hazelcastInstance(instance())
                .disableSnapshots()
                .disableProgressAssertion()
                .expectExactOutput(out(ownderAddress));

        Assert.assertEquals(keyOwnerPartitionId, pmsGen.partitionId);
    }

    private static class TestGenPSupplier implements ProcessorSupplier {
        private Address ownderAddress;

        @Override
        public void init(@NotNull ProcessorSupplier.Context context) throws Exception {
            int[] memberPartitions = context.memberPartitions();
            Map<Address, int[]> addressMap = context.partitionAssignment();
            for (Entry<Address, int[]> entry : addressMap.entrySet()) {
                if (Arrays.equals(memberPartitions, entry.getValue())) {
                    ownderAddress = entry.getKey();
                }
            }
            if (ownderAddress == null) {
                throw new AssertionError("No owner address found");
            }
        }

        @Nonnull
        @Override
        public Collection<? extends Processor> get(int count) {
            return Collections.nCopies(count, new GenP(ownderAddress));
        }
    }

    private static class GenP extends AbstractProcessor {
        private final Address item;

        GenP(Address item) {
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
