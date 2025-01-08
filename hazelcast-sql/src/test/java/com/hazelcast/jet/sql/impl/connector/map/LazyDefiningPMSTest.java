/*
 * Copyright 2025 Hazelcast Inc.
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
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import org.jetbrains.annotations.NotNull;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.jet.core.test.TestSupport.out;
import static com.hazelcast.jet.impl.JetServiceBackend.SQL_ARGUMENTS_KEY_NAME;
import static com.hazelcast.jet.sql.impl.connector.map.LazyDefiningSpecificMemberPms.lazyForceTotalParallelismOne;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;

public class LazyDefiningPMSTest extends SimpleTestInClusterSupport {
    private static final int ITERATIONS = 1000;

    /** Address of the master member. */
    private static Address ownerAddress;
    /** A partition key owned by the member having {@link #ownerAddress}. */
    private static int partitionKey;

    @BeforeClass
    public static void beforeClass() throws Exception {
        initialize(7, null);

        ownerAddress = instance().getCluster().getLocalMember().getAddress();
        Map<Address, int[]> partitionAssignment = getPartitionAssignment(instance());
        for (int i = 1; i < ITERATIONS; i++) {
            int partitionId = instance().getPartitionService().getPartition(i).getPartitionId();
            if (Arrays.binarySearch(partitionAssignment.get(ownerAddress), partitionId) >= 0) {
                partitionKey = i;
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
                .setArgument(SQL_ARGUMENTS_KEY_NAME, List.of(0, partitionKey, 2));

        // When & Then
        TestSupport.verifyProcessor(pmsGen)
                .hazelcastInstance(instance())
                .jobConfig(config)
                .disableSnapshots()
                .disableProgressAssertion()
                .expectExactOutput(out(ownerAddress));
    }

    @Test
    public void test_expressionSupplier() {
        // Given
        LazyDefiningSpecificMemberPms pmsGen = (LazyDefiningSpecificMemberPms) lazyForceTotalParallelismOne(
                new TestGenPSupplier(),
                (SupplierEx<Expression<?>>) () -> ConstantExpression.create(partitionKey, INT));

        JobConfig config = new JobConfig()
                .setArgument(SQL_ARGUMENTS_KEY_NAME, List.of(0, partitionKey, 2));

        // When & Then
        TestSupport.verifyProcessor(pmsGen)
                .hazelcastInstance(instance())
                .jobConfig(config)
                .disableSnapshots()
                .disableProgressAssertion()
                .expectExactOutput(out(ownerAddress));
    }

    @Test
    public void test_nonPrunableExpressionSupplier() {
        // Given
        LazyDefiningSpecificMemberPms pmsGen = (LazyDefiningSpecificMemberPms) lazyForceTotalParallelismOne(
                new TestGenPSupplier(),
                (SupplierEx<Expression<?>>) () -> ConstantExpression.create(partitionKey, INT));

        // When & Then
        TestSupport.verifyProcessor(pmsGen)
                .hazelcastInstance(instance())
                .disableSnapshots()
                .disableProgressAssertion()
                .expectExactOutput(out(ownerAddress));
    }

    private static class TestGenPSupplier implements ProcessorSupplier {
        private Address ownderAddress;

        @Override
        public void init(@NotNull ProcessorSupplier.Context context) {
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
