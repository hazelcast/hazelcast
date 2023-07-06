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
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.jet.impl.execution.init.ExecutionPlanBuilder;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.jet.core.test.TestSupport.out;
import static com.hazelcast.jet.impl.JetServiceBackend.SQL_ARGUMENTS_KEY_NAME;
import static com.hazelcast.jet.sql.impl.connector.map.LazyDefiningSpecificMemberPms.lazyForceTotalParallelismOne;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static java.util.stream.Collectors.toMap;

public class LazyDefiningPMSTest extends SimpleTestInClusterSupport {
    private static int ITERATIONS = 1000;

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
                        Util.getMembersView(nodeEngine).getMembers(), null)
                .entrySet().stream().collect(toMap(en -> en.getKey().getAddress(), Entry::getValue));
        Address address = instance().getCluster().getLocalMember().getAddress();
        for (int i = 1; i < ITERATIONS; ++i) {
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
                (SupplierEx<Expression<?>>) () -> ConstantExpression.create(pKey, INT));

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
