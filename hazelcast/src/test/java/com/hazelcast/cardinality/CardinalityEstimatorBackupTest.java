/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cardinality;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.CardinalityEstimator;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;

public class CardinalityEstimatorBackupTest
        extends HazelcastTestSupport {

    private HazelcastInstance instance1;

    private HazelcastInstance instance2;

    private String name = randomName();

    private int partitionId;

    private CardinalityEstimator estimator;

    @Before
    public void init() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        instance1 = factory.newHazelcastInstance();
        instance2 = factory.newHazelcastInstance();
        warmUpPartitions(instance1, instance2);

        partitionId = instance1.getPartitionService().getPartition(name).getPartitionId();
        estimator = instance1.getCardinalityEstimator(name);
    }

    @Test
    public void testAggregate() {
        estimator.aggregate(10000L);

        assertEstimateValue(instance1, 1L);
        assertEstimateValue(instance2, 1L);
    }

    @Test
    public void testAggregateAll() {
        estimator.aggregateAll(new long[] { 10000L, 20000L, 30000L });

        assertEstimateValue(instance1, 3L);
        assertEstimateValue(instance2, 3L);
    }

    private void assertEstimateValue(final HazelcastInstance instance, final long value) {
        assertEquals(value, readEstimate(instance));
    }

    private long readEstimate(final HazelcastInstance instance) {
        final OperationServiceImpl operationService = (OperationServiceImpl) getOperationService(instance);
        final CardinalityEstimatorService cardinalityEstimatorService = getNodeEngineImpl(instance)
                .getService(CardinalityEstimatorService.SERVICE_NAME);

        final CardinalityEstimatorBackupTest.GetEstimate task =
                new CardinalityEstimatorBackupTest.GetEstimate(cardinalityEstimatorService);
        operationService.execute(task);
        assertOpenEventually(task.latch);
        return task.value;
    }

    private class GetEstimate implements PartitionSpecificRunnable {

        final CountDownLatch latch = new CountDownLatch(1);

        final CardinalityEstimatorService cardinalityEstimatorService;

        long value;

        GetEstimate(CardinalityEstimatorService cardinalityEstimatorService) {
            this.cardinalityEstimatorService = cardinalityEstimatorService;
        }

        @Override
        public int getPartitionId() {
            return partitionId;
        }

        @Override
        public void run() {
            final CardinalityEstimatorContainer estimatorContainer =
                    cardinalityEstimatorService.getCardinalityEstimatorContainer(name);
            value = estimatorContainer.estimate();
            latch.countDown();
        }
    }

}
