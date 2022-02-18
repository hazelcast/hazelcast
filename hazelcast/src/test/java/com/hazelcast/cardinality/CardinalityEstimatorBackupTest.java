/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cardinality.impl.CardinalityEstimatorContainer;
import com.hazelcast.cardinality.impl.CardinalityEstimatorService;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static com.hazelcast.test.Accessors.getOperationService;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CardinalityEstimatorBackupTest extends HazelcastTestSupport {

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
    public void testAdd() {
        estimator.add(10000L);

        assertEstimateValue(instance1, 1L);
        assertEstimateValue(instance2, 1L);
    }

    @Test
    public void testAddAll() {
        estimator.add(10000L);
        estimator.add(20000L);
        estimator.add(30000L);

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
