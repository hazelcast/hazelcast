/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.concurrent.atomiclong;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IFunction;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class AtomicLongBackupTest extends HazelcastTestSupport {

    public int backupCount = 1;

    private String name = randomName();

    private HazelcastInstance[] instances;

    private int partitionId;
    private IAtomicLong atomicLong;

    @Before
    public void setup() {
        Config config = new Config();

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(backupCount + 1);
        instances = factory.newInstances(config);
        warmUpPartitions(instances);

        partitionId = instances[0].getPartitionService().getPartition(name).getPartitionId();
        atomicLong = instances[0].getAtomicLong(name);
    }

    @Test
    public void testSet() {
        atomicLong.set(5);

        assertAtomicLongValue(instances, 5);
    }

    @Test
    public void testCompareAndSet() {
        atomicLong.set(5);
        atomicLong.compareAndSet(5, 10);

        assertAtomicLongValue(instances, 10);
    }

    @Test
    public void testAlter() {
        atomicLong.set(5);
        atomicLong.alter(new SetFunction(10));

        assertAtomicLongValue(instances, 10);
    }

    @Test
    public void testAlterAndGet() {
        atomicLong.set(5);
        atomicLong.alterAndGet(new SetFunction(10));

        assertAtomicLongValue(instances, 10);
    }

    @Test
    public void testGetAndAlter() {
        atomicLong.set(5);
        atomicLong.getAndAlter(new SetFunction(10));

        assertAtomicLongValue(instances, 10);
    }

    @Test
    public void testAddAndGet() {
        atomicLong.set(5);
        atomicLong.addAndGet(5);

        assertAtomicLongValue(instances, 10);
    }

    @Test
    public void testGetAndAdd() {
        atomicLong.set(5);
        atomicLong.getAndAdd(5);

        assertAtomicLongValue(instances, 10);
    }

    @Test
    public void testIncrementAndGet() {
        atomicLong.set(5);
        atomicLong.incrementAndGet();

        assertAtomicLongValue(instances, 6);
    }

    @Test
    public void testDecrementAndGet() {
        atomicLong.set(5);
        atomicLong.decrementAndGet();

        assertAtomicLongValue(instances, 4);
    }

    private void assertAtomicLongValue(HazelcastInstance[] instances, long value) {
        for (HazelcastInstance instance : instances) {
            assertEquals(value, readAtomicLongValue(instance));
        }
    }

    private long readAtomicLongValue(HazelcastInstance instance) {
        OperationServiceImpl operationService = (OperationServiceImpl) getOperationService(instance);
        AtomicLongService atomicLongService = getNodeEngineImpl(instance).getService(AtomicLongService.SERVICE_NAME);

        GetLongValue task = new GetLongValue(atomicLongService);
        operationService.execute(task);
        assertOpenEventually(task.latch);
        return task.value;
    }

    private class GetLongValue implements PartitionSpecificRunnable {

        final CountDownLatch latch = new CountDownLatch(1);

        final AtomicLongService atomicLongService;

        long value;

        GetLongValue(AtomicLongService atomicLongService) {
            this.atomicLongService = atomicLongService;
        }

        @Override
        public int getPartitionId() {
            return partitionId;
        }

        @Override
        public void run() {
            final AtomicLongContainer longContainer = atomicLongService.getLongContainer(name);
            value = longContainer.get();
            latch.countDown();
        }
    }

    private static class SetFunction implements IFunction<Long, Long>, Serializable {

        private long value;

        SetFunction(long value) {
            this.value = value;
        }

        @Override
        public Long apply(Long input) {
            return value;
        }
    }
}
