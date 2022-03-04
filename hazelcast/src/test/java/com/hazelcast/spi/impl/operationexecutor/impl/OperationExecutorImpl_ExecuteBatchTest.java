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

package com.hazelcast.spi.impl.operationexecutor.impl;

import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.PartitionTaskFactory;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.BitSet;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.spi.properties.ClusterProperty.PARTITION_COUNT;
import static com.hazelcast.spi.properties.ClusterProperty.PARTITION_OPERATION_THREAD_COUNT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class OperationExecutorImpl_ExecuteBatchTest extends OperationExecutorImpl_AbstractTest {

    @Test(expected = NullPointerException.class)
    public void whenNullFactory() {
        initExecutor();

        executor.executeOnPartitions(null, new BitSet());
    }

    @Test(expected = NullPointerException.class)
    public void whenNullPartitions() {
        initExecutor();

        executor.executeOnPartitions(mock(PartitionTaskFactory.class), null);
    }

    @Test
    public void executeOnEachPartition() {
        config.setProperty(PARTITION_OPERATION_THREAD_COUNT.getName(), "16");
        initExecutor();

        final BitSet partitions = newPartitions();
        final DummyPartitionTaskFactory taskFactory = new DummyPartitionTaskFactory();
        executor.executeOnPartitions(taskFactory, partitions);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(partitions.length(), taskFactory.completed.get());
            }
        });
    }

    @Test
    public void noMoreBubble() {
        config.setProperty(PARTITION_OPERATION_THREAD_COUNT.getName(), "1");
        initExecutor();

        final DummyPartitionTaskFactory taskFactory = new DummyPartitionTaskFactory();
        taskFactory.delayMs = 1000;
        executor.executeOnPartitions(taskFactory, newPartitions());

        final DummyOperation op = new DummyOperation();
        executor.execute(op);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue(op.completed);
            }
        }, 5);
    }

    private BitSet newPartitions() {
        HazelcastProperties properties = new HazelcastProperties(config);
        BitSet bitSet = new BitSet(properties.getInteger(PARTITION_COUNT));
        for (int k = 0; k < bitSet.size(); k++) {
            bitSet.set(k);
        }
        return bitSet;
    }

    class DummyOperation extends Operation {
        private volatile boolean completed;

        @Override
        public void run() {
            completed = true;
        }
    }

    class DummyPartitionTaskFactory implements PartitionTaskFactory {
        private final AtomicLong completed = new AtomicLong();
        private int delayMs;

        @Override
        public Object create(int partitionId) {
            return new DummyTask(completed, delayMs);
        }
    }

    class DummyTask implements Runnable {
        private final AtomicLong completed;
        private final int delayMs;

        DummyTask(AtomicLong completed, int delayMs) {
            this.completed = completed;
            this.delayMs = delayMs;
        }

        @Override
        public void run() {
            sleepMillis(delayMs);
            completed.incrementAndGet();
        }
    }
}
