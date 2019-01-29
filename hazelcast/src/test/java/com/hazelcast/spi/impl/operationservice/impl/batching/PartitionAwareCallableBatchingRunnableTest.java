/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.operationservice.impl.batching;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.spi.impl.operationexecutor.impl.OperationExecutorImpl;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.RequireAssertEnabled;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@RequireAssertEnabled
@Category({QuickTest.class, ParallelTest.class})
public class PartitionAwareCallableBatchingRunnableTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void test_NotRunningOnPartitionThread() {
        PartitionAwareCallableBatchingRunnable runnable = new PartitionAwareCallableBatchingRunnable(
                getNodeEngineImpl(createHazelcastInstance()), new TestPartitionAwareCallableFactory());

        exception.expect(AssertionError.class);
        runnable.run();
    }

    @Test
    public void test_whenRunningOnPartitionThread() throws IllegalAccessException, ExecutionException, InterruptedException {
        HazelcastInstance hz = createHazelcastInstance();
        PartitionAwareCallableBatchingRunnable runnable = new PartitionAwareCallableBatchingRunnable(
                getNodeEngineImpl(hz), new TestPartitionAwareCallableFactory());

        // init data and partitions
        IMap map = hz.getMap("testMap");
        for (int i = 0; i < 1000; i++) {
            map.put(i, i);
        }

        OperationServiceImpl ops = ((OperationServiceImpl) getNodeEngineImpl(hz).getOperationService());
        int partitionCount = getNodeEngineImpl(hz).getPartitionService().getPartitionCount();

        OperationExecutorImpl executor = (OperationExecutorImpl) ops.getOperationExecutor();
        executor.executeOnPartitionThreads(runnable);
        List result = (List) runnable.getFuture().get();

        assertTrue(runnable.getFuture().isDone());
        assertEquals(partitionCount, result.size());
        Set sortedResult = new TreeSet(result);
        for (int i = 0; i < partitionCount; i++) {
            assertTrue(sortedResult.contains(i));
        }
    }

    private static class TestPartitionAwareCallableFactory implements PartitionAwareCallableFactory {
        @Override
        public PartitionAwareCallable create() {
            return new TestPartitionAwareCallable();
        }
    }

    private static class TestPartitionAwareCallable implements PartitionAwareCallable {
        @Override
        public Object call(int partitionId) {
            return partitionId;
        }
    }
}
