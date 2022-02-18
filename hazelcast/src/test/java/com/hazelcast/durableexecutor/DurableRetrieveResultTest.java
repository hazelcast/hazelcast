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

package com.hazelcast.durableexecutor;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.executor.ExecutorServiceTestSupport;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DurableRetrieveResultTest extends ExecutorServiceTestSupport {

    @Test
    public void testRetrieveResult_WhenNewNodesJoin() throws ExecutionException, InterruptedException {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance instance1 = factory.newHazelcastInstance(smallInstanceConfig());

        DurableExecutorService executorService = instance1.getDurableExecutorService(randomString());
        SleepingTask task = new SleepingTask(5);
        DurableExecutorServiceFuture<Boolean> future = executorService.submit(task);
        factory.newHazelcastInstance(smallInstanceConfig());
        factory.newHazelcastInstance(smallInstanceConfig());
        assertTrue(future.get());
        Future<Boolean> retrievedFuture = executorService.retrieveAndDisposeResult(future.getTaskId());
        assertTrue(retrievedFuture.get());
    }

    @Test
    public void testDisposeResult() throws Exception {
        String key = randomString();
        String name = randomString();
        HazelcastInstance instance = createHazelcastInstance(smallInstanceConfig());
        DurableExecutorService executorService = instance.getDurableExecutorService(name);
        BasicTestCallable task = new BasicTestCallable();
        DurableExecutorServiceFuture<String> future = executorService.submitToKeyOwner(task, key);
        future.get();
        executorService.disposeResult(future.getTaskId());
        Future<Object> resultFuture = executorService.retrieveResult(future.getTaskId());
        assertNull(resultFuture.get());
    }

    @Test
    public void testRetrieveAndDispose_WhenSubmitterMemberDown() throws Exception {
        String name = randomString();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance instance1 = factory.newHazelcastInstance(smallInstanceConfig());
        HazelcastInstance instance2 = factory.newHazelcastInstance(smallInstanceConfig());
        factory.newHazelcastInstance(smallInstanceConfig());
        String key = generateKeyOwnedBy(instance2);

        DurableExecutorService executorService = instance1.getDurableExecutorService(name);
        SleepingTask task = new SleepingTask(4);
        long taskId = executorService.submitToKeyOwner(task, key).getTaskId();

        instance1.shutdown();

        executorService = instance2.getDurableExecutorService(name);
        Future<Boolean> future = executorService.retrieveAndDisposeResult(taskId);
        assertTrue(future.get());

        Future<Object> resultFuture = executorService.retrieveResult(taskId);
        assertNull(resultFuture.get());
    }

    @Test
    public void testRetrieveAndDispose_WhenOwnerMemberDown() throws Exception {
        String name = randomString();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance instance1 = factory.newHazelcastInstance(smallInstanceConfig());
        HazelcastInstance instance2 = factory.newHazelcastInstance(smallInstanceConfig());
        factory.newHazelcastInstance(smallInstanceConfig());
        String key = generateKeyOwnedBy(instance1);

        DurableExecutorService executorService = instance1.getDurableExecutorService(name);
        SleepingTask task = new SleepingTask(4);
        long taskId = executorService.submitToKeyOwner(task, key).getTaskId();

        instance1.shutdown();

        executorService = instance2.getDurableExecutorService(name);
        Future<Boolean> future = executorService.retrieveAndDisposeResult(taskId);
        assertTrue(future.get());

        Future<Object> resultFuture = executorService.retrieveResult(taskId);
        assertNull(resultFuture.get());
    }

    @Test
    public void testSingleExecution_WhenMigratedAfterCompletion_WhenOwnerMemberKilled() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);

        HazelcastInstance[] instances = factory.newInstances(smallInstanceConfig());
        HazelcastInstance first = instances[0];
        HazelcastInstance second = instances[1];

        waitAllForSafeState(instances);

        String key = generateKeyOwnedBy(first);

        String runCounterName = "runCount";
        IAtomicLong runCount = second.getCPSubsystem().getAtomicLong(runCounterName);

        String name = randomString();
        DurableExecutorService executorService = first.getDurableExecutorService(name);
        IncrementAtomicLongRunnable task = new IncrementAtomicLongRunnable(runCounterName);
        DurableExecutorServiceFuture<?> future = executorService.submitToKeyOwner(task, key);

        future.get(); // Wait for it to finish

        // Avoid race between PutResult & SHUTDOWN
        sleepSeconds(3);

        first.getLifecycleService().terminate();

        executorService = second.getDurableExecutorService(name);
        Future<Object> newFuture = executorService.retrieveResult(future.getTaskId());
        newFuture.get(); // Make sure its completed

        assertEquals(1, runCount.get());
    }

    @Test
    public void testRetrieve_WhenSubmitterMemberDown() throws Exception {
        String name = randomString();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance instance1 = factory.newHazelcastInstance(smallInstanceConfig());
        HazelcastInstance instance2 = factory.newHazelcastInstance(smallInstanceConfig());
        factory.newHazelcastInstance(smallInstanceConfig());
        String key = generateKeyOwnedBy(instance2);

        DurableExecutorService executorService = instance1.getDurableExecutorService(name);
        SleepingTask task = new SleepingTask(4);
        long taskId = executorService.submitToKeyOwner(task, key).getTaskId();

        instance1.shutdown();

        executorService = instance2.getDurableExecutorService(name);
        Future<Boolean> future = executorService.retrieveResult(taskId);
        assertTrue(future.get());
    }

    @Test
    public void testRetrieve_WhenOwnerMemberDown() throws Exception {
        String name = randomString();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance instance1 = factory.newHazelcastInstance(smallInstanceConfig());
        HazelcastInstance instance2 = factory.newHazelcastInstance(smallInstanceConfig());
        factory.newHazelcastInstance(smallInstanceConfig());
        String key = generateKeyOwnedBy(instance1);

        DurableExecutorService executorService = instance1.getDurableExecutorService(name);
        SleepingTask task = new SleepingTask(4);
        long taskId = executorService.submitToKeyOwner(task, key).getTaskId();

        instance1.shutdown();

        executorService = instance2.getDurableExecutorService(name);
        Future<Boolean> future = executorService.retrieveResult(taskId);
        assertTrue(future.get());
    }

    @Test
    public void testRetrieve_WhenResultOverwritten() throws Exception {
        String name = randomString();
        Config config = smallInstanceConfig();
        config.getDurableExecutorConfig(name).setCapacity(1).setDurability(0);
        HazelcastInstance instance = createHazelcastInstance(config);
        DurableExecutorService executorService = instance.getDurableExecutorService(name);
        DurableExecutorServiceFuture<String> future = executorService.submitToKeyOwner(new BasicTestCallable(), name);
        long taskId = future.getTaskId();
        future.get();

        executorService.submitToKeyOwner(new BasicTestCallable(), name);

        Future<Object> resultFuture = executorService.retrieveResult(taskId);
        try {
            resultFuture.get();
            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof StaleTaskIdException);
        }
    }
}
