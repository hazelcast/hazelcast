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

package com.hazelcast.durableexecutor;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.Member;
import com.hazelcast.executor.ExecutorServiceTestSupport;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DurableSmallClusterTest extends ExecutorServiceTestSupport {

    private static final int TEST_TIMEOUT = 60000;
    private static final int NODE_COUNT = 3;

    private HazelcastInstance[] instances;

    @Before
    public void setup() {
        instances = createHazelcastInstanceFactory(NODE_COUNT).newInstances();
    }

    @Test
    public void executionCallback_notified() throws Exception {
        DurableExecutorService executorService = instances[1].getDurableExecutorService(randomString());
        BasicTestCallable task = new BasicTestCallable();
        String key = generateKeyOwnedBy(instances[0]);
        ICompletableFuture<String> future = executorService.submitToKeyOwner(task, key);
        CountingDownExecutionCallback<String> callback = new CountingDownExecutionCallback<String>(1);
        future.andThen(callback);
        future.get();
        assertOpenEventually(callback.getLatch(), 10);
    }

    @Test
    public void submitToSeveralNodes_runnable() throws Exception {
        for (HazelcastInstance instance : instances) {
            DurableExecutorService service = instance.getDurableExecutorService("testExecuteMultipleNode");
            int rand = new Random().nextInt(100);
            Future<Integer> future = service.submit(new IncrementAtomicLongRunnable("count"), rand);
            assertEquals(Integer.valueOf(rand), future.get());
        }

        IAtomicLong count = instances[0].getAtomicLong("count");
        assertEquals(instances.length, count.get());
    }

    @Test
    public void submitToKeyOwner_runnable() {
        NullResponseCountingCallback callback = new NullResponseCountingCallback(instances.length);

        for (HazelcastInstance instance : instances) {
            DurableExecutorService service = instance.getDurableExecutorService("testSubmitToKeyOwnerRunnable");
            Member localMember = instance.getCluster().getLocalMember();
            String uuid = localMember.getUuid();
            Runnable runnable = new IncrementAtomicLongIfMemberUUIDNotMatchRunnable(uuid, "testSubmitToKeyOwnerRunnable");
            int key = findNextKeyForMember(instance, localMember);
            service.submitToKeyOwner(runnable, key).andThen(callback);
        }

        assertOpenEventually(callback.getResponseLatch());
        assertEquals(0, instances[0].getAtomicLong("testSubmitToKeyOwnerRunnable").get());
        assertEquals(instances.length, callback.getNullResponseCount());
    }

    @Test
    public void submitToSeveralNodes_callable() throws Exception {
        for (int i = 0; i < instances.length; i++) {
            DurableExecutorService service = instances[i].getDurableExecutorService("testSubmitMultipleNode");
            Future future = service.submit(new IncrementAtomicLongCallable("testSubmitMultipleNode"));
            assertEquals((long) (i + 1), future.get());
        }
    }

    @Test(timeout = TEST_TIMEOUT)
    public void submitToKeyOwner_callable() throws Exception {
        List<Future> futures = new ArrayList<Future>();

        for (HazelcastInstance instance : instances) {
            DurableExecutorService service = instance.getDurableExecutorService("testSubmitToKeyOwnerCallable");
            Member localMember = instance.getCluster().getLocalMember();
            int key = findNextKeyForMember(instance, localMember);

            Future future = service.submitToKeyOwner(new MemberUUIDCheckCallable(localMember.getUuid()), key);
            futures.add(future);
        }

        for (Future future : futures) {
            assertTrue((Boolean) future.get(60, TimeUnit.SECONDS));
        }
    }

    @Test(timeout = TEST_TIMEOUT)
    public void submitToKeyOwner_callable_withCallback() {
        BooleanSuccessResponseCountingCallback callback = new BooleanSuccessResponseCountingCallback(instances.length);

        for (HazelcastInstance instance : instances) {
            DurableExecutorService service = instance.getDurableExecutorService("testSubmitToKeyOwnerCallable");
            Member localMember = instance.getCluster().getLocalMember();
            int key = findNextKeyForMember(instance, localMember);
            service.submitToKeyOwner(new MemberUUIDCheckCallable(localMember.getUuid()), key).andThen(callback);
        }

        assertOpenEventually(callback.getResponseLatch());
        assertEquals(instances.length, callback.getSuccessResponseCount());
    }
}
