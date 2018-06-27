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

package com.hazelcast.executor;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.Member;
import com.hazelcast.core.MultiExecutionCallback;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class SmallClusterTest extends ExecutorServiceTestSupport {

    private static final int TEST_TIMEOUT = 60000;
    private static final int NODE_COUNT = 3;

    private HazelcastInstance[] instances;

    @Before
    public void setUp() {
        instances = createHazelcastInstanceFactory(NODE_COUNT).newInstances(new Config());
        warmUpPartitions(instances);
    }

    @Test
    public void executionCallback_notified() throws Exception {
        IExecutorService executorService = instances[1].getExecutorService(randomString());
        BasicTestCallable task = new BasicTestCallable();
        String key = generateKeyOwnedBy(instances[0]);
        ICompletableFuture<String> future = (ICompletableFuture<String>) executorService.submitToKeyOwner(task, key);
        CountingDownExecutionCallback<String> callback = new CountingDownExecutionCallback<String>(1);
        future.andThen(callback);
        future.get();
        assertOpenEventually(callback.getLatch(), 10);
    }

    @Test
    public void submitToSeveralNodes_runnable() throws Exception {
        for (HazelcastInstance instance : instances) {
            IExecutorService service = instance.getExecutorService("testExecuteMultipleNode");
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
            IExecutorService service = instance.getExecutorService("testSubmitToKeyOwnerRunnable");
            Member localMember = instance.getCluster().getLocalMember();
            int key = findNextKeyForMember(instance, localMember);
            service.submitToKeyOwner(
                    new IncrementAtomicLongIfMemberUUIDNotMatchRunnable(localMember.getUuid(), "testSubmitToKeyOwnerRunnable"),
                    key, callback);
        }

        assertOpenEventually(callback.getResponseLatch());
        assertEquals(0, instances[0].getAtomicLong("testSubmitToKeyOwnerRunnable").get());
        assertEquals(instances.length, callback.getNullResponseCount());
    }

    @Test
    public void submitToMember_runnable() {
        NullResponseCountingCallback callback = new NullResponseCountingCallback(instances.length);

        for (HazelcastInstance instance : instances) {
            IExecutorService service = instance.getExecutorService("testSubmitToMemberRunnable");
            Member localMember = instance.getCluster().getLocalMember();
            service.submitToMember(
                    new IncrementAtomicLongIfMemberUUIDNotMatchRunnable(localMember.getUuid(), "testSubmitToMemberRunnable"),
                    localMember, callback);
        }

        assertOpenEventually(callback.getResponseLatch());
        assertEquals(0, instances[0].getAtomicLong("testSubmitToMemberRunnable").get());
        assertEquals(instances.length, callback.getNullResponseCount());
    }

    @Test
    public void submitToMembers_runnable() {
        int sum = 0;
        Set<Member> membersSet = instances[0].getCluster().getMembers();
        Member[] members = membersSet.toArray(new Member[0]);
        Random random = new Random();

        ResponseCountingMultiExecutionCallback callback = new ResponseCountingMultiExecutionCallback(instances.length);
        for (HazelcastInstance instance : instances) {
            IExecutorService service = instance.getExecutorService("testSubmitToMembersRunnable");
            int n = random.nextInt(instances.length) + 1;
            sum += n;
            Member[] m = new Member[n];
            System.arraycopy(members, 0, m, 0, n);
            service.submitToMembers(new IncrementAtomicLongRunnable("testSubmitToMembersRunnable"), Arrays.asList(m), callback);
        }
        assertOpenEventually(callback.getLatch());

        IAtomicLong result = instances[0].getAtomicLong("testSubmitToMembersRunnable");
        assertEquals(sum, result.get());
        assertEquals(sum, callback.getCount());
    }

    @Test
    public void submitToAllMembers_runnable() {
        ResponseCountingMultiExecutionCallback callback = new ResponseCountingMultiExecutionCallback(instances.length);

        for (HazelcastInstance instance : instances) {
            IExecutorService service = instance.getExecutorService("testSubmitToAllMembersRunnable");
            service.submitToAllMembers(new IncrementAtomicLongRunnable("testSubmitToAllMembersRunnable"), callback);
        }
        assertOpenEventually(callback.getLatch());

        IAtomicLong result = instances[0].getAtomicLong("testSubmitToAllMembersRunnable");
        assertEquals(instances.length * instances.length, result.get());
        assertEquals(instances.length * instances.length, callback.getCount());
    }

    @Test
    public void submitToSeveralNodes_callable() throws Exception {
        for (int i = 0; i < instances.length; i++) {
            IExecutorService service = instances[i].getExecutorService("testSubmitMultipleNode");
            Future future = service.submit(new IncrementAtomicLongCallable("testSubmitMultipleNode"));
            assertEquals((long) (i + 1), future.get());
        }
    }

    @Test(timeout = TEST_TIMEOUT)
    public void submitToKeyOwner_callable() throws Exception {
        List<Future> futures = new ArrayList<Future>();

        for (HazelcastInstance instance : instances) {
            IExecutorService service = instance.getExecutorService("testSubmitToKeyOwnerCallable");
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
            IExecutorService service = instance.getExecutorService("testSubmitToKeyOwnerCallable");
            Member localMember = instance.getCluster().getLocalMember();
            int key = findNextKeyForMember(instance, localMember);
            service.submitToKeyOwner(new MemberUUIDCheckCallable(localMember.getUuid()), key, callback);
        }

        assertOpenEventually(callback.getResponseLatch());
        assertEquals(instances.length, callback.getSuccessResponseCount());
    }

    @Test(timeout = TEST_TIMEOUT)
    public void submitToMember_callable() throws Exception {
        List<Future> futures = new ArrayList<Future>();

        for (HazelcastInstance instance : instances) {
            IExecutorService service = instance.getExecutorService("testSubmitToMemberCallable");
            Member localMember = instance.getCluster().getLocalMember();

            Future future = service.submitToMember(new MemberUUIDCheckCallable(localMember.getUuid()), localMember);
            futures.add(future);
        }

        for (Future future : futures) {
            assertTrue((Boolean) future.get());
        }
    }

    @Test(timeout = TEST_TIMEOUT)
    public void submitToMember_callable_withCallback() {
        BooleanSuccessResponseCountingCallback callback = new BooleanSuccessResponseCountingCallback(instances.length);

        for (HazelcastInstance instance : instances) {
            IExecutorService service = instance.getExecutorService("testSubmitToMemberCallable");
            Member localMember = instance.getCluster().getLocalMember();
            service.submitToMember(new MemberUUIDCheckCallable(localMember.getUuid()), localMember, callback);
        }

        assertOpenEventually(callback.getResponseLatch());
        assertEquals(instances.length, callback.getSuccessResponseCount());
    }

    @Test
    public void submitToMembers_callable() {
        int sum = 0;
        ResponseCountingMultiExecutionCallback callback = new ResponseCountingMultiExecutionCallback(instances.length);

        Set<Member> membersSet = instances[0].getCluster().getMembers();
        Member[] members = membersSet.toArray(new Member[0]);
        Random random = new Random();
        String name = "testSubmitToMembersCallable";
        for (HazelcastInstance instance : instances) {
            IExecutorService service = instance.getExecutorService(name);
            int n = random.nextInt(instances.length) + 1;
            sum += n;
            Member[] m = new Member[n];
            System.arraycopy(members, 0, m, 0, n);
            service.submitToMembers(new IncrementAtomicLongCallable(name), Arrays.asList(m), callback);
        }
        assertOpenEventually(callback.getLatch());

        IAtomicLong result = instances[0].getAtomicLong(name);
        assertEquals(sum, result.get());
        assertEquals(sum, callback.getCount());
    }

    @Test
    public void submitToAllMembers_callable() {
        ResponseCountingMultiExecutionCallback callback = new ResponseCountingMultiExecutionCallback(instances.length);

        for (HazelcastInstance instance : instances) {
            IExecutorService service = instance.getExecutorService("testSubmitToAllMembersCallable");
            service.submitToAllMembers(new IncrementAtomicLongCallable("testSubmitToAllMembersCallable"), callback);
        }
        assertOpenEventually(callback.getLatch());

        IAtomicLong result = instances[0].getAtomicLong("testSubmitToAllMembersCallable");
        assertEquals(instances.length * instances.length, result.get());
        assertEquals(instances.length * instances.length, callback.getCount());
    }

    @Test
    public void submitToAllMembers_statefulCallable() {
        IExecutorService executorService = instances[0].getExecutorService(randomString());
        InternallyCountingCallable internallyCountingCallable = new InternallyCountingCallable();

        final CountDownLatch completedLatch = new CountDownLatch(1);
        final AtomicBoolean failed = new AtomicBoolean();
        // local execution of callable may change the state of callable before sent to other members
        // we avoid this by serializing beforehand
        executorService.submitToAllMembers(internallyCountingCallable, new MultiExecutionCallback() {
            @Override
            public void onResponse(Member member, Object value) {
                if ((Integer) value != 1) {
                    failed.set(true);
                }
            }

            @Override
            public void onComplete(Map<Member, Object> values) {
                completedLatch.countDown();
            }
        });

        assertOpenEventually(completedLatch);
        assertFalse(failed.get());
    }

    private static class InternallyCountingCallable implements Callable<Integer>, Serializable {

        private int state;

        @Override
        public Integer call() throws Exception {
            return ++state;
        }
    }

    @Test
    public void submitToAllMembers_NonSerializableResponse() {
        IExecutorService executorService = instances[0].getExecutorService(randomString());
        NonSerializableResponseCallable nonSerializableResponseCallable = new NonSerializableResponseCallable();

        final AtomicLong exceptionCount = new AtomicLong();
        final AtomicLong responseCount = new AtomicLong();
        final CountDownLatch completedLatch = new CountDownLatch(1);
        executorService.submitToAllMembers(nonSerializableResponseCallable, new MultiExecutionCallback() {
            @Override
            public void onResponse(Member member, Object value) {
                if (value instanceof HazelcastSerializationException) {
                    exceptionCount.incrementAndGet();
                } else {
                    responseCount.incrementAndGet();
                }
            }

            @Override
            public void onComplete(Map<Member, Object> values) {
                completedLatch.countDown();
            }
        });

        assertOpenEventually(completedLatch);
        // two exceptions from remote nodes
        assertEquals(2, exceptionCount.get());
        // one response from local node since, it does not need to serialize/deserialize the response
        assertEquals(1, responseCount.get());
    }

    private static class NonSerializableResponseCallable implements Callable, Serializable {

        @Override
        public Object call() throws Exception {
            return new NonSerializableResponse();
        }
    }

    private static class NonSerializableResponse {

    }
}
