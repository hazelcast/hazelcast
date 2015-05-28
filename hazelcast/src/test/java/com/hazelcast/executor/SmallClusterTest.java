/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.test.HazelcastParallelClassRunner;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class SmallClusterTest
        extends ExecutorServiceTestSupport {

    public static final int NODE_COUNT = 3;

    private HazelcastInstance[] instances;

    @Before
    public void setup() {
        instances = createHazelcastInstanceFactory(NODE_COUNT).newInstances(new Config());
    }

    @Test
    public void executionCallback_notified()
            throws Exception {
        IExecutorService executorService = instances[1].getExecutorService(randomString());
        BasicTestCallable task = new BasicTestCallable();
        String key = generateKeyOwnedBy(instances[0]);
        ICompletableFuture<String> future = (ICompletableFuture<String>) executorService.submitToKeyOwner(task, key);
        final CountingDownExecutionCallback<String> callback = new CountingDownExecutionCallback<String>(1);
        future.andThen(callback);
        future.get();
        assertOpenEventually(callback.getLatch(), 10);
    }

    @Test
    public void submitToSeveralNodes_runnable()
            throws Exception {
        for (HazelcastInstance instance : instances) {
            final IExecutorService service = instance.getExecutorService("testExecuteMultipleNode");

            final int rand = new Random().nextInt(100);
            final Future<Integer> future = service.submit(new IncrementAtomicLongRunnable("count"), rand);
            assertEquals(Integer.valueOf(rand), future.get());
        }
        final IAtomicLong count = instances[0].getAtomicLong("count");
        assertEquals(instances.length, count.get());
    }

    @Test
    public void submitToKeyOwner_runnable()
            throws Exception {
        final NullResponseCountingCallback callback = new NullResponseCountingCallback(instances.length);

        for (final HazelcastInstance instance : instances) {
            final IExecutorService service = instance.getExecutorService("testSubmitToKeyOwnerRunnable");
            final Member localMember = instance.getCluster().getLocalMember();
            final int key = findNextKeyForMember(instance, localMember);

            service.submitToKeyOwner(
                    new IncrementAtomicLongIfMemberUUIDNotMatchRunnable(localMember.getUuid(), "testSubmitToKeyOwnerRunnable"),
                    key, callback);
        }


        assertOpenEventually(callback.getResponseLatch());
        assertEquals(0, instances[0].getAtomicLong("testSubmitToKeyOwnerRunnable").get());
        assertEquals(instances.length, callback.getNullResponseCount());
    }

    @Test
    public void submitToMember_runnable()
            throws Exception {
        final NullResponseCountingCallback callback = new NullResponseCountingCallback(instances.length);

        for (final HazelcastInstance instance : instances) {
            final IExecutorService service = instance.getExecutorService("testSubmitToMemberRunnable");
            final Member localMember = instance.getCluster().getLocalMember();

            service.submitToMember(
                    new IncrementAtomicLongIfMemberUUIDNotMatchRunnable(localMember.getUuid(), "testSubmitToMemberRunnable"),
                    localMember, callback);
        }
        assertOpenEventually(callback.getResponseLatch());
        assertEquals(0, instances[0].getAtomicLong("testSubmitToMemberRunnable").get());
        assertEquals(instances.length, callback.getNullResponseCount());
    }

    @Test
    public void submitToMembers_runnable()
            throws Exception {
        int sum = 0;
        final Set<Member> membersSet = instances[0].getCluster().getMembers();
        final Member[] members = membersSet.toArray(new Member[membersSet.size()]);
        final Random random = new Random();

        final ResponseCountingMultiExecutionCallback callback = new ResponseCountingMultiExecutionCallback(instances.length);
        for (HazelcastInstance instance : instances) {
            final IExecutorService service = instance.getExecutorService("testSubmitToMembersRunnable");
            final int n = random.nextInt(instances.length) + 1;
            sum += n;
            Member[] m = new Member[n];
            System.arraycopy(members, 0, m, 0, n);
            service.submitToMembers(new IncrementAtomicLongRunnable("testSubmitToMembersRunnable"), Arrays.asList(m), callback);
        }

        assertOpenEventually(callback.getLatch());
        final IAtomicLong result = instances[0].getAtomicLong("testSubmitToMembersRunnable");
        assertEquals(sum, result.get());
        assertEquals(sum, callback.getCount());
    }

    @Test
    public void submitToAllMembers_runnable()
            throws Exception {
        final ResponseCountingMultiExecutionCallback callback = new ResponseCountingMultiExecutionCallback(instances.length);

        for (HazelcastInstance instance : instances) {
            final IExecutorService service = instance.getExecutorService("testSubmitToAllMembersRunnable");
            service.submitToAllMembers(new IncrementAtomicLongRunnable("testSubmitToAllMembersRunnable"), callback);
        }
        assertOpenEventually(callback.getLatch());
        final IAtomicLong result = instances[0].getAtomicLong("testSubmitToAllMembersRunnable");
        assertEquals(instances.length * instances.length, result.get());
        assertEquals(instances.length * instances.length, callback.getCount());
    }

    @Test
    public void submitToSeveralNodes_callable()
            throws Exception {
        for (int i = 0; i < instances.length; i++) {
            final IExecutorService service = instances[i].getExecutorService("testSubmitMultipleNode");
            final Future future = service.submit(new IncrementAtomicLongCallable("testSubmitMultipleNode"));
            assertEquals((long) (i + 1), future.get());
        }
    }

    @Test(timeout = 30000)
    public void submitToKeyOwner_callable()
            throws Exception {
        final List<Future> futures = new ArrayList<Future>();

        for (int i = 0; i < instances.length; i++) {
            final HazelcastInstance instance = instances[i];
            final IExecutorService service = instance.getExecutorService("testSubmitToKeyOwnerCallable");
            final Member localMember = instance.getCluster().getLocalMember();
            final int key = findNextKeyForMember(instance, localMember);

            final Future f = service.submitToKeyOwner(new MemberUUIDCheckCallable(localMember.getUuid()), key);
            futures.add(f);
        }

        for (Future future : futures) {
            assertTrue((Boolean) future.get(60, TimeUnit.SECONDS));
        }
    }

    @Test(timeout = 30000)
    public void submitToKeyOwner_callable_withCallback()
            throws Exception {
        final BooleanSuccessResponseCountingCallback callback = new BooleanSuccessResponseCountingCallback(instances.length);

        for (int i = 0; i < instances.length; i++) {
            final HazelcastInstance instance = instances[i];
            final IExecutorService service = instance.getExecutorService("testSubmitToKeyOwnerCallable");
            final Member localMember = instance.getCluster().getLocalMember();
            final int key = findNextKeyForMember(instance, localMember);

            service.submitToKeyOwner(new MemberUUIDCheckCallable(localMember.getUuid()), key, callback);

        }

        assertOpenEventually(callback.getResponseLatch());
        assertEquals(instances.length, callback.getSuccessResponseCount());
    }

    @Test(timeout = 30000)
    public void submitToMember_callable()
            throws Exception {
        final List<Future> futures = new ArrayList<Future>();

        for (int i = 0; i < instances.length; i++) {
            final HazelcastInstance instance = instances[i];
            final IExecutorService service = instance.getExecutorService("testSubmitToMemberCallable");
            final Member localMember = instance.getCluster().getLocalMember();

            final Future f = service.submitToMember(new MemberUUIDCheckCallable(localMember.getUuid()), localMember);
            futures.add(f);
        }

        for (Future future : futures) {
            assertTrue((Boolean) future.get());
        }
    }

    @Test(timeout = 30000)
    public void submitToMember_callable_withCallback()
            throws Exception {
        final BooleanSuccessResponseCountingCallback callback = new BooleanSuccessResponseCountingCallback(instances.length);

        for (int i = 0; i < instances.length; i++) {
            final HazelcastInstance instance = instances[i];
            final IExecutorService service = instance.getExecutorService("testSubmitToMemberCallable");
            final Member localMember = instance.getCluster().getLocalMember();

            service.submitToMember(new MemberUUIDCheckCallable(localMember.getUuid()), localMember, callback);

        }
        assertOpenEventually(callback.getResponseLatch());
        assertEquals(instances.length, callback.getSuccessResponseCount());
    }

    @Test
    public void submitToMembers_callable()
            throws Exception {
        int sum = 0;
        final ResponseCountingMultiExecutionCallback callback = new ResponseCountingMultiExecutionCallback(instances.length);

        final Set<Member> membersSet = instances[0].getCluster().getMembers();
        final Member[] members = membersSet.toArray(new Member[membersSet.size()]);
        final Random random = new Random();
        final String name = "testSubmitToMembersCallable";
        for (HazelcastInstance instance : instances) {
            final IExecutorService service = instance.getExecutorService(name);
            final int n = random.nextInt(instances.length) + 1;
            sum += n;
            Member[] m = new Member[n];
            System.arraycopy(members, 0, m, 0, n);
            service.submitToMembers(new IncrementAtomicLongCallable(name), Arrays.asList(m), callback);
        }

        assertOpenEventually(callback.getLatch());
        final IAtomicLong result = instances[0].getAtomicLong(name);
        assertEquals(sum, result.get());
        assertEquals(sum, callback.getCount());
    }

    @Test
    public void submitToAllMembers_callable()
            throws Exception {

        final ResponseCountingMultiExecutionCallback callback = new ResponseCountingMultiExecutionCallback(instances.length);

        for (HazelcastInstance instance : instances) {
            final IExecutorService service = instance.getExecutorService("testSubmitToAllMembersCallable");
            service.submitToAllMembers(new IncrementAtomicLongCallable("testSubmitToAllMembersCallable"), callback);
        }

        assertOpenEventually(callback.getLatch());
        final IAtomicLong result = instances[0].getAtomicLong("testSubmitToAllMembersCallable");
        assertEquals(instances.length * instances.length, result.get());
        assertEquals(instances.length * instances.length, callback.getCount());
    }

    @Test
    public void submitToAllMembers_statefulCallable()
            throws Exception {
        IExecutorService executorService = instances[0].getExecutorService(randomString());
        InternallyCountingCallable internallyCountingCallable = new InternallyCountingCallable();
        final CountDownLatch completedLatch = new CountDownLatch(1);
        final AtomicBoolean failed = new AtomicBoolean();
        // Local execution of callable may change the state of callable before sent to other members
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

    private static class InternallyCountingCallable
            implements Callable<Integer>, Serializable {
        private int state;

        @Override
        public Integer call()
                throws Exception {
            return ++state;
        }
    }
}
