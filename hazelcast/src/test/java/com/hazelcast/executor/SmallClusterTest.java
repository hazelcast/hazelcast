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
import com.hazelcast.core.ExecutionCallback;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class SmallClusterTest extends ExecutorServiceTestSupport {

    public static final int NODE_COUNT = 3;

    private HazelcastInstance[] instances;
    private AtomicInteger count;
    private CountDownLatch latch;
    private final ExecutionCallback callback = new ExecutionCallback() {
        public void onResponse(Object response) {
            if (response == null || response instanceof Boolean && (Boolean)response) {
                count.incrementAndGet();
            }
            latch.countDown();
        }
        public void onFailure(Throwable t) { }
    };
    private final MultiExecutionCallback multiCallback = new MultiExecutionCallback() {
        public void onResponse(Member member, Object value) {
            count.incrementAndGet();
        }
        public void onComplete(Map<Member, Object> values) {
            latch.countDown();
        }
    };

    @Before public void setup() {
        instances = createHazelcastInstanceFactory(NODE_COUNT).newInstances(new Config());
        count = new AtomicInteger(0);
        latch = new CountDownLatch(instances.length);
    }

    @Test
    public void executionCallback_notified() throws Exception {
        IExecutorService executorService = instances[1].getExecutorService(randomString());
        BasicTestTask task = new BasicTestTask();
        String key = generateKeyOwnedBy(instances[0]);
        ICompletableFuture<String> future = (ICompletableFuture<String>) executorService.submitToKeyOwner(task, key);
        final CountDownLatch latch = new CountDownLatch(1);
        future.andThen(new ExecutionCallback<String>() {
            @Override public void onResponse(String response) {
                latch.countDown();
            }
            @Override public void onFailure(Throwable t) { }
        });
        future.get();
        assertOpenEventually(latch, 10);
    }

    @Test
    public void submitToSeveralNodes_runnable() throws Exception {
        for (HazelcastInstance instance : instances) {
            final IExecutorService service = instance.getExecutorService("testExecuteMultipleNode");
            final String script = "hazelcast.getAtomicLong('count').incrementAndGet();";
            final int rand = new Random().nextInt(100);
            final Future<Integer> future = service.submit(new ScriptRunnable(script, null), rand);
            assertEquals(Integer.valueOf(rand), future.get());
        }
        final IAtomicLong count = instances[0].getAtomicLong("count");
        assertEquals(instances.length, count.get());
    }

    @Test
    public void submitToKeyOwner_runnable() throws Exception {
        for (final HazelcastInstance instance : instances) {
            final IExecutorService service = instance.getExecutorService("testSubmitToKeyOwnerRunnable");
            final String script = "if(!hazelcast.getCluster().getLocalMember().getUuid().equals(memberUUID)) " +
                    "hazelcast.getAtomicLong('testSubmitToKeyOwnerRunnable').incrementAndGet();";
            final HashMap<String,String> map = new HashMap<String,String>();
            final Member localMember = instance.getCluster().getLocalMember();
            map.put("memberUUID", localMember.getUuid());
            int key = 0;
            while (!localMember.equals(instance.getPartitionService().getPartition(++key).getOwner())) {
                Thread.sleep(1);
            }
            service.submitToKeyOwner(new ScriptRunnable(script, map), key, callback);
        }
        assertOpenEventually(latch);
        assertEquals(0, instances[0].getAtomicLong("testSubmitToKeyOwnerRunnable").get());
        assertEquals(instances.length, count.get());
    }

    @Test
    public void submitToMember_runnable() throws Exception {
        for (final HazelcastInstance instance : instances) {
            final IExecutorService service = instance.getExecutorService("testSubmitToMemberRunnable");
            final String script = "if(!hazelcast.getCluster().getLocalMember().getUuid().equals(memberUUID)) " +
                    "hazelcast.getAtomicLong('testSubmitToMemberRunnable').incrementAndGet();";
            final HashMap<String, String> map = new HashMap<String, String>();
            map.put("memberUUID", instance.getCluster().getLocalMember().getUuid());
            service.submitToMember(new ScriptRunnable(script, map), instance.getCluster().getLocalMember(), callback);
        }
        assertOpenEventually(latch);
        assertEquals(0, instances[0].getAtomicLong("testSubmitToMemberRunnable").get());
        assertEquals(instances.length, count.get());
    }

    @Test
    public void submitToMembers_runnable() throws Exception {
        int sum = 0;
        final Set<Member> membersSet = instances[0].getCluster().getMembers();
        final Member[] members = membersSet.toArray(new Member[membersSet.size()]);
        final Random random = new Random();
        for (HazelcastInstance instance : instances) {
            final IExecutorService service = instance.getExecutorService("testSubmitToMembersRunnable");
            final String script = "hazelcast.getAtomicLong('testSubmitToMembersRunnable').incrementAndGet();";
            final int n = random.nextInt(instances.length) + 1;
            sum += n;
            Member[] m = new Member[n];
            System.arraycopy(members, 0, m, 0, n);
            service.submitToMembers(new ScriptRunnable(script, null), Arrays.asList(m), multiCallback);
        }

        assertTrue(latch.await(30, TimeUnit.SECONDS));
        final IAtomicLong result = instances[0].getAtomicLong("testSubmitToMembersRunnable");
        assertEquals(sum, result.get());
        assertEquals(sum, count.get());
    }

    @Test
    public void submitToAllMembers_runnable() throws Exception {
        for (HazelcastInstance instance : instances) {
            final IExecutorService service = instance.getExecutorService("testSubmitToAllMembersRunnable");
            final String script = "hazelcast.getAtomicLong('testSubmitToAllMembersRunnable').incrementAndGet();";
            service.submitToAllMembers(new ScriptRunnable(script, null), multiCallback);
        }
        assertTrue(latch.await(30, TimeUnit.SECONDS));
        final IAtomicLong result = instances[0].getAtomicLong("testSubmitToAllMembersRunnable");
        assertEquals(instances.length * instances.length, result.get());
        assertEquals(instances.length * instances.length, count.get());
    }

    @Test
    public void submitToSeveralNodes_callable() throws Exception {
        for (int i = 0; i < instances.length; i++) {
            final IExecutorService service = instances[i].getExecutorService("testSubmitMultipleNode");
            final String script = "hazelcast.getAtomicLong('testSubmitMultipleNode').incrementAndGet();";
            final Future future = service.submit(new ScriptCallable(script, null));
            assertEquals((long) (i + 1), future.get());
        }
    }

    @Test(timeout = 30000)
    public void submitToKeyOwner_callable() throws Exception {
        latch = new CountDownLatch(instances.length / 2);
        for (int i = 0; i < instances.length; i++) {
            final HazelcastInstance instance = instances[i];
            final IExecutorService service = instance.getExecutorService("testSubmitToKeyOwnerCallable");
            final String script = "hazelcast.getCluster().getLocalMember().getUuid().equals(memberUUID)";
            final HashMap<String,String> map = new HashMap<String,String>();
            final Member localMember = instance.getCluster().getLocalMember();
            map.put("memberUUID", localMember.getUuid());
            int key = 0;
            while (!localMember.equals(instance.getPartitionService().getPartition(++key).getOwner())) ;
            if (i % 2 == 0) {
                final Future f = service.submitToKeyOwner(new ScriptCallable(script, map), key);
                assertTrue((Boolean) f.get(60, TimeUnit.SECONDS));
            } else {
                service.submitToKeyOwner(new ScriptCallable(script, map), key, callback);
            }
        }
        assertOpenEventually(latch);
        assertEquals(instances.length / 2, count.get());
    }

    @Test(timeout = 30000)
    public void submitToMember_callable() throws Exception {
        latch = new CountDownLatch(instances.length / 2);
        for (int i = 0; i < instances.length; i++) {
            final HazelcastInstance instance = instances[i];
            final IExecutorService service = instance.getExecutorService("testSubmitToMemberCallable");
            final String script = "hazelcast.getCluster().getLocalMember().getUuid().equals(memberUUID); ";
            final HashMap<String,String> map = new HashMap<String,String>();
            map.put("memberUUID", instance.getCluster().getLocalMember().getUuid());
            if (i % 2 == 0) {
                final Future f = service.submitToMember(new ScriptCallable(script, map), instance.getCluster().getLocalMember());
                assertTrue((Boolean) f.get());
            } else {
                service.submitToMember(new ScriptCallable(script, map), instance.getCluster().getLocalMember(), callback);
            }
        }
        assertOpenEventually(latch);
        assertEquals(instances.length / 2, count.get());
    }

    @Test
    public void submitToMembers_callable() throws Exception {
        int sum = 0;
        final Set<Member> membersSet = instances[0].getCluster().getMembers();
        final Member[] members = membersSet.toArray(new Member[membersSet.size()]);
        final Random random = new Random();
        final String name = "testSubmitToMembersCallable";
        for (HazelcastInstance instance : instances) {
            final IExecutorService service = instance.getExecutorService(name);
            final String script = "hazelcast.getAtomicLong('" + name + "').incrementAndGet();";
            final int n = random.nextInt(instances.length) + 1;
            sum += n;
            Member[] m = new Member[n];
            System.arraycopy(members, 0, m, 0, n);
            service.submitToMembers(new ScriptCallable(script, null), Arrays.asList(m), multiCallback);
        }

        assertTrue(latch.await(30, TimeUnit.SECONDS));
        final IAtomicLong result = instances[0].getAtomicLong(name);
        assertEquals(sum, result.get());
        assertEquals(sum, count.get());
    }

    @Test
    public void submitToAllMembers_callable() throws Exception {
        for (HazelcastInstance instance : instances) {
            final IExecutorService service = instance.getExecutorService("testSubmitToAllMembersCallable");
            final String script = "hazelcast.getAtomicLong('testSubmitToAllMembersCallable').incrementAndGet();";
            service.submitToAllMembers(new ScriptCallable(script, null), multiCallback);
        }
        latch.await(30, TimeUnit.SECONDS);
        final IAtomicLong result = instances[0].getAtomicLong("testSubmitToAllMembersCallable");
        assertEquals(instances.length * instances.length, result.get());
        assertEquals(instances.length * instances.length, count.get());
    }

    @Test
    public void submitToAllMembers_statefulCallable() throws Exception {
        IExecutorService executorService = instances[0].getExecutorService(randomString());
        MyTask myTask = new MyTask();
        final CountDownLatch completedLatch = new CountDownLatch(1);
        final AtomicBoolean failed = new AtomicBoolean();
        // Local execution of callable may change the state of callable before sent to other members
        // we avoid this by serializing beforehand
        executorService.submitToAllMembers(myTask, new MultiExecutionCallback() {
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
        completedLatch.await(1, TimeUnit.MINUTES);
        assertFalse(failed.get());
    }
    private static class MyTask implements Callable<Integer>, Serializable {
        private int state;
        @Override public Integer call() throws Exception {
            return ++state;
        }
    }
}
