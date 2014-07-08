/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.executor;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.executor.tasks.AppendCallable;
import com.hazelcast.client.executor.tasks.GetMemberUuidTask;
import com.hazelcast.client.executor.tasks.MapPutPartitionAwareCallable;
import com.hazelcast.client.executor.tasks.MapPutPartitionAwareRunnable;
import com.hazelcast.client.executor.tasks.MapPutRunnable;
import com.hazelcast.client.executor.tasks.NullCallable;
import com.hazelcast.client.executor.tasks.SelectAllMembers;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberSelector;
import com.hazelcast.core.MultiExecutionCallback;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.test.HazelcastTestSupport.assertOpenEventually;
import static com.hazelcast.test.HazelcastTestSupport.assertSizeEventually;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ClientExecutorServiceSubmitTest {

    static final int CLUSTER_SIZE = 3;
    static HazelcastInstance instance1;
    static HazelcastInstance instance2;
    static HazelcastInstance instance3;
    static HazelcastInstance client;

    @BeforeClass
    public static void init() {
        instance1 = Hazelcast.newHazelcastInstance();
        instance2 = Hazelcast.newHazelcastInstance();
        instance3 = Hazelcast.newHazelcastInstance();
        client = HazelcastClient.newHazelcastClient();
    }

    @AfterClass
    public static void destroy() {
        client.shutdown();
        Hazelcast.shutdownAll();
    }

    @Test(expected = NullPointerException.class)
    public void testSubmitCallableNullTask() throws Exception {
        final IExecutorService service = client.getExecutorService(randomString());
        final Callable<String> callable = null;

        service.submit(callable);
    }

    @Test
    public void testSubmitCallableToMember() throws Exception {
        final IExecutorService service = client.getExecutorService(randomString());

        final Callable<String> getUuidCallable = new GetMemberUuidTask();
        final Member member = instance2.getCluster().getLocalMember();

        final Future<String> result = service.submitToMember(getUuidCallable, member);

        assertEquals(member.getUuid(), result.get());
    }

    @Test
    public void testSubmitCallableToMembers() throws Exception {
        final IExecutorService service = client.getExecutorService(randomString());

        final Callable<String> getUuidCallable = new GetMemberUuidTask();
        final Collection<Member> collection = instance2.getCluster().getMembers();

        final Map<Member, Future<String>> map = service.submitToMembers(getUuidCallable, collection);

        for (final Member member : map.keySet()) {
            final Future<String> result = map.get(member);
            String uuid = result.get();

            assertEquals(member.getUuid(), uuid);
        }
    }

    @Test
    public void testSubmitCallable_withMemberSelector() throws Exception {
        final IExecutorService service = client.getExecutorService(randomString());

        final String msg = randomString();
        final Callable<String> callable = new AppendCallable(msg);
        final MemberSelector selectAll = new SelectAllMembers();

        final Future<String> f = service.submit(callable, selectAll);

        assertEquals(msg + AppendCallable.APPENDAGE, f.get());
    }

    @Test
    public void testSubmitCallableToMembers_withMemberSelector() throws Exception {
        final IExecutorService service = client.getExecutorService(randomString());

        final Callable<String> getUuidCallable = new GetMemberUuidTask();
        final MemberSelector selectAll = new SelectAllMembers();

        final Map<Member, Future<String>> map = service.submitToMembers(getUuidCallable, selectAll);

        for (final Member member : map.keySet()) {
            final Future<String> result = map.get(member);
            String uuid = result.get();

            assertEquals(member.getUuid(), uuid);
        }
    }

    @Test
    public void submitCallableToAllMembers() throws Exception {
        final IExecutorService service = client.getExecutorService(randomString());

        final String msg = randomString();
        final Callable<String> callable = new AppendCallable(msg);

        final Map<Member, Future<String>> map = service.submitToAllMembers(callable);
        for (final Member member : map.keySet()) {
            final Future<String> result = map.get(member);
            assertEquals(msg + AppendCallable.APPENDAGE, result.get());
        }
    }

    @Test
    public void submitRunnableToMember_withExecutionCallback() {
        final IExecutorService service = client.getExecutorService(randomString());

        final String mapName = randomString();
        final Runnable runnable = new MapPutRunnable(mapName);
        final Member member = instance2.getCluster().getLocalMember();
        final CountDownLatch responseLatch = new CountDownLatch(1);

        service.submitToMember(runnable, member, new ExecutionCallback() {
            public void onResponse(final Object response) {
                responseLatch.countDown();
            }

            public void onFailure(final Throwable t) {}
        });
        final Map map = client.getMap(mapName);

        assertOpenEventually("responseLatch", responseLatch);
        assertEquals(1, map.size());
    }

    @Test
    public void submitRunnableToMembers_withMultiExecutionCallback() {
        final IExecutorService service = client.getExecutorService(randomString());

        final String mapName = randomString();
        final Runnable runnable = new MapPutRunnable(mapName);
        final Collection<Member> collection = instance2.getCluster().getMembers();
        final CountDownLatch responseLatch = new CountDownLatch(CLUSTER_SIZE);
        final CountDownLatch completeLatch = new CountDownLatch(1);

        service.submitToMembers(runnable, collection, new MultiExecutionCallback() {
            public void onResponse(final Member member, final Object value) {
                responseLatch.countDown();
            }

            public void onComplete(final Map<Member, Object> values) {
                completeLatch.countDown();
            }
        });
        final Map map = client.getMap(mapName);

        assertOpenEventually("responseLatch", responseLatch);
        assertOpenEventually("completeLatch", completeLatch);
        assertEquals(CLUSTER_SIZE, map.size());
    }

    @Test
    public void testSubmitCallableToMember_withExecutionCallback() throws Exception {
        final IExecutorService service = client.getExecutorService(randomString());

        final Callable getUuidCallable = new GetMemberUuidTask();
        final Member member = instance2.getCluster().getLocalMember();
        final CountDownLatch responseLatch = new CountDownLatch(1);
        final AtomicReference<Object> result = new AtomicReference<Object>();

        service.submitToMember(getUuidCallable, member, new ExecutionCallback() {
            @Override
            public void onResponse(final Object response) {
                result.set(response);
                responseLatch.countDown();
            }

            @Override
            public void onFailure(final Throwable t) {}
        });

        assertOpenEventually("responseLatch", responseLatch);
        assertEquals(member.getUuid(), result.get());
    }

    @Test
    public void submitCallableToMember_withMultiExecutionCallback() throws Exception {
        final IExecutorService service = client.getExecutorService(randomString());

        final CountDownLatch responseLatch = new CountDownLatch(CLUSTER_SIZE);
        final CountDownLatch completeLatch = new CountDownLatch(CLUSTER_SIZE);
        final String msg = randomString();
        final Callable<String> callable = new AppendCallable(msg);
        final Collection<Member> collection = instance2.getCluster().getMembers();

        service.submitToMembers(callable, collection, new MultiExecutionCallback() {
            public void onResponse(final Member member, final Object value) {
                if (value.equals(msg + AppendCallable.APPENDAGE)) {
                    responseLatch.countDown();
                }
            }

            public void onComplete(final Map<Member, Object> values) {
                for (final Member member : values.keySet()) {
                    final Object value = values.get(member);
                    if (value.equals(msg + AppendCallable.APPENDAGE)) {
                        completeLatch.countDown();
                    }
                }
            }
        });

        assertOpenEventually("responseLatch", responseLatch);
        assertOpenEventually("completeLatch", completeLatch);
    }

    @Test
    public void submitRunnable_withExecutionCallback() {
        final IExecutorService service = client.getExecutorService(randomString());

        final CountDownLatch responseLatch = new CountDownLatch(1);
        final String mapName = randomString();
        final Runnable runnable = new MapPutRunnable(mapName);
        final MemberSelector selector = new SelectAllMembers();

        service.submit(runnable, selector, new ExecutionCallback() {
            public void onResponse(final Object response) {
                responseLatch.countDown();
            }

            public void onFailure(final Throwable t) {}
        });
        final IMap map = client.getMap(mapName);

        assertOpenEventually("responseLatch", responseLatch);
        assertEquals(1, map.size());
    }

    @Test
    public void submitRunnableToMembers_withExecutionCallback() {
        final IExecutorService service = client.getExecutorService(randomString());

        final CountDownLatch responseLatch = new CountDownLatch(CLUSTER_SIZE);
        final CountDownLatch completeLatch = new CountDownLatch(1);
        final String mapName = randomString();
        final Runnable runnable = new MapPutRunnable(mapName);
        final MemberSelector selector = new SelectAllMembers();

        service.submitToMembers(runnable, selector, new MultiExecutionCallback() {
            public void onResponse(final Member member, final Object value) {
                responseLatch.countDown();
            }

            public void onComplete(final Map<Member, Object> values) {
                completeLatch.countDown();
            }
        });
        final IMap map = client.getMap(mapName);

        assertOpenEventually("responseLatch", responseLatch);
        assertOpenEventually("completeLatch", completeLatch);
        assertEquals(CLUSTER_SIZE, map.size());
    }

    @Test
    public void submitCallable_withExecutionCallback() {
        final IExecutorService service = client.getExecutorService(randomString());

        final CountDownLatch responseLatch = new CountDownLatch(1);
        final String msg = randomString();
        final Callable runnable = new AppendCallable(msg);
        final MemberSelector selector = new SelectAllMembers();
        final AtomicReference<Object> result = new AtomicReference<Object>();

        service.submit(runnable, selector, new ExecutionCallback() {
            public void onResponse(final Object response) {
                result.set(response);
                responseLatch.countDown();
            }

            public void onFailure(final Throwable t) {}
        });

        assertOpenEventually("responseLatch", responseLatch);
        assertEquals(msg + AppendCallable.APPENDAGE, result.get());
    }

    @Test
    public void submitCallableToMembers_withExecutionCallback() {
        final IExecutorService service = client.getExecutorService(randomString());

        final CountDownLatch responseLatch = new CountDownLatch(CLUSTER_SIZE);
        final CountDownLatch completeLatch = new CountDownLatch(1);
        final String msg = randomString();
        final Callable callable = new AppendCallable(msg);
        final MemberSelector selector = new SelectAllMembers();

        service.submitToMembers(callable, selector, new MultiExecutionCallback() {
            public void onResponse(final Member member, final Object value) {
                if (value.equals(msg + AppendCallable.APPENDAGE)) {
                    responseLatch.countDown();
                }
            }

            public void onComplete(final Map<Member, Object> values) {
                completeLatch.countDown();
            }
        });

        assertOpenEventually("responseLatch", responseLatch);
        assertOpenEventually("completeLatch", completeLatch);
    }

    @Test
    public void submitRunnableToAllMembers_withMultiExecutionCallback() throws Exception {
        final IExecutorService service = client.getExecutorService(randomString());

        final CountDownLatch responseLatch = new CountDownLatch(CLUSTER_SIZE);
        final CountDownLatch completeLatch = new CountDownLatch(1);
        final String mapName = randomString();
        final Runnable runnable = new MapPutRunnable(mapName);

        service.submitToAllMembers(runnable, new MultiExecutionCallback() {
            public void onResponse(final Member member, final Object value) {
                responseLatch.countDown();
            }

            public void onComplete(final Map<Member, Object> values) {
                completeLatch.countDown();
            }
        });
        final IMap map = client.getMap(mapName);

        assertOpenEventually("responseLatch", responseLatch);
        assertOpenEventually("completeLatch", completeLatch);
        assertEquals(CLUSTER_SIZE, map.size());
    }

    @Test
    public void submitCallableToAllMembers_withMultiExecutionCallback() throws Exception {
        final IExecutorService service = client.getExecutorService(randomString());

        final CountDownLatch responseLatch = new CountDownLatch(CLUSTER_SIZE);
        final CountDownLatch completeLatch = new CountDownLatch(CLUSTER_SIZE);
        final String msg = randomString();
        final Callable callable = new AppendCallable(msg);

        service.submitToAllMembers(callable, new MultiExecutionCallback() {
            public void onResponse(final Member member, final Object value) {
                if (value.equals(msg + AppendCallable.APPENDAGE)) {
                    responseLatch.countDown();
                }
            }

            public void onComplete(final Map<Member, Object> values) {
                for (Member member : values.keySet()) {
                    Object value = values.get(member);
                    if (value.equals(msg + AppendCallable.APPENDAGE)) {
                        completeLatch.countDown();
                    }
                }
            }
        });

        assertOpenEventually("responseLatch", responseLatch);
        assertOpenEventually("completeLatch", completeLatch);
    }

    @Test
    public void submitCallableWithNullResultToAllMembers_withMultiExecutionCallback() throws Exception {
        final IExecutorService service = client.getExecutorService(randomString());

        final CountDownLatch responseLatch = new CountDownLatch(CLUSTER_SIZE);
        final CountDownLatch completeLatch = new CountDownLatch(CLUSTER_SIZE);
        final Callable callable = new NullCallable();

        service.submitToAllMembers(callable, new MultiExecutionCallback() {
            public void onResponse(final Member member, final Object value) {
                if (value == null) {
                    responseLatch.countDown();
                }
            }

            public void onComplete(final Map<Member, Object> values) {
                for (Member member : values.keySet()) {
                    Object value = values.get(member);
                    if (value == null) {
                        completeLatch.countDown();
                    }
                }
            }
        });

        assertOpenEventually("responseLatch", responseLatch);
        assertOpenEventually("completeLatch", completeLatch);
    }

    @Test
    public void submitRunnable() {
        final IExecutorService service = client.getExecutorService(randomString());

        final String mapName = randomString();
        final Runnable runnable = new MapPutRunnable(mapName);

        service.submit(runnable);

        final IMap map = client.getMap(mapName);

        assertSizeEventually(1, map);
    }

    @Test
    public void testSubmitRunnable_WithResult() throws ExecutionException, InterruptedException {
        final IExecutorService service = client.getExecutorService(randomString());

        final String mapName = randomString();
        final Object givenResult = "givenResult";
        final Future future = service.submit(new MapPutRunnable(mapName), givenResult);
        final Object result = future.get();

        final IMap map = client.getMap(mapName);

        assertEquals(givenResult, result);
        assertEquals(1, map.size());
    }

    @Test
    public void testSubmitCallable() throws Exception {
        final IExecutorService service = client.getExecutorService(randomString());

        final String msg = randomString();
        final Callable callable = new AppendCallable(msg);
        final Future result = service.submit(callable);

        assertEquals(msg + AppendCallable.APPENDAGE, result.get());
    }

    @Test
    public void testSubmitRunnable_withExecutionCallback() throws Exception {
        final IExecutorService service = client.getExecutorService(randomString());

        final String mapName = randomString();
        final Runnable runnable = new MapPutRunnable(mapName);
        final CountDownLatch responseLatch = new CountDownLatch(1);

        service.submit(runnable, new ExecutionCallback() {
            public void onResponse(final Object response) {
                responseLatch.countDown();
            }

            public void onFailure(final Throwable t) {}
        });
        final IMap map = client.getMap(mapName);

        assertOpenEventually("responseLatch", responseLatch);
        assertEquals(1, map.size());
    }

    @Test
    public void testSubmitCallable_withExecutionCallback() throws Exception {
        final IExecutorService service = client.getExecutorService(randomString());

        final String msg = randomString();
        final Callable<String> callable = new AppendCallable(msg);
        final AtomicReference<String> result = new AtomicReference<String>();
        final CountDownLatch responseLatch = new CountDownLatch(1);

        service.submit(callable, new ExecutionCallback<String>() {
            public void onResponse(final String response) {
                result.set(response);
                responseLatch.countDown();
            }

            public void onFailure(final Throwable t) {}
        });

        assertOpenEventually("responseLatch", responseLatch);
        assertEquals(msg + AppendCallable.APPENDAGE, result.get());
    }

    @Test
    public void submitCallableToKeyOwner() throws Exception {
        final IExecutorService service = client.getExecutorService(randomString());

        final String msg = randomString();
        final Callable<String> callable = new AppendCallable(msg);

        final Future<String> result = service.submitToKeyOwner(callable, "key");

        assertEquals(msg + AppendCallable.APPENDAGE, result.get());
    }

    @Test
    public void submitRunnableToKeyOwner() throws Exception {
        final IExecutorService service = client.getExecutorService(randomString());

        final String mapName = randomString();
        final Runnable runnable = new MapPutRunnable(mapName);
        final CountDownLatch responseLatch = new CountDownLatch(1);

        service.submitToKeyOwner(runnable, "key", new ExecutionCallback() {
            public void onResponse(Object response) {
                responseLatch.countDown();
            }

            public void onFailure(Throwable t) {}
        });
        final IMap map = client.getMap(mapName);

        assertOpenEventually("responseLatch", responseLatch);
        assertEquals(1, map.size());
    }

    @Test
    public void submitCallableToKeyOwner_withExecutionCallback() throws Exception {
        final IExecutorService service = client.getExecutorService(randomString());

        final String msg = randomString();
        final Callable<String> callable = new AppendCallable(msg);
        final CountDownLatch responseLatch = new CountDownLatch(1);
        final AtomicReference<String> result = new AtomicReference<String>();

        service.submitToKeyOwner(callable, "key", new ExecutionCallback<String>() {
            public void onResponse(final String response) {
                result.set(response);
                responseLatch.countDown();
            }

            public void onFailure(Throwable t) {}
        });

        assertOpenEventually("responseLatch", responseLatch, 5);
        assertEquals(msg + AppendCallable.APPENDAGE, result.get());
    }

    @Test
    public void submitRunnablePartitionAware() throws Exception {
        final IExecutorService service = client.getExecutorService(randomString());

        final String mapName = randomString();
        final String key = HazelcastTestSupport.generateKeyOwnedBy(instance2);
        final Member member = instance2.getCluster().getLocalMember();

        //this task should execute on a node owning the given key argument,
        //the action is to put the UUid of the executing node into a map with the given name
        final Runnable runnable = new MapPutPartitionAwareRunnable<String>(mapName, key);

        service.submit(runnable);
        final IMap map = client.getMap(mapName);

        assertTrueEventually(new AssertTask() {
            public void run() throws Exception {
                assertTrue(map.containsKey(member.getUuid()));
            }
        });
    }

    @Test
    public void submitRunnablePartitionAware_withResult() throws Exception {
        final IExecutorService service = client.getExecutorService(randomString());

        final String expectedResult = "result";
        final String mapName = randomString();
        final String key = HazelcastTestSupport.generateKeyOwnedBy(instance2);
        final Member member = instance2.getCluster().getLocalMember();

        final Runnable runnable = new MapPutPartitionAwareRunnable<String>(mapName, key);

        final Future result = service.submit(runnable, expectedResult);
        final IMap map = client.getMap(mapName);

        assertEquals(expectedResult, result.get());
        assertTrueEventually(new AssertTask() {
            public void run() throws Exception {
                assertTrue(map.containsKey(member.getUuid()));
            }
        });
    }

    @Test
    public void submitRunnablePartitionAware_withExecutionCallback() throws Exception {
        final IExecutorService service = client.getExecutorService(randomString());

        final String mapName = randomString();
        final String key = HazelcastTestSupport.generateKeyOwnedBy(instance2);
        final Member member = instance2.getCluster().getLocalMember();
        final Runnable runnable = new MapPutPartitionAwareRunnable<String>(mapName, key);
        final CountDownLatch responseLatch = new CountDownLatch(1);

        service.submit(runnable, new ExecutionCallback() {
            @Override
            public void onResponse(Object response) {
                responseLatch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {}
        });
        final IMap map = client.getMap(mapName);

        assertOpenEventually("responseLatch", responseLatch);
        assertTrue(map.containsKey(member.getUuid()));
    }

    @Test
    public void submitCallablePartitionAware() throws Exception {
        final IExecutorService service = client.getExecutorService(randomString());

        final String mapName = randomString();
        final IMap map = client.getMap(mapName);
        final String key = HazelcastTestSupport.generateKeyOwnedBy(instance2);
        final Member member = instance2.getCluster().getLocalMember();

        final Callable<String> callable = new MapPutPartitionAwareCallable<String, String>(mapName, key);
        final Future<String> result = service.submit(callable);

        assertEquals(member.getUuid(), result.get());
        assertTrue(map.containsKey(member.getUuid()));
    }

    @Test
    public void submitCallablePartitionAware_WithExecutionCallback() throws Exception {
        final IExecutorService service = client.getExecutorService(randomString());

        final String mapName = randomString();
        final IMap map = client.getMap(mapName);
        final String key = HazelcastTestSupport.generateKeyOwnedBy(instance2);
        final Member member = instance2.getCluster().getLocalMember();

        final Callable<String> runnable = new MapPutPartitionAwareCallable<String, String>(mapName, key);

        final AtomicReference<String> result = new AtomicReference<String>();
        final CountDownLatch responseLatch = new CountDownLatch(1);

        service.submit(runnable, new ExecutionCallback<String>() {
            public void onResponse(final String response) {
                result.set(response);
                responseLatch.countDown();
            }

            public void onFailure(final Throwable t) {}
        });

        assertOpenEventually("responseLatch", responseLatch);
        assertEquals(member.getUuid(), result.get());
        assertTrue(map.containsKey(member.getUuid()));
    }
}