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

package com.hazelcast.client.executor;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.client.executor.tasks.MapPutPartitionAwareRunnable;
import com.hazelcast.client.executor.tasks.MapPutRunnable;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.client.test.executor.tasks.AppendCallable;
import com.hazelcast.client.test.executor.tasks.GetMemberUuidTask;
import com.hazelcast.client.test.executor.tasks.MapPutPartitionAwareCallable;
import com.hazelcast.client.test.executor.tasks.NullCallable;
import com.hazelcast.client.test.executor.tasks.SelectAllMembers;
import com.hazelcast.cluster.memberselector.MemberSelectors;
import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.map.IMap;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MemberSelector;
import com.hazelcast.core.MultiExecutionCallback;
import com.hazelcast.executor.ExecutorServiceTestSupport;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.test.HazelcastTestSupport.assertOpenEventually;
import static com.hazelcast.test.HazelcastTestSupport.assertSizeEventually;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.randomName;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static com.hazelcast.internal.util.FutureUtil.waitForever;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientExecutorServiceSubmitTest {

    private static final int CLUSTER_SIZE = 3;
    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();
    private HazelcastInstance server;
    private HazelcastInstance client;

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Before
    public void setup()
            throws IOException {
        Config config = new XmlConfigBuilder(getClass().getClassLoader().getResourceAsStream("hazelcast-test-executor.xml"))
                .build();
        ClientConfig clientConfig = new XmlClientConfigBuilder("classpath:hazelcast-client-test-executor.xml").build();

        hazelcastFactory.newHazelcastInstance(config);
        server = hazelcastFactory.newHazelcastInstance(config);
        hazelcastFactory.newHazelcastInstance(config);
        client = hazelcastFactory.newHazelcastClient(clientConfig);
    }

    @Test(expected = NullPointerException.class)
    public void testSubmitCallableNullTask() {
        IExecutorService service = client.getExecutorService(randomString());
        Callable<String> callable = null;

        service.submit(callable);
    }

    @Test
    public void testSubmitCallableToMember()
            throws Exception {
        IExecutorService service = client.getExecutorService(randomString());

        Callable<UUID> getUuidCallable = new GetMemberUuidTask();
        Member member = server.getCluster().getLocalMember();

        Future<UUID> result = service.submitToMember(getUuidCallable, member);

        assertEquals(member.getUuid(), result.get());
    }

    @Test
    public void testSubmitCallableToMembers()
            throws Exception {
        IExecutorService service = client.getExecutorService(randomString());

        Callable<UUID> getUuidCallable = new GetMemberUuidTask();
        Collection<Member> collection = server.getCluster().getMembers();

        Map<Member, Future<UUID>> map = service.submitToMembers(getUuidCallable, collection);
        for (Member member : map.keySet()) {
            Future<UUID> result = map.get(member);
            UUID uuid = result.get();

            assertEquals(member.getUuid(), uuid);
        }
    }

    @Test
    public void testSubmitCallable_withMemberSelector()
            throws Exception {
        IExecutorService service = client.getExecutorService(randomString());

        String msg = randomString();
        Callable<String> callable = new AppendCallable(msg);
        MemberSelector selectAll = new SelectAllMembers();

        Future<String> f = service.submit(callable, selectAll);

        assertEquals(msg + AppendCallable.APPENDAGE, f.get());
    }

    @Test
    public void testSubmitCallableToMembers_withMemberSelector()
            throws Exception {
        IExecutorService service = client.getExecutorService(randomString());

        Callable<UUID> getUuidCallable = new GetMemberUuidTask();
        MemberSelector selectAll = new SelectAllMembers();

        Map<Member, Future<UUID>> map = service.submitToMembers(getUuidCallable, selectAll);
        for (Member member : map.keySet()) {
            Future<UUID> result = map.get(member);
            UUID uuid = result.get();

            assertEquals(member.getUuid(), uuid);
        }
    }

    @Test
    public void submitCallableToAllMembers()
            throws Exception {
        IExecutorService service = client.getExecutorService(randomString());

        String msg = randomString();
        Callable<String> callable = new AppendCallable(msg);

        Map<Member, Future<String>> map = service.submitToAllMembers(callable);
        for (Member member : map.keySet()) {
            Future<String> result = map.get(member);
            assertEquals(msg + AppendCallable.APPENDAGE, result.get());
        }
    }

    @Test
    public void submitRunnableToMember_withExecutionCallback() {
        IExecutorService service = client.getExecutorService(randomString());

        String mapName = randomString();
        Runnable runnable = new MapPutRunnable(mapName);
        Member member = server.getCluster().getLocalMember();
        final CountDownLatch responseLatch = new CountDownLatch(1);

        service.submitToMember(runnable, member, new ExecutionCallback() {
            public void onResponse(Object response) {
                responseLatch.countDown();
            }

            public void onFailure(Throwable t) {
            }
        });
        Map map = client.getMap(mapName);

        assertOpenEventually("responseLatch", responseLatch);
        assertEquals(1, map.size());
    }

    @Test
    public void submitRunnableToMembers_withMultiExecutionCallback() {
        IExecutorService service = client.getExecutorService(randomString());

        String mapName = randomString();
        Runnable runnable = new MapPutRunnable(mapName);
        Collection<Member> collection = server.getCluster().getMembers();
        final CountDownLatch responseLatch = new CountDownLatch(CLUSTER_SIZE);
        final CountDownLatch completeLatch = new CountDownLatch(1);

        service.submitToMembers(runnable, collection, new MultiExecutionCallback() {
            public void onResponse(Member member, Object value) {
                responseLatch.countDown();
            }

            public void onComplete(Map<Member, Object> values) {
                completeLatch.countDown();
            }
        });
        Map map = client.getMap(mapName);

        assertOpenEventually("responseLatch", responseLatch);
        assertOpenEventually("completeLatch", completeLatch);
        assertEquals(CLUSTER_SIZE, map.size());
    }

    @Test
    public void testSubmitCallableToMember_withExecutionCallback() {
        IExecutorService service = client.getExecutorService(randomString());

        Callable getUuidCallable = new GetMemberUuidTask();
        Member member = server.getCluster().getLocalMember();
        final CountDownLatch responseLatch = new CountDownLatch(1);
        final AtomicReference<Object> result = new AtomicReference<Object>();

        service.submitToMember(getUuidCallable, member, new ExecutionCallback() {
            @Override
            public void onResponse(Object response) {
                result.set(response);
                responseLatch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
            }
        });

        assertOpenEventually("responseLatch", responseLatch);
        assertEquals(member.getUuid(), result.get());
    }

    @Test
    public void submitCallableToMember_withMultiExecutionCallback() {
        IExecutorService service = client.getExecutorService(randomString());

        final CountDownLatch responseLatch = new CountDownLatch(CLUSTER_SIZE);
        final CountDownLatch completeLatch = new CountDownLatch(CLUSTER_SIZE);
        final String msg = randomString();
        Callable<String> callable = new AppendCallable(msg);
        Collection<Member> collection = server.getCluster().getMembers();

        service.submitToMembers(callable, collection, new MultiExecutionCallback() {
            public void onResponse(Member member, Object value) {
                if (value.equals(msg + AppendCallable.APPENDAGE)) {
                    responseLatch.countDown();
                }
            }

            public void onComplete(Map<Member, Object> values) {
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
    public void submitRunnable_withExecutionCallback() {
        IExecutorService service = client.getExecutorService(randomString());

        final CountDownLatch responseLatch = new CountDownLatch(1);
        String mapName = randomString();
        Runnable runnable = new MapPutRunnable(mapName);
        MemberSelector selector = new SelectAllMembers();

        service.submit(runnable, selector, new ExecutionCallback() {
            public void onResponse(Object response) {
                responseLatch.countDown();
            }

            public void onFailure(Throwable t) {
            }
        });
        IMap map = client.getMap(mapName);

        assertOpenEventually("responseLatch", responseLatch);
        assertEquals(1, map.size());
    }

    @Test
    public void submitRunnableToMembers_withExecutionCallback() {
        IExecutorService service = client.getExecutorService(randomString());

        final CountDownLatch responseLatch = new CountDownLatch(CLUSTER_SIZE);
        final CountDownLatch completeLatch = new CountDownLatch(1);
        String mapName = randomString();
        Runnable runnable = new MapPutRunnable(mapName);
        MemberSelector selector = new SelectAllMembers();

        service.submitToMembers(runnable, selector, new MultiExecutionCallback() {
            public void onResponse(Member member, Object value) {
                responseLatch.countDown();
            }

            public void onComplete(Map<Member, Object> values) {
                completeLatch.countDown();
            }
        });
        IMap map = client.getMap(mapName);

        assertOpenEventually("responseLatch", responseLatch);
        assertOpenEventually("completeLatch", completeLatch);
        assertEquals(CLUSTER_SIZE, map.size());
    }

    @Test
    public void submitCallable_withExecutionCallback() {
        IExecutorService service = client.getExecutorService(randomString());

        final CountDownLatch responseLatch = new CountDownLatch(1);
        String msg = randomString();
        Callable runnable = new AppendCallable(msg);
        MemberSelector selector = new SelectAllMembers();
        final AtomicReference<Object> result = new AtomicReference<Object>();

        service.submit(runnable, selector, new ExecutionCallback() {
            public void onResponse(Object response) {
                result.set(response);
                responseLatch.countDown();
            }

            public void onFailure(Throwable t) {
            }
        });

        assertOpenEventually("responseLatch", responseLatch);
        assertEquals(msg + AppendCallable.APPENDAGE, result.get());
    }

    @Test
    public void submitCallableToMembers_withExecutionCallback() {
        IExecutorService service = client.getExecutorService(randomString());

        final CountDownLatch responseLatch = new CountDownLatch(CLUSTER_SIZE);
        final CountDownLatch completeLatch = new CountDownLatch(1);
        final String msg = randomString();
        Callable callable = new AppendCallable(msg);
        MemberSelector selector = new SelectAllMembers();

        service.submitToMembers(callable, selector, new MultiExecutionCallback() {
            public void onResponse(Member member, Object value) {
                if (value.equals(msg + AppendCallable.APPENDAGE)) {
                    responseLatch.countDown();
                }
            }

            public void onComplete(Map<Member, Object> values) {
                completeLatch.countDown();
            }
        });

        assertOpenEventually("responseLatch", responseLatch);
        assertOpenEventually("completeLatch", completeLatch);
    }

    @Test
    public void submitRunnableToAllMembers_withMultiExecutionCallback() {
        IExecutorService service = client.getExecutorService(randomString());

        final CountDownLatch responseLatch = new CountDownLatch(CLUSTER_SIZE);
        final CountDownLatch completeLatch = new CountDownLatch(1);
        String mapName = randomString();
        Runnable runnable = new MapPutRunnable(mapName);

        service.submitToAllMembers(runnable, new MultiExecutionCallback() {
            public void onResponse(Member member, Object value) {
                responseLatch.countDown();
            }

            public void onComplete(Map<Member, Object> values) {
                completeLatch.countDown();
            }
        });
        IMap map = client.getMap(mapName);

        assertOpenEventually("responseLatch", responseLatch);
        assertOpenEventually("completeLatch", completeLatch);
        assertEquals(CLUSTER_SIZE, map.size());
    }

    @Test
    public void submitCallableToAllMembers_withMultiExecutionCallback() {
        IExecutorService service = client.getExecutorService(randomString());

        final CountDownLatch responseLatch = new CountDownLatch(CLUSTER_SIZE);
        final CountDownLatch completeLatch = new CountDownLatch(CLUSTER_SIZE);
        final String msg = randomString();
        Callable callable = new AppendCallable(msg);

        service.submitToAllMembers(callable, new MultiExecutionCallback() {
            public void onResponse(Member member, Object value) {
                if (value.equals(msg + AppendCallable.APPENDAGE)) {
                    responseLatch.countDown();
                }
            }

            public void onComplete(Map<Member, Object> values) {
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
    public void submitCallableWithNullResultToAllMembers_withMultiExecutionCallback() {
        IExecutorService service = client.getExecutorService(randomString());

        final CountDownLatch responseLatch = new CountDownLatch(CLUSTER_SIZE);
        final CountDownLatch completeLatch = new CountDownLatch(CLUSTER_SIZE);
        Callable callable = new NullCallable();

        service.submitToAllMembers(callable, new MultiExecutionCallback() {
            public void onResponse(Member member, Object value) {
                if (value == null) {
                    responseLatch.countDown();
                }
            }

            public void onComplete(Map<Member, Object> values) {
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
        IExecutorService service = client.getExecutorService(randomString());

        String mapName = randomString();
        Runnable runnable = new MapPutRunnable(mapName);

        service.submit(runnable);

        IMap map = client.getMap(mapName);

        assertSizeEventually(1, map);
    }

    @Test
    public void testSubmitRunnable_WithResult()
            throws ExecutionException, InterruptedException {
        IExecutorService service = client.getExecutorService(randomString());

        String mapName = randomString();
        Object givenResult = "givenResult";
        Future future = service.submit(new MapPutRunnable(mapName), givenResult);
        Object result = future.get();

        IMap map = client.getMap(mapName);

        assertEquals(givenResult, result);
        assertEquals(1, map.size());
    }

    @Test
    public void testSubmitCallable()
            throws Exception {
        IExecutorService service = client.getExecutorService(randomString());

        String msg = randomString();
        Callable callable = new AppendCallable(msg);
        Future result = service.submit(callable);

        assertEquals(msg + AppendCallable.APPENDAGE, result.get());
    }

    @Test
    public void testSubmitRunnable_withExecutionCallback() {
        IExecutorService service = client.getExecutorService(randomString());

        String mapName = randomString();
        Runnable runnable = new MapPutRunnable(mapName);
        final CountDownLatch responseLatch = new CountDownLatch(1);

        service.submit(runnable, new ExecutionCallback() {
            public void onResponse(Object response) {
                responseLatch.countDown();
            }

            public void onFailure(Throwable t) {
            }
        });
        IMap map = client.getMap(mapName);

        assertOpenEventually("responseLatch", responseLatch);
        assertEquals(1, map.size());
    }

    @Test
    public void testSubmitCallable_withExecutionCallback() {
        IExecutorService service = client.getExecutorService(randomString());

        String msg = randomString();
        Callable<String> callable = new AppendCallable(msg);
        final AtomicReference<String> result = new AtomicReference<String>();
        final CountDownLatch responseLatch = new CountDownLatch(1);

        service.submit(callable, new ExecutionCallback<String>() {
            public void onResponse(String response) {
                result.set(response);
                responseLatch.countDown();
            }

            public void onFailure(Throwable t) {
            }
        });

        assertOpenEventually("responseLatch", responseLatch);
        assertEquals(msg + AppendCallable.APPENDAGE, result.get());
    }

    @Test
    public void submitCallableToKeyOwner()
            throws Exception {
        IExecutorService service = client.getExecutorService(randomString());

        String msg = randomString();
        Callable<String> callable = new AppendCallable(msg);

        Future<String> result = service.submitToKeyOwner(callable, "key");

        assertEquals(msg + AppendCallable.APPENDAGE, result.get());
    }

    @Test
    public void submitRunnableToKeyOwner() {
        IExecutorService service = client.getExecutorService(randomString());

        String mapName = randomString();
        Runnable runnable = new MapPutRunnable(mapName);
        final CountDownLatch responseLatch = new CountDownLatch(1);

        service.submitToKeyOwner(runnable, "key", new ExecutionCallback() {
            public void onResponse(Object response) {
                responseLatch.countDown();
            }

            public void onFailure(Throwable t) {
            }
        });
        IMap map = client.getMap(mapName);

        assertOpenEventually("responseLatch", responseLatch);
        assertEquals(1, map.size());
    }

    @Test
    public void submitCallableToKeyOwner_withExecutionCallback() {
        IExecutorService service = client.getExecutorService(randomString());

        String msg = randomString();
        Callable<String> callable = new AppendCallable(msg);
        final CountDownLatch responseLatch = new CountDownLatch(1);
        final AtomicReference<String> result = new AtomicReference<String>();

        service.submitToKeyOwner(callable, "key", new ExecutionCallback<String>() {
            public void onResponse(String response) {
                result.set(response);
                responseLatch.countDown();
            }

            public void onFailure(Throwable t) {
            }
        });

        assertOpenEventually("responseLatch", responseLatch);
        assertEquals(msg + AppendCallable.APPENDAGE, result.get());
    }

    @Test
    public void submitRunnablePartitionAware() {
        IExecutorService service = client.getExecutorService(randomString());

        String mapName = randomString();
        String key = HazelcastTestSupport.generateKeyOwnedBy(server);
        final Member member = server.getCluster().getLocalMember();

        //this task should execute on a node owning the given key argument,
        //the action is to put the UUid of the executing node into a map with the given name
        Runnable runnable = new MapPutPartitionAwareRunnable<String>(mapName, key);

        service.submit(runnable);
        final IMap map = client.getMap(mapName);

        assertTrueEventually(new AssertTask() {
            public void run() {
                assertTrue(map.containsKey(member.getUuid()));
            }
        });
    }

    @Test
    public void submitRunnablePartitionAware_withResult()
            throws Exception {
        IExecutorService service = client.getExecutorService(randomString());

        String expectedResult = "result";
        String mapName = randomString();
        String key = HazelcastTestSupport.generateKeyOwnedBy(server);
        final Member member = server.getCluster().getLocalMember();

        Runnable runnable = new MapPutPartitionAwareRunnable<String>(mapName, key);

        Future result = service.submit(runnable, expectedResult);
        final IMap map = client.getMap(mapName);

        assertEquals(expectedResult, result.get());
        assertTrueEventually(new AssertTask() {
            public void run() {
                assertTrue(map.containsKey(member.getUuid()));
            }
        });
    }

    @Test
    public void submitRunnablePartitionAware_withExecutionCallback() {
        IExecutorService service = client.getExecutorService(randomString());

        String mapName = randomString();
        String key = HazelcastTestSupport.generateKeyOwnedBy(server);
        Member member = server.getCluster().getLocalMember();
        Runnable runnable = new MapPutPartitionAwareRunnable<String>(mapName, key);
        final CountDownLatch responseLatch = new CountDownLatch(1);

        service.submit(runnable, new ExecutionCallback() {
            @Override
            public void onResponse(Object response) {
                responseLatch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
            }
        });
        IMap map = client.getMap(mapName);

        assertOpenEventually("responseLatch", responseLatch);
        assertTrue(map.containsKey(member.getUuid()));
    }

    @Test
    public void submitCallablePartitionAware()
            throws Exception {
        IExecutorService service = client.getExecutorService(randomString());

        String mapName = randomString();
        IMap map = client.getMap(mapName);
        String key = HazelcastTestSupport.generateKeyOwnedBy(server);
        Member member = server.getCluster().getLocalMember();

        Callable<UUID> callable = new MapPutPartitionAwareCallable<>(mapName, key);
        Future<UUID> result = service.submit(callable);

        assertEquals(member.getUuid(), result.get());
        assertTrue(map.containsKey(member.getUuid()));
    }

    @Test
    public void submitCallablePartitionAware_WithExecutionCallback() {
        IExecutorService service = client.getExecutorService(randomString());

        String mapName = randomString();
        IMap map = client.getMap(mapName);
        String key = HazelcastTestSupport.generateKeyOwnedBy(server);
        Member member = server.getCluster().getLocalMember();

        Callable<UUID> runnable = new MapPutPartitionAwareCallable<>(mapName, key);

        final AtomicReference<UUID> result = new AtomicReference<>();
        final CountDownLatch responseLatch = new CountDownLatch(1);

        service.submit(runnable, new ExecutionCallback<UUID>() {
            public void onResponse(UUID response) {
                result.set(response);
                responseLatch.countDown();
            }

            public void onFailure(Throwable t) {
            }
        });

        assertOpenEventually("responseLatch", responseLatch);
        assertEquals(member.getUuid(), result.get());
        assertTrue(map.containsKey(member.getUuid()));
    }

    @Test
    public void testSubmitToAllMembersSerializesTheTaskOnlyOnce() {
        IExecutorService executorService = client.getExecutorService(randomName());
        ExecutorServiceTestSupport.SerializationCountingCallable countingCallable = new ExecutorServiceTestSupport.SerializationCountingCallable();
        Map<Member, Future<Void>> futures = executorService.submitToAllMembers(countingCallable);
        waitForever(futures.values());
        assertEquals(1, countingCallable.getSerializationCount());
    }

    @Test
    public void testSubmitToAllMembersSerializesTheTaskOnlyOnce_withCallback() throws InterruptedException {
        IExecutorService executorService = client.getExecutorService(randomName());
        ExecutorServiceTestSupport.SerializationCountingCallable countingCallable = new ExecutorServiceTestSupport.SerializationCountingCallable();
        CountDownLatch complete = new CountDownLatch(1);
        executorService.submitToAllMembers(countingCallable, new MultiExecutionCallback() {
            @Override
            public void onResponse(Member member, Object value) {

            }

            @Override
            public void onComplete(Map<Member, Object> values) {
                complete.countDown();
            }
        });
        complete.await();
        assertEquals(1, countingCallable.getSerializationCount());
    }

    @Test
    public void testSubmitToMembersSerializesTheTaskOnlyOnce_withSelector() {
        IExecutorService executorService = client.getExecutorService(randomName());
        ExecutorServiceTestSupport.SerializationCountingCallable countingCallable = new ExecutorServiceTestSupport.SerializationCountingCallable();
        Map<Member, Future<Void>> futures = executorService.submitToMembers(countingCallable, MemberSelectors.NON_LOCAL_MEMBER_SELECTOR);
        waitForever(futures.values());
        assertEquals(1, countingCallable.getSerializationCount());
    }

    @Test
    public void testSubmitToMembersSerializesTheTaskOnlyOnce_withCollection() {
        IExecutorService executorService = client.getExecutorService(randomName());
        ExecutorServiceTestSupport.SerializationCountingCallable countingCallable = new ExecutorServiceTestSupport.SerializationCountingCallable();
        Map<Member, Future<Void>> futures = executorService.submitToMembers(countingCallable, client.getCluster().getMembers());
        waitForever(futures.values());
        assertEquals(1, countingCallable.getSerializationCount());
    }

    @Test
    public void testSubmitToMembersSerializesTheTaskOnlyOnce_withSelectorAndCallback() throws InterruptedException {
        IExecutorService executorService = client.getExecutorService(randomName());
        ExecutorServiceTestSupport.SerializationCountingCallable countingCallable = new ExecutorServiceTestSupport.SerializationCountingCallable();
        CountDownLatch complete = new CountDownLatch(1);
        executorService.submitToMembers(countingCallable, MemberSelectors.DATA_MEMBER_SELECTOR, new MultiExecutionCallback() {
            @Override
            public void onResponse(Member member, Object value) {

            }

            @Override
            public void onComplete(Map<Member, Object> values) {
                complete.countDown();
            }
        });
        complete.await();
        assertEquals(1, countingCallable.getSerializationCount());
    }

    @Test
    public void testSubmitToMembersSerializesTheTaskOnlyOnce_withCollectionAndCallback() throws InterruptedException {
        IExecutorService executorService = client.getExecutorService(randomName());
        ExecutorServiceTestSupport.SerializationCountingCallable countingCallable = new ExecutorServiceTestSupport.SerializationCountingCallable();
        CountDownLatch complete = new CountDownLatch(1);
        executorService.submitToMembers(countingCallable, client.getCluster().getMembers(), new MultiExecutionCallback() {
            @Override
            public void onResponse(Member member, Object value) {

            }

            @Override
            public void onComplete(Map<Member, Object> values) {
                complete.countDown();
            }
        });
        complete.await();
        assertEquals(1, countingCallable.getSerializationCount());
    }
}
