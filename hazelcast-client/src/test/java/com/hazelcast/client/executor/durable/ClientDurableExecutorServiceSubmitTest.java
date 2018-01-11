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

package com.hazelcast.client.executor.durable;

import com.hazelcast.client.executor.tasks.AppendCallable;
import com.hazelcast.client.executor.tasks.MapPutPartitionAwareCallable;
import com.hazelcast.client.executor.tasks.MapPutPartitionAwareRunnable;
import com.hazelcast.client.executor.tasks.MapPutRunnable;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.durableexecutor.DurableExecutorService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.test.HazelcastTestSupport.assertOpenEventually;
import static com.hazelcast.test.HazelcastTestSupport.assertSizeEventually;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientDurableExecutorServiceSubmitTest {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    private HazelcastInstance server;
    private HazelcastInstance client;

    @Before
    public void setup() {
        hazelcastFactory.newHazelcastInstance();
        server = hazelcastFactory.newHazelcastInstance();
        hazelcastFactory.newHazelcastInstance();
        client = hazelcastFactory.newHazelcastClient();
    }

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }


    @Test(expected = NullPointerException.class)
    @SuppressWarnings("ConstantConditions")
    public void testSubmitCallableNullTask() {
        DurableExecutorService service = client.getDurableExecutorService(randomString());
        Callable callable = null;

        service.submit(callable);
    }

    @Test
    public void submitRunnable() {
        DurableExecutorService service = client.getDurableExecutorService(randomString());

        String mapName = randomString();
        Runnable runnable = new MapPutRunnable(mapName);

        service.submit(runnable);

        IMap map = client.getMap(mapName);

        assertSizeEventually(1, map);
    }

    @Test
    public void testSubmitRunnable_WithResult() throws Exception {
        DurableExecutorService service = client.getDurableExecutorService(randomString());

        String mapName = randomString();
        Object givenResult = "givenResult";
        Future future = service.submit(new MapPutRunnable(mapName), givenResult);
        Object result = future.get();

        IMap map = client.getMap(mapName);

        assertEquals(givenResult, result);
        assertEquals(1, map.size());
    }

    @Test
    public void testSubmitCallable() throws Exception {
        DurableExecutorService service = client.getDurableExecutorService(randomString());

        String msg = randomString();
        Callable callable = new AppendCallable(msg);
        Future result = service.submit(callable);

        assertEquals(msg + AppendCallable.APPENDAGE, result.get());
    }

    @Test
    public void testSubmitRunnable_withExecutionCallback() {
        DurableExecutorService service = client.getDurableExecutorService(randomString());

        String mapName = randomString();
        Runnable runnable = new MapPutRunnable(mapName);
        final CountDownLatch responseLatch = new CountDownLatch(1);

        service.submit(runnable).andThen(new ExecutionCallback() {
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
        DurableExecutorService service = client.getDurableExecutorService(randomString());

        String msg = randomString();
        Callable<String> callable = new AppendCallable(msg);
        final AtomicReference<String> result = new AtomicReference<String>();
        final CountDownLatch responseLatch = new CountDownLatch(1);

        service.submit(callable).andThen(new ExecutionCallback<String>() {
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
    public void submitCallableToKeyOwner() throws Exception {
        DurableExecutorService service = client.getDurableExecutorService(randomString());

        String msg = randomString();
        Callable<String> callable = new AppendCallable(msg);

        Future<String> result = service.submitToKeyOwner(callable, "key");

        assertEquals(msg + AppendCallable.APPENDAGE, result.get());
    }

    @Test
    public void submitRunnableToKeyOwner() {
        DurableExecutorService service = client.getDurableExecutorService(randomString());

        String mapName = randomString();
        Runnable runnable = new MapPutRunnable(mapName);
        final CountDownLatch responseLatch = new CountDownLatch(1);

        service.submitToKeyOwner(runnable, "key").andThen(new ExecutionCallback() {
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
        DurableExecutorService service = client.getDurableExecutorService(randomString());

        String msg = randomString();
        Callable<String> callable = new AppendCallable(msg);
        final CountDownLatch responseLatch = new CountDownLatch(1);
        final AtomicReference<String> result = new AtomicReference<String>();

        service.submitToKeyOwner(callable, "key").andThen(new ExecutionCallback<String>() {
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
        DurableExecutorService service = client.getDurableExecutorService(randomString());

        String mapName = randomString();
        String key = HazelcastTestSupport.generateKeyOwnedBy(server);
        final Member member = server.getCluster().getLocalMember();

        // this task should execute on a node owning the given key argument,
        // the action is to put the UUid of the executing node into a map with the given name
        Runnable runnable = new MapPutPartitionAwareRunnable<String>(mapName, key);

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
        DurableExecutorService service = client.getDurableExecutorService(randomString());

        String expectedResult = "result";
        String mapName = randomString();
        String key = HazelcastTestSupport.generateKeyOwnedBy(server);
        final Member member = server.getCluster().getLocalMember();

        Runnable runnable = new MapPutPartitionAwareRunnable<String>(mapName, key);

        Future result = service.submit(runnable, expectedResult);
        final IMap map = client.getMap(mapName);

        assertEquals(expectedResult, result.get());
        assertTrueEventually(new AssertTask() {
            public void run() throws Exception {
                assertTrue(map.containsKey(member.getUuid()));
            }
        });
    }

    @Test
    public void submitRunnablePartitionAware_withExecutionCallback() {
        DurableExecutorService service = client.getDurableExecutorService(randomString());

        String mapName = randomString();
        String key = HazelcastTestSupport.generateKeyOwnedBy(server);
        Member member = server.getCluster().getLocalMember();
        Runnable runnable = new MapPutPartitionAwareRunnable<String>(mapName, key);
        final CountDownLatch responseLatch = new CountDownLatch(1);

        service.submit(runnable).andThen(new ExecutionCallback() {
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
    public void submitCallablePartitionAware() throws Exception {
        DurableExecutorService service = client.getDurableExecutorService(randomString());

        String mapName = randomString();
        IMap map = client.getMap(mapName);
        String key = HazelcastTestSupport.generateKeyOwnedBy(server);
        Member member = server.getCluster().getLocalMember();

        Callable<String> callable = new MapPutPartitionAwareCallable<String, String>(mapName, key);
        Future<String> result = service.submit(callable);

        assertEquals(member.getUuid(), result.get());
        assertTrue(map.containsKey(member.getUuid()));
    }

    @Test
    public void submitCallablePartitionAware_WithExecutionCallback() {
        DurableExecutorService service = client.getDurableExecutorService(randomString());

        String mapName = randomString();
        IMap map = client.getMap(mapName);
        String key = HazelcastTestSupport.generateKeyOwnedBy(server);
        Member member = server.getCluster().getLocalMember();

        Callable<String> runnable = new MapPutPartitionAwareCallable<String, String>(mapName, key);

        final AtomicReference<String> result = new AtomicReference<String>();
        final CountDownLatch responseLatch = new CountDownLatch(1);

        service.submit(runnable).andThen(new ExecutionCallback<String>() {
            public void onResponse(String response) {
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
}
