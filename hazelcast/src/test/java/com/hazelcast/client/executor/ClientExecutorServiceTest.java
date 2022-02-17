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
import com.hazelcast.client.executor.tasks.MapPutRunnable;
import com.hazelcast.client.impl.clientside.ClientTestUtil;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.client.test.executor.tasks.CancellationAwareTask;
import com.hazelcast.client.test.executor.tasks.FailingCallable;
import com.hazelcast.client.test.executor.tasks.SelectNoMembers;
import com.hazelcast.client.test.executor.tasks.SerializedCounterCallable;
import com.hazelcast.client.test.executor.tasks.TaskWithUnserializableResponse;
import com.hazelcast.cluster.MemberSelector;
import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.test.HazelcastTestSupport.assertOpenEventually;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.ignore;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientExecutorServiceTest {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();
    private HazelcastInstance client;
    private HazelcastInstance instance;

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

        instance = hazelcastFactory.newHazelcastInstance(config);
        hazelcastFactory.newHazelcastInstance(config);
        hazelcastFactory.newHazelcastInstance(config);
        hazelcastFactory.newHazelcastInstance(config);
        client = hazelcastFactory.newHazelcastClient(clientConfig);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetLocalExecutorStats() {
        IExecutorService service = client.getExecutorService(randomString());

        service.getLocalExecutorStats();
    }

    @Test
    public void testIsTerminated() {
        IExecutorService service = client.getExecutorService(randomString());

        assertFalse(service.isTerminated());
    }

    @Test
    public void testIsShutdown() {
        IExecutorService service = client.getExecutorService(randomString());

        assertFalse(service.isShutdown());
    }

    @Test
    public void testShutdownNow() {
        final IExecutorService service = client.getExecutorService(randomString());

        service.shutdownNow();

        assertTrueEventually(new AssertTask() {
            public void run() {
                assertTrue(service.isShutdown());
            }
        });
    }

    @Test
    public void testShutdownMultipleTimes() {
        final IExecutorService service = client.getExecutorService(randomString());
        service.shutdownNow();
        service.shutdown();

        assertTrueEventually(new AssertTask() {
            public void run() {
                assertTrue(service.isShutdown());
            }
        });
    }

    @Test(expected = TimeoutException.class)
    public void testCancellationAwareTask_whenTimeOut()
            throws InterruptedException, ExecutionException, TimeoutException {
        IExecutorService service = client.getExecutorService(randomString());
        CancellationAwareTask task = new CancellationAwareTask(Long.MAX_VALUE);

        Future future = service.submit(task);

        future.get(1, TimeUnit.SECONDS);
    }

    @Test
    public void testFutureAfterCancellationAwareTaskTimeOut()
            throws InterruptedException, ExecutionException {
        IExecutorService service = client.getExecutorService(randomString());
        CancellationAwareTask task = new CancellationAwareTask(Long.MAX_VALUE);

        Future future = service.submit(task);

        try {
            future.get(1, TimeUnit.SECONDS);
        } catch (TimeoutException ignored) {
        }

        assertFalse(future.isDone());
        assertFalse(future.isCancelled());
    }

    @Test
    @Ignore("https://github.com/hazelcast/hazelcast/issues/4677")
    //Ignored because fixing it requires extensive refactoring see ClientExecutorServiceCancelTest
    public void testCancelFutureAfterCancellationAwareTaskTimeOut()
            throws InterruptedException, ExecutionException {
        IExecutorService service = client.getExecutorService(randomString());
        CancellationAwareTask task = new CancellationAwareTask(Long.MAX_VALUE);

        Future future = service.submit(task);

        try {
            future.get(1, TimeUnit.SECONDS);
        } catch (TimeoutException ignored) {
        }

        assertTrue(future.cancel(true));
        assertTrue(future.isCancelled());
        assertTrue(future.isDone());
    }

    @Test(expected = CancellationException.class)
    public void testGetFutureAfterCancel()
            throws InterruptedException, ExecutionException {
        IExecutorService service = client.getExecutorService(randomString());
        CancellationAwareTask task = new CancellationAwareTask(Long.MAX_VALUE);

        Future future = service.submit(task);
        try {
            future.get(1, TimeUnit.SECONDS);
        } catch (TimeoutException ignored) {
        }
        future.cancel(true);

        future.get();
    }

    @Test(expected = ExecutionException.class)
    public void testSubmitFailingCallableException()
            throws ExecutionException, InterruptedException {
        IExecutorService service = client.getExecutorService(randomString());
        Future<String> failingFuture = service.submit(new FailingCallable());

        failingFuture.get();
    }

    @Test
    public void testSubmitFailingCallableException_withExecutionCallback()
            throws InterruptedException {
        IExecutorService service = client.getExecutorService(randomString());
        final CountDownLatch latch = new CountDownLatch(1);
        service.submit(new FailingCallable(), new ExecutionCallback<String>() {
            @Override
            public void onResponse(String response) {
            }

            @Override
            public void onFailure(Throwable t) {
                latch.countDown();
            }
        });
        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    @Test(expected = IllegalStateException.class)
    public void testSubmitFailingCallableReasonExceptionCause()
            throws Throwable {
        IExecutorService service = client.getExecutorService(randomString());
        Future<String> failingFuture = service.submit(new FailingCallable());

        try {
            failingFuture.get();
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }

    @Test(expected = RejectedExecutionException.class)
    public void testExecute_withNoMemberSelected() {
        IExecutorService service = client.getExecutorService(randomString());
        String mapName = randomString();
        MemberSelector selector = new SelectNoMembers();

        service.execute(new MapPutRunnable(mapName), selector);
    }

    @Test
    public void testCallableSerializedOnce()
            throws ExecutionException, InterruptedException {
        String name = randomString();
        IExecutorService service = client.getExecutorService(name);
        SerializedCounterCallable counterCallable = new SerializedCounterCallable();
        Future future = service.submitToKeyOwner(counterCallable, name);
        assertEquals(2, future.get());
    }

    @Test
    public void testCallableSerializedOnce_submitToAddress()
            throws ExecutionException, InterruptedException {
        String name = randomString();
        IExecutorService service = client.getExecutorService(name);
        SerializedCounterCallable counterCallable = new SerializedCounterCallable();
        Future future = service.submitToMember(counterCallable, instance.getCluster().getLocalMember());
        assertEquals(2, future.get());
    }

    @Test(expected = HazelcastSerializationException.class)
    public void testUnserializableResponse_exceptionPropagatesToClient()
            throws Throwable {
        IExecutorService service = client.getExecutorService("executor");
        TaskWithUnserializableResponse counterCallable = new TaskWithUnserializableResponse();
        Future future = service.submit(counterCallable);
        try {
            future.get();
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }

    @Test(expected = HazelcastSerializationException.class)
    public void testUnserializableResponse_exceptionPropagatesToClientCallback()
            throws Throwable {
        IExecutorService service = client.getExecutorService("executor");
        TaskWithUnserializableResponse counterCallable = new TaskWithUnserializableResponse();
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final AtomicReference<Throwable> throwable = new AtomicReference<Throwable>();
        service.submit(counterCallable, new ExecutionCallback() {
            @Override
            public void onResponse(Object response) {

            }

            @Override
            public void onFailure(Throwable t) {
                throwable.set(t.getCause());
                countDownLatch.countDown();
            }
        });
        assertOpenEventually(countDownLatch);
        throw throwable.get();
    }

    @Ignore("Behaviour needs a better test, due to switching from user executor to ForkJoinPool.commonPool")
    @Test
    public void testExecutionCallback_whenExecutionRejected() {
        final AtomicReference<Throwable> exceptionThrown = new AtomicReference<>();
        final CountDownLatch waitForShutdown = new CountDownLatch(1);
        final CountDownLatch didShutdown = new CountDownLatch(1);
        final HazelcastClientInstanceImpl hzClient = ClientTestUtil.getHazelcastClientInstanceImpl(client);

        ExecutionRejectedRunnable.waitForShutdown = waitForShutdown;
        ExecutionRejectedRunnable.didShutdown = didShutdown;

        IExecutorService executorService = hzClient.getExecutorService("executor");
        Thread t = new Thread(() -> {
            try {
                waitForShutdown.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                ignore(e);
            }
            ForkJoinPool.commonPool().shutdown();
            didShutdown.countDown();
        });
        t.start();
        executorService.submit(new ExecutionRejectedRunnable(), new ExecutionCallback<Object>() {
            @Override
            public void onResponse(Object response) {
            }

            @Override
            public void onFailure(Throwable t) {
                exceptionThrown.set(t);
            }
        });

        // assert a RejectedExecutionException is thrown from already shutdown user executor
        // it is not wrapped in a HazelcastClientNotActiveException because the client is still running
        assertTrueEventually(() -> assertTrue(exceptionThrown.get() instanceof RejectedExecutionException));
    }

    private static class ExecutionRejectedRunnable implements Runnable, Serializable {
        static CountDownLatch waitForShutdown;
        static CountDownLatch didShutdown;

        @Override
        public void run() {
            waitForShutdown.countDown();
            try {
                didShutdown.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                ignore(e);
            }
        }
    }
}
