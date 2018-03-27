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

package com.hazelcast.client.executor;

import com.hazelcast.client.executor.tasks.CancellationAwareTask;
import com.hazelcast.client.executor.tasks.FailingCallable;
import com.hazelcast.client.executor.tasks.MapPutRunnable;
import com.hazelcast.client.executor.tasks.SelectNoMembers;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.MemberSelector;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientExecutorServiceTest {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();
    private HazelcastInstance client;
    private HazelcastInstance instance;

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Before
    public void setup() throws IOException {
        instance = hazelcastFactory.newHazelcastInstance();
        hazelcastFactory.newHazelcastInstance();
        hazelcastFactory.newHazelcastInstance();
        hazelcastFactory.newHazelcastInstance();
        client = hazelcastFactory.newHazelcastClient();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetLocalExecutorStats() throws Throwable {
        IExecutorService service = client.getExecutorService(randomString());

        service.getLocalExecutorStats();
    }

    @Test
    public void testIsTerminated() throws InterruptedException, ExecutionException, TimeoutException {
        IExecutorService service = client.getExecutorService(randomString());

        assertFalse(service.isTerminated());
    }

    @Test
    public void testIsShutdown() throws InterruptedException, ExecutionException, TimeoutException {
        IExecutorService service = client.getExecutorService(randomString());

        assertFalse(service.isShutdown());
    }

    @Test
    public void testShutdownNow() throws InterruptedException, ExecutionException, TimeoutException {
        final IExecutorService service = client.getExecutorService(randomString());

        service.shutdownNow();

        assertTrueEventually(new AssertTask() {
            public void run() throws Exception {
                assertTrue(service.isShutdown());
            }
        });
    }

    @Test
    public void testShutdownMultipleTimes() throws InterruptedException, ExecutionException, TimeoutException {
        final IExecutorService service = client.getExecutorService(randomString());
        service.shutdownNow();
        service.shutdown();

        assertTrueEventually(new AssertTask() {
            public void run() throws Exception {
                assertTrue(service.isShutdown());
            }
        });
    }

    @Test(expected = TimeoutException.class)
    public void testCancellationAwareTask_whenTimeOut() throws InterruptedException, ExecutionException, TimeoutException {
        IExecutorService service = client.getExecutorService(randomString());
        CancellationAwareTask task = new CancellationAwareTask(Long.MAX_VALUE);

        Future future = service.submit(task);

        future.get(1, TimeUnit.SECONDS);
    }

    @Test
    public void testFutureAfterCancellationAwareTaskTimeOut() throws InterruptedException, ExecutionException, TimeoutException {
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
    public void testCancelFutureAfterCancellationAwareTaskTimeOut() throws InterruptedException, ExecutionException, TimeoutException {
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
    public void testGetFutureAfterCancel() throws InterruptedException, ExecutionException, TimeoutException {
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
    public void testSubmitFailingCallableException() throws ExecutionException, InterruptedException {
        IExecutorService service = client.getExecutorService(randomString());
        Future<String> failingFuture = service.submit(new FailingCallable());

        failingFuture.get();
    }

    @Test
    public void testSubmitFailingCallableException_withExecutionCallback() throws ExecutionException, InterruptedException {
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
    public void testSubmitFailingCallableReasonExceptionCause() throws Throwable {
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
    public void testCallableSerializedOnce() throws ExecutionException, InterruptedException {
        String name = randomString();
        IExecutorService service = client.getExecutorService(name);
        SerializedCounterCallable counterCallable = new SerializedCounterCallable();
        Future future = service.submitToKeyOwner(counterCallable, name);
        assertEquals(2, future.get());
    }

    @Test
    public void testCallableSerializedOnce_submitToAddress() throws ExecutionException, InterruptedException {
        String name = randomString();
        IExecutorService service = client.getExecutorService(name);
        SerializedCounterCallable counterCallable = new SerializedCounterCallable();
        Future future = service.submitToMember(counterCallable, instance.getCluster().getLocalMember());
        assertEquals(2, future.get());
    }

    static class SerializedCounterCallable implements Callable, DataSerializable {

        int counter;

        public SerializedCounterCallable() {
        }

        @Override
        public Object call() throws Exception {
            return counter;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(++counter);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            counter = in.readInt() + 1;
        }
    }
}
