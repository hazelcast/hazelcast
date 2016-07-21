/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.executor.tasks.FailingCallable;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.DurableExecutorConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.durableexecutor.DurableExecutorService;
import com.hazelcast.durableexecutor.DurableExecutorServiceFuture;
import com.hazelcast.executor.ExecutorServiceTestSupport.BasicTestCallable;
import com.hazelcast.executor.ExecutorServiceTestSupport.SleepingTask;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientDurableExecutorServiceTest {

    private static final String SINGLE_TASK = "singleTask";

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();
    private HazelcastInstance client;

    @Before
    public void setup() {
        Config config = new Config();
        DurableExecutorConfig durableExecutorConfig = config.getDurableExecutorConfig(SINGLE_TASK + "*");
        durableExecutorConfig.setCapacity(1);

        hazelcastFactory.newHazelcastInstance(config);
        hazelcastFactory.newHazelcastInstance(config);
        hazelcastFactory.newHazelcastInstance(config);
        hazelcastFactory.newHazelcastInstance(config);
        client = hazelcastFactory.newHazelcastClient();
    }

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testInvokeAll() throws Exception {
        DurableExecutorService service = client.getDurableExecutorService(randomString());
        List<BasicTestCallable> callables = Collections.emptyList();
        service.invokeAll(callables);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testInvokeAll_WithTimeout() throws Exception {
        DurableExecutorService service = client.getDurableExecutorService(randomString());
        List<BasicTestCallable> callables = Collections.emptyList();
        service.invokeAll(callables, 1, TimeUnit.SECONDS);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testInvokeAny() throws Exception {
        DurableExecutorService service = client.getDurableExecutorService(randomString());
        List<BasicTestCallable> callables = Collections.emptyList();
        service.invokeAny(callables);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testInvokeAny_WithTimeout() throws Exception {
        DurableExecutorService service = client.getDurableExecutorService(randomString());
        List<BasicTestCallable> callables = Collections.emptyList();
        service.invokeAny(callables, 1, TimeUnit.SECONDS);
    }

    @Test
    public void testAwaitTermination() throws Exception {
        DurableExecutorService service = client.getDurableExecutorService(randomString());
        assertFalse(service.awaitTermination(1, TimeUnit.SECONDS));
    }

    @Test
    public void testFullRingBuffer() throws Exception {
        String key = randomString();
        DurableExecutorService service = client.getDurableExecutorService(SINGLE_TASK + randomString());
        service.submitToKeyOwner(new SleepingTask(100), key);
        DurableExecutorServiceFuture<String> future = service.submitToKeyOwner(new BasicTestCallable(), key);
        try {
            future.get();
            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof RejectedExecutionException);
        }
    }

    @Test
    public void testIsTerminated() {
        DurableExecutorService service = client.getDurableExecutorService(randomString());

        assertFalse(service.isTerminated());
    }

    @Test
    public void testIsShutdown() {
        DurableExecutorService service = client.getDurableExecutorService(randomString());

        assertFalse(service.isShutdown());
    }

    @Test
    public void testShutdownNow() {
        final DurableExecutorService service = client.getDurableExecutorService(randomString());

        service.shutdownNow();

        assertTrueEventually(new AssertTask() {
            public void run() throws Exception {
                assertTrue(service.isShutdown());
            }
        });
    }

    @Test
    public void testShutdownMultipleTimes() {
        final DurableExecutorService service = client.getDurableExecutorService(randomString());
        service.shutdownNow();
        service.shutdown();

        assertTrueEventually(new AssertTask() {
            public void run() throws Exception {
                assertTrue(service.isShutdown());
            }
        });
    }

    @Test(expected = ExecutionException.class)
    public void testSubmitFailingCallableException() throws Exception {
        DurableExecutorService service = client.getDurableExecutorService(randomString());
        Future<String> failingFuture = service.submit(new FailingCallable());

        failingFuture.get();
    }

    @Test
    public void testSubmitFailingCallableException_withExecutionCallback() throws Exception {
        DurableExecutorService service = client.getDurableExecutorService(randomString());
        final CountDownLatch latch = new CountDownLatch(1);
        service.submit(new FailingCallable()).andThen(new ExecutionCallback<String>() {
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
        DurableExecutorService service = client.getDurableExecutorService(randomString());
        Future<String> failingFuture = service.submit(new FailingCallable());

        try {
            failingFuture.get();
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }

    @Test
    public void testCallableSerializedOnce() throws Exception {
        String name = randomString();
        DurableExecutorService service = client.getDurableExecutorService(name);
        SerializedCounterCallable counterCallable = new SerializedCounterCallable();
        Future future = service.submitToKeyOwner(counterCallable, name);
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
