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

package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.ClientDelegatingFuture;
import com.hazelcast.client.properties.ClientProperty;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastOverloadException;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientMaxAllowedInvocationTest extends ClientTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void cleanup() {
        hazelcastFactory.terminateAll();
    }


    @Test(expected = HazelcastOverloadException.class)
    public void testMaxAllowed_withSyncOperation() {
        int MAX_ALLOWED = 10;
        hazelcastFactory.newHazelcastInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(ClientProperty.MAX_CONCURRENT_INVOCATIONS.getName(), String.valueOf(MAX_ALLOWED));
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);
        IMap map = client.getMap(randomString());

        IExecutorService executorService = client.getExecutorService(randomString());

        for (int i = 0; i < MAX_ALLOWED; i++) {
            executorService.submit(new SleepyProcessor(Integer.MAX_VALUE));
        }

        map.get(2);
    }

    @Test(expected = HazelcastOverloadException.class)
    public void testMaxAllowed_withAsyncOperation() {
        int MAX_ALLOWED = 10;
        hazelcastFactory.newHazelcastInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(ClientProperty.MAX_CONCURRENT_INVOCATIONS.getName(), String.valueOf(MAX_ALLOWED));
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);
        IMap map = client.getMap(randomString());

        IExecutorService executorService = client.getExecutorService(randomString());

        for (int i = 0; i < MAX_ALLOWED; i++) {
            executorService.submit(new SleepyProcessor(Integer.MAX_VALUE));
        }

        map.getAsync(1);
    }

    static class SleepyProcessor implements Callable, Serializable {

        private long millis;

        SleepyProcessor(long millis) {
            this.millis = millis;
        }

        @Override
        public Object call() throws Exception {
            ILogger logger = Logger.getLogger(getClass());
            try {
                logger.info("SleepyProcessor(" + this + ") sleeping for " + millis + " milliseconds");
                Thread.sleep(millis);
                logger.info("SleepyProcessor(" + this + ") woke up.");
            } catch (InterruptedException e) {
                //ignored
                logger.info("SleepyProcessor(" + this + ") is interrupted.");
            }
            return null;
        }
    }

    @Test(expected = HazelcastOverloadException.class)
    public void testMaxAllowed_andThenInternal() throws ExecutionException, InterruptedException {
        testMaxAllowed((future, callback) -> future.whenCompleteAsync(callback));
    }

    @Test(expected = HazelcastOverloadException.class)
    public void testMaxAllowed_andThen() throws ExecutionException, InterruptedException {
        testMaxAllowed((future, callback) -> future.whenCompleteAsync(callback));
    }

    @Test(expected = HazelcastOverloadException.class)
    public void testMaxAllowed_andThenExecutor() throws ExecutionException, InterruptedException {
        testMaxAllowed((future, callback) -> {
            ExecutorService executor = Executors.newSingleThreadExecutor();
            future.whenCompleteAsync(callback, executor);
        });
    }

    private void testMaxAllowed(RegisterCallback registerCallbackCall) throws ExecutionException, InterruptedException {
        int MAX_ALLOWED = 10;
        hazelcastFactory.newHazelcastInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(ClientProperty.MAX_CONCURRENT_INVOCATIONS.getName(), String.valueOf(MAX_ALLOWED));
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);
        String name = randomString();
        IMap map = client.getMap(name);

        CountDownLatch countDownLatch = new CountDownLatch(1);
        SleepyCallback sleepyCallback = new SleepyCallback(countDownLatch);
        try {
            IExecutorService executorService = client.getExecutorService(randomString());
            for (int i = 0; i < MAX_ALLOWED - 1; i++) {
                executorService.submit(new SleepyProcessor(Integer.MAX_VALUE));
            }

            ClientDelegatingFuture future = (ClientDelegatingFuture) executorService.submit(new SleepyProcessor(0));
            registerCallbackCall.call(future, sleepyCallback);
            future.get();
            map.get(1);
        } finally {
            countDownLatch.countDown();
        }
    }

    @Test(expected = HazelcastOverloadException.class)
    public void testMaxAllowed_withWaitingCallbacks_andThenInternal() throws ExecutionException, InterruptedException {
        testMaxAllowed_withWaitingCallbacks((future, callback) -> future.whenCompleteAsync(callback));
    }

    @Test(expected = HazelcastOverloadException.class)
    public void testMaxAllowed_withWaitingCallbacks_a_andThen() throws ExecutionException, InterruptedException {
        testMaxAllowed_withWaitingCallbacks((future, callback) -> future.whenCompleteAsync(callback));
    }

    @Test(expected = HazelcastOverloadException.class)
    public void testMaxAllowed_withWaitingCallbacks_andThenExecutor() throws ExecutionException, InterruptedException {
        testMaxAllowed_withWaitingCallbacks((future, callback) -> {
            ExecutorService executor = Executors.newSingleThreadExecutor();
            future.whenCompleteAsync(callback, executor);
        });
    }

    private void testMaxAllowed_withWaitingCallbacks(RegisterCallback registerCallbackCall) throws ExecutionException, InterruptedException {
        int MAX_ALLOWED = 10;
        hazelcastFactory.newHazelcastInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(ClientProperty.MAX_CONCURRENT_INVOCATIONS.getName(), String.valueOf(MAX_ALLOWED));
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);
        String name = randomString();
        IMap map = client.getMap(name);

        IExecutorService executorService = client.getExecutorService(randomString());
        CountDownLatch countDownLatch = new CountDownLatch(1);
        SleepyCallback sleepyCallback = new SleepyCallback(countDownLatch);

        try {
            for (int i = 0; i < MAX_ALLOWED; i++) {
                ClientDelegatingFuture future = (ClientDelegatingFuture) executorService.submit(new SleepyProcessor(0));
                registerCallbackCall.call(future, sleepyCallback);
                future.get();
            }
            map.get(1);
        } finally {
            countDownLatch.countDown();
        }
    }

    interface RegisterCallback {
        void call(ClientDelegatingFuture clientDelegatingFuture, BiConsumer<Object, Throwable> biConsumer);
    }

    static class SleepyCallback implements BiConsumer<Object, Throwable> {
        final ILogger logger = Logger.getLogger(getClass());
        final CountDownLatch countDownLatch;

        SleepyCallback(CountDownLatch countDownLatch) {
            this.countDownLatch = countDownLatch;
        }

        @Override
        public void accept(Object response, Throwable t) {
            if (t == null) {
                try {
                    logger.info("SleepyCallback onResponse entered. Will await for latch.");
                    countDownLatch.await();
                    logger.info("SleepyCallback onResponse latch wait finished.");
                } catch (InterruptedException e) {
                    //ignored
                    logger.info("SleepyCallback onResponse is interrupted.");
                }
            } else {
                logger.info("SleepyCallback onFailure is entered.");
            }
        }
    }
}
