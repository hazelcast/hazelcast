/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.spi.properties.ClientProperty;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.client.util.ClientDelegatingFuture;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastOverloadException;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
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
            try {

                Thread.sleep(millis);
            } catch (InterruptedException e) {
                //ignored
            }
            return null;
        }
    }

    @Test(expected = HazelcastOverloadException.class)
    public void testMaxAllowed_withUnfinishedCallback() throws ExecutionException, InterruptedException {
        int MAX_ALLOWED = 10;
        hazelcastFactory.newHazelcastInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(ClientProperty.MAX_CONCURRENT_INVOCATIONS.getName(), String.valueOf(MAX_ALLOWED));
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);
        String name = randomString();
        IMap map = client.getMap(name);

        IExecutorService executorService = client.getExecutorService(randomString());
        for (int i = 0; i < MAX_ALLOWED - 1; i++) {
            executorService.submit(new SleepyProcessor(Integer.MAX_VALUE));
        }

        ClientDelegatingFuture future = (ClientDelegatingFuture) executorService.submit(new SleepyProcessor(2000));
        CountDownLatch countDownLatch = new CountDownLatch(1);
        future.andThenInternal(new SleepyCallback(countDownLatch), false);
        future.get();
        try {
            map.get(1);
        } catch (HazelcastOverloadException e) {
            throw e;
        } finally {
            countDownLatch.countDown();
        }
    }

    static class SleepyCallback implements ExecutionCallback<ClientMessage> {

        final CountDownLatch countDownLatch;

        public SleepyCallback(CountDownLatch countDownLatch) {
            this.countDownLatch = countDownLatch;
        }

        @Override
        public void onResponse(ClientMessage response) {
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                //ignored
            }
        }

        @Override
        public void onFailure(Throwable t) {

        }
    }
}
