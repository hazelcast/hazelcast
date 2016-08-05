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

package com.hazelcast.client.spi.impl;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientProperty;
import com.hazelcast.client.impl.HazelcastClientProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastOverloadException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientUserExecutorQueueCapacityTest
        extends HazelcastTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void cleanup() {
        hazelcastFactory.terminateAll();
    }

    @Test(expected = HazelcastOverloadException.class)
    public void testUserExecutorQueueOverload() {
        final int MAX_ALLOWED = 5;

        final int MAX_POOL_SIZE = 2;

        final long THREAD_SLEEP_TIME = 30000;

        hazelcastFactory.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();

        clientConfig.setExecutorPoolSize(MAX_POOL_SIZE);

        clientConfig.setProperty(ClientProperty.USER_EXECUTOR_QUEUE_CAPACITY.getName(), String.valueOf(MAX_ALLOWED));

        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        HazelcastClientProxy clientProxy = (HazelcastClientProxy) client;

        ExecutorService userExecutor = clientProxy.client.getClientExecutionService().getAsyncExecutor();

        CountDownLatch testFinishLatch = new CountDownLatch(1);

        CountDownLatch startPoolLatch = new CountDownLatch(MAX_POOL_SIZE);

        // run threads to consume the executor available pool
        for (int i = 0; i < MAX_POOL_SIZE; i++) {
            userExecutor.execute(new SleepyProcessor(startPoolLatch, testFinishLatch));
        }

        try {
            assertTrue(startPoolLatch.await(2, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            fail("Could not wait for pool threads to start.");
        }

        // Run threads to fill the queue
        for (int i = 0; i < MAX_ALLOWED; i++) {
            userExecutor.execute(new SleepyProcessor(testFinishLatch));
        }

        try {
            // Try executing another thread and we should get the exception
            userExecutor.execute(new SleepyProcessor(testFinishLatch));
        } finally {
            testFinishLatch.countDown();
        }
    }

    static class SleepyProcessor implements Runnable, Serializable {

        private CountDownLatch startLatch;
        private final CountDownLatch finishLatch;

        SleepyProcessor(CountDownLatch startThreadLatch, CountDownLatch threadFinishLatch) {
            this.startLatch = startThreadLatch;
            this.finishLatch = threadFinishLatch;
        }

        SleepyProcessor(CountDownLatch threadFinishLatch) {
            this.finishLatch = threadFinishLatch;
        }

        @Override
        public void run() {
            if (null != startLatch) {
                startLatch.countDown();
            }
            try {
                finishLatch.await();
            } catch (InterruptedException e) {
                fail("Sleepy thread could not sleep enough");
            }
        }
    }
}
