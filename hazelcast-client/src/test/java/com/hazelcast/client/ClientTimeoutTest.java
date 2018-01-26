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
import com.hazelcast.client.spi.properties.ClientProperty;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientTimeoutTest {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void cleanup() {
        hazelcastFactory.terminateAll();
    }


    @Test(timeout = 20000, expected = IllegalStateException.class)
    public void testTimeoutToOutsideNetwork() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getGroupConfig().setName("dev").setPassword("dev-pass");
        clientConfig.getNetworkConfig().addAddress("8.8.8.8:5701");
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);
        client.getList("test");
    }

    @Test
    public void testConnectionTimeout_withIntMax() {
        testConnectionTimeout(Integer.MAX_VALUE);
    }

    @Test
    public void testConnectionTimeout_withZeroValue() {
        testConnectionTimeout(0);
    }

    public void testConnectionTimeout(int timeoutInMillis) {
        //Should work without throwing exception.
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setConnectionTimeout(timeoutInMillis);
        hazelcastFactory.newHazelcastInstance();
        hazelcastFactory.newHazelcastClient(clientConfig);
    }

    @Test(expected = OperationTimeoutException.class)
    public void testInvocationTimeOut() throws Throwable {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(ClientProperty.INVOCATION_TIMEOUT_SECONDS.getName(), "0");
        hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);
        IExecutorService executorService = client.getExecutorService("test");
        Future<Boolean> future = executorService.submit(new RetryableExceptionThrowingCallable());
        try {
            future.get();
        } catch (InterruptedException e) {
            //ignored
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }

    public static class RetryableExceptionThrowingCallable implements Callable, Serializable {
        public Object call() throws Exception {
            throw new RetryableHazelcastException();
        }
    }
}
