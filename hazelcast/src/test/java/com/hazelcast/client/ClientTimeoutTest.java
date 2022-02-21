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
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.properties.ClientProperty;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Tests in this class intentionally use real network.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class ClientTimeoutTest {

    @After
    public void cleanup() {
        Hazelcast.shutdownAll();
        HazelcastClient.shutdownAll();
    }

    @Test(timeout = 20000, expected = IllegalStateException.class)
    public void testTimeoutToOutsideNetwork() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName("dev");

        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        networkConfig.addAddress("8.8.8.8:5701");
        // Do only one connection-attempt
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(1000);
        // Timeout connection-attempt after 1000 millis
        networkConfig.setConnectionTimeout(1000);

        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
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
        Hazelcast.newHazelcastInstance();
        HazelcastClient.newHazelcastClient(clientConfig);
    }

    @Test(expected = OperationTimeoutException.class)
    public void testInvocationTimeOut() throws Throwable {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(ClientProperty.INVOCATION_TIMEOUT_SECONDS.getName(), "0");
        Hazelcast.newHazelcastInstance();
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
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
