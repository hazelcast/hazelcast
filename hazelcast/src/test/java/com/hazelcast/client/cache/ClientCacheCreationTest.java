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

package com.hazelcast.client.cache;

import com.hazelcast.cache.CacheCreationTest;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.properties.ClientProperty;
import com.hazelcast.config.Config;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastOverloadException;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.executor.ExecutorServiceTestSupport;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.CacheManager;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.spi.CachingProvider;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.cache.CacheTestSupport.createClientCachingProvider;
import static java.util.Collections.singletonList;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class ClientCacheCreationTest extends CacheCreationTest {

    @Override
    protected CachingProvider createCachingProvider(Config hzConfig) {
        Hazelcast.newHazelcastInstance(hzConfig);
        ClientConfig clientConfig = null;
        if (hzConfig != null) {
            clientConfig = new ClientConfig();
            clientConfig.setClusterName(hzConfig.getClusterName());
            clientConfig.getNetworkConfig().setAddresses(singletonList("127.0.0.1"));
        }
        return createClientCachingProvider(HazelcastClient.newHazelcastClient(clientConfig));
    }

    @Test(expected = OperationTimeoutException.class)
    public void createSingleCache_whenMemberDown_throwsOperationTimeoutException() {
        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        clientConfig.setProperty(ClientProperty.INVOCATION_TIMEOUT_SECONDS.getName(), "2");
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        HazelcastClientCachingProvider cachingProvider = createClientCachingProvider(client);
        CacheManager cacheManager = cachingProvider.getCacheManager();

        hazelcastInstance.shutdown();
        MutableConfiguration configuration = new MutableConfiguration();
        cacheManager.createCache("xmlCache", configuration);
    }

    @Test
    public void createSingleCache_whenMemberBounce() throws InterruptedException {
        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        HazelcastClientCachingProvider cachingProvider = createClientCachingProvider(client);
        final CacheManager cacheManager = cachingProvider.getCacheManager();

        hazelcastInstance.shutdown();

        final CountDownLatch cacheCreated = new CountDownLatch(1);
        new Thread(new Runnable() {
            @Override
            public void run() {
                MutableConfiguration configuration = new MutableConfiguration();
                cacheManager.createCache("xmlCache", configuration);
                cacheCreated.countDown();
            }
        }).start();

        //leave some gap to let create cache to start and retry
        Thread.sleep(2000);
        Hazelcast.newHazelcastInstance();
        assertOpenEventually(cacheCreated);
    }

    @Test
    public void recreateCacheOnRestartedCluster_whenMaxConcurrentInvocationLow() {
        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        clientConfig.setProperty(ClientProperty.MAX_CONCURRENT_INVOCATIONS.getName(), "1");
        // disable metrics collection (the periodic send statistics task may interfere with the test)
        clientConfig.getMetricsConfig().setEnabled(false);
        // disable backup acknowledgements (the backup listener registration may interfere with the test)
        clientConfig.setBackupAckToClientEnabled(false);
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        HazelcastClientCachingProvider cachingProvider = createClientCachingProvider(client);
        final CacheManager cacheManager = cachingProvider.getCacheManager();
        MutableConfiguration configuration = new MutableConfiguration();
        // ensure cache is created despite the low concurrent invocation limit
        assertTrueEventually(() -> {
            try {
                cacheManager.createCache("xmlCache", configuration);
            } catch (HazelcastOverloadException e) {
                throw new AssertionError("Could not create cache due to "
                        + "low concurrent invocation count.");
            }
        });

        IExecutorService executorService = client.getExecutorService("exec");
        //keep the slot for one invocation to test if client can reconnect even if all slots are kept
        CountDownLatch testFinished = new CountDownLatch(1);
        executorService.submit(new ExecutorServiceTestSupport.SleepingTask(0),
                new ExecutionCallback<Boolean>() {
                    @Override
                    public void onResponse(Boolean response) {
                        try {
                            testFinished.await();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        try {
                            testFinished.await();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                });

        hazelcastInstance.shutdown();

        HazelcastInstance instance = Hazelcast.newHazelcastInstance();
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                try {
                    instance.getCacheManager().getCache("xmlCache");
                } catch (Exception e) {
                    fail();
                }
            }
        });
        testFinished.countDown();
    }

    @Test
    @Ignore("Only applicable for member-side HazelcastInstance")
    @Override
    public void createInvalidCache_fromDeclarativeConfig_throwsException_fromHazelcastInstanceCreation() {
    }

    @Override
    public void teardown() {
        super.teardown();
        HazelcastClient.shutdownAll();
    }
}
