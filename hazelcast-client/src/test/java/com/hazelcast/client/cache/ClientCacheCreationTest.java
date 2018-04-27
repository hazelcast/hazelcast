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

package com.hazelcast.client.cache;

import com.hazelcast.cache.CacheCreationTest;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.HazelcastClientManager;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.spi.properties.ClientProperty;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.OperationTimeoutException;
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

import static java.util.Collections.singletonList;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class ClientCacheCreationTest extends CacheCreationTest {

    @Override
    protected CachingProvider createCachingProvider(Config hzConfig) {
        Hazelcast.newHazelcastInstance(hzConfig);
        ClientConfig clientConfig = null;
        if (hzConfig != null) {
            clientConfig = new ClientConfig();
            clientConfig.getGroupConfig().setName(hzConfig.getGroupConfig().getName());
            clientConfig.getGroupConfig().setPassword(hzConfig.getGroupConfig().getPassword());
            clientConfig.getNetworkConfig().setAddresses(singletonList("127.0.0.1"));
        }
        return HazelcastClientCachingProvider.createCachingProvider(HazelcastClient.newHazelcastClient(clientConfig));
    }

    @Test(expected = OperationTimeoutException.class)
    public void createSingleCache_whenMemberDown_throwsOperationTimeoutException() {
        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setConnectionAttemptLimit(Integer.MAX_VALUE);
        clientConfig.setProperty(ClientProperty.INVOCATION_TIMEOUT_SECONDS.getName(), "2");
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        HazelcastClientCachingProvider cachingProvider = HazelcastClientCachingProvider.createCachingProvider(client);
        CacheManager cacheManager = cachingProvider.getCacheManager();

        hazelcastInstance.shutdown();
        MutableConfiguration configuration = new MutableConfiguration();
        cacheManager.createCache("xmlCache", configuration);
    }

    @Test
    public void createSingleCache_whenMemberBounce() throws InterruptedException {
        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setConnectionAttemptLimit(Integer.MAX_VALUE);
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        HazelcastClientCachingProvider cachingProvider = HazelcastClientCachingProvider.createCachingProvider(client);
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
    @Ignore("Only applicable for member-side HazelcastInstance")
    @Override
    public void createInvalidCache_fromDeclarativeConfig_throwsException_fromHazelcastInstanceCreation() {
    }

    @Override
    public void teardown() {
        super.teardown();
        HazelcastClientManager.shutdownAll();
    }
}
