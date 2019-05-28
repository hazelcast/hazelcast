/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.CacheManager;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.spi.CachingProvider;

import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientCacheProxyTest extends ClientTestSupport {

    private TestHazelcastFactory factory = new TestHazelcastFactory();

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void clusterRestart_proxyRemainsUsableOnClient() {
        HazelcastInstance instance = factory.newHazelcastInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setConnectionAttemptLimit(Integer.MAX_VALUE);
        HazelcastInstance client = factory.newHazelcastClient(clientConfig);

        CachingProvider cachingProvider = HazelcastClientCachingProvider.createCachingProvider(client);

        CacheManager cacheManager = cachingProvider.getCacheManager();

        CompleteConfiguration<String, String> config =
                new MutableConfiguration<String, String>()
                        .setTypes(String.class, String.class);

        javax.cache.Cache<String, String> cache = cacheManager.createCache("example", config);
        //restarting cluster
        instance.shutdown();

        final CountDownLatch clientConnectedBack = new CountDownLatch(1);
        client.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                if (LifecycleEvent.LifecycleState.CLIENT_CONNECTED.equals(event.getState())) {
                    clientConnectedBack.countDown();
                }
            }
        });

        factory.newHazelcastInstance();

        assertOpenEventually(clientConnectedBack);
        //expected to work without throwing exception
        assertNull(cache.get("key"));

    }

    @Test
    public void clientRestart_proxyRemainsUsableOnClient() {
        factory.newHazelcastInstance();

        HazelcastInstance client = factory.newHazelcastClient();
        CachingProvider cachingProvider = HazelcastClientCachingProvider.createCachingProvider(client);
        CacheManager cacheManager = cachingProvider.getCacheManager();
        CompleteConfiguration<String, String> config =
                new MutableConfiguration<String, String>()
                        .setTypes(String.class, String.class);
        String cacheName = "example";
        cacheManager.createCache(cacheName, config);

        //restarting client and getting already created cache
        client.shutdown();
        client = factory.newHazelcastClient();
        cachingProvider = HazelcastClientCachingProvider.createCachingProvider(client);
        cacheManager = cachingProvider.getCacheManager();
        javax.cache.Cache<String, String> cache = cacheManager.getCache(cacheName);

        //expected to work without throwing exception
        assertNull(cache.get("key"));
    }
}

