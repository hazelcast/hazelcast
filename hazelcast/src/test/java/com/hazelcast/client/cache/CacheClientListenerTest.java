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

import com.hazelcast.cache.CacheListenerTest;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.cache.CacheTestSupport.createClientCachingProvider;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class CacheClientListenerTest extends CacheListenerTest {
    @Before
    @After
    public void cleanup() {
        HazelcastClient.shutdownAll();
        HazelcastInstanceFactory.terminateAll();
    }

    @Override
    protected CachingProvider getCachingProvider() {
        Config config = new Config();
        JoinConfig joinConfig = config.getNetworkConfig().getJoin();
        joinConfig.getAwsConfig().setEnabled(false);
        joinConfig.getMulticastConfig().setEnabled(false);
        joinConfig.getTcpIpConfig().setEnabled(false);

        Hazelcast.newHazelcastInstance(config);
        Hazelcast.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().addAddress("127.0.0.1");

        hazelcastInstance = HazelcastClient.newHazelcastClient(clientConfig);

        return createClientCachingProvider(hazelcastInstance);
    }

    @Test
    public void testEntryListenerUsingMemberConfig() {
        System.setProperty("hazelcast.config", "classpath:hazelcast-cache-entrylistener-test.xml");
        Hazelcast.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().addAddress("127.0.0.1");

        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        HazelcastClientCachingProvider cachingProvider = createClientCachingProvider(client);

        CacheManager cacheManager = cachingProvider.getCacheManager();

        Cache<String, String> cache = cacheManager.getCache("entrylistenertestcache");

        cache.put("foo", "bar");

        HazelcastInstance client2 = HazelcastClient.newHazelcastClient(clientConfig);

        HazelcastClientCachingProvider cachingProvider2 = createClientCachingProvider(client2);

        CacheManager cacheManager2 = cachingProvider2.getCacheManager();

        Cache<String, String> cache2 = cacheManager2.getCache("entrylistenertestcache");

        client.shutdown();

        // sleep enough so that the put entry is expired
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Trigger expiry event
        cache2.get("foo");

        final CountDownLatch expiredLatch = ClientCacheEntryExpiredLatchCountdownListener.getExpiredLatch();
        assertCountEventually("The expired event should only be received one time", 1, expiredLatch, 3);

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertEquals("Expired event is received more than once", 1, expiredLatch.getCount());
            }
        }, 3);
    }
}
