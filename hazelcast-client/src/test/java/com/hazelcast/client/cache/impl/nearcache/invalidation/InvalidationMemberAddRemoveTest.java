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

package com.hazelcast.client.cache.impl.nearcache.invalidation;

import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.client.cache.impl.nearcache.ClientNearCacheTestSupport;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.HazelcastClientProxy;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.ENTRY_COUNT;
import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.util.RandomPicker.getInt;
import static java.lang.Integer.MAX_VALUE;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class InvalidationMemberAddRemoveTest extends ClientNearCacheTestSupport {

    @Override
    protected Config createConfig() {
        Config config = super.createConfig();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "271");
        config.setProperty(GroupProperty.CACHE_INVALIDATION_MESSAGE_BATCH_ENABLED.getName(), "true");
        config.setProperty(GroupProperty.CACHE_INVALIDATION_MESSAGE_BATCH_SIZE.getName(), "10");
        return config;
    }

    protected ClientConfig createClientConfig() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty("hazelcast.invalidation.max.tolerated.miss.count", "0");
        return clientConfig;
    }

    @Override
    protected CacheConfig createCacheConfig(InMemoryFormat inMemoryFormat) {
        CacheConfig cacheConfig = super.createCacheConfig(inMemoryFormat);
        cacheConfig.getEvictionConfig()
                .setMaximumSizePolicy(ENTRY_COUNT)
                .setSize(MAX_VALUE);
        return cacheConfig;
    }


    @Override
    protected NearCacheConfig createNearCacheConfig(InMemoryFormat inMemoryFormat) {
        NearCacheConfig nearCacheConfig = super.createNearCacheConfig(inMemoryFormat);
        nearCacheConfig.setInvalidateOnChange(true)
                .getEvictionConfig()
                .setMaximumSizePolicy(ENTRY_COUNT)
                .setSize(MAX_VALUE);
        return nearCacheConfig;
    }

    @Test
    public void ensure_nearCachedClient_and_member_data_sync_eventually() throws Exception {
        final int cacheSize = 100000;
        final AtomicBoolean stopTest = new AtomicBoolean();

        final Config config = createConfig();
        hazelcastFactory.newHazelcastInstance(config);

        CachingProvider provider = HazelcastServerCachingProvider.createCachingProvider(serverInstance);
        CacheManager serverCacheManager = provider.getCacheManager();

        // populated from member.
        final Cache<Integer, Integer> memberCache = serverCacheManager.createCache(DEFAULT_CACHE_NAME, createCacheConfig(BINARY));
        for (int i = 0; i < cacheSize; i++) {
            memberCache.put(i, i);
        }

        ClientConfig clientConfig = createClientConfig();
        clientConfig.addNearCacheConfig(createNearCacheConfig(BINARY));
        HazelcastClientProxy client = (HazelcastClientProxy) hazelcastFactory.newHazelcastClient(clientConfig);
        CachingProvider clientCachingProvider = HazelcastClientCachingProvider.createCachingProvider(client);

        final Cache<Integer, Integer> clientCache = clientCachingProvider.getCacheManager().createCache(
                DEFAULT_CACHE_NAME, createCacheConfig(BINARY));


        // continuously adds and removes member
        Thread shadowMember = new Thread(new Runnable() {
            @Override
            public void run() {
                while (!stopTest.get()) {
                    HazelcastInstance member = hazelcastFactory.newHazelcastInstance(config);
                    sleepSeconds(5);
                    member.getLifecycleService().shutdown();
                }

            }
        });

        // populates client near-cache
        Thread populateClientNearCache = new Thread(new Runnable() {
            public void run() {
                while (!stopTest.get()) {
                    for (int i = 0; i < cacheSize; i++) {
                        clientCache.get(i);
                    }
                }
            }
        });

        // updates data from member.
        Thread putFromMember = new Thread(new Runnable() {
            public void run() {
                while (!stopTest.get()) {
                    int key = getInt(cacheSize);
                    int value = getInt(Integer.MAX_VALUE);
                    memberCache.put(key, value);

                    sleepAtLeastMillis(5);
                }
            }
        });

        // start threads
        putFromMember.start();
        populateClientNearCache.start();
        shadowMember.start();

        // stress system some seconds
        sleepSeconds(60);

        //stop threads
        stopTest.set(true);
        shadowMember.join();
        populateClientNearCache.join();
        putFromMember.join();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (int i = 0; i < cacheSize; i++) {
                    Integer valueSeenFromMember = memberCache.get(i);
                    Integer valueSeenFromClient = clientCache.get(i);

                    assertEquals(valueSeenFromMember, valueSeenFromClient);
                }
            }
        });
    }
}
