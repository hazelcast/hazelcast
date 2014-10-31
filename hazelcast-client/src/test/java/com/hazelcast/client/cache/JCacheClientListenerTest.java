/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryExpiredListener;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryRemovedListener;
import javax.cache.event.CacheEntryUpdatedListener;
import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class JCacheClientListenerTest {

    @Test
    public void testSyncListenerFromClient() throws Exception {
        try {
            HazelcastInstance hz1 = Hazelcast.newHazelcastInstance();
            HazelcastInstance hz2 = Hazelcast.newHazelcastInstance();

            ClientConfig clientConfig = new ClientConfig();
            clientConfig.getNetworkConfig().addAddress("127.0.0.1");

            HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

            HazelcastClientCachingProvider cachingProvider = HazelcastClientCachingProvider.createCachingProvider(client);
            CacheManager cacheManager = cachingProvider.getCacheManager();

            AtomicInteger counter = new AtomicInteger();

            CompleteConfiguration<String, String> config = new MutableConfiguration<String, String>()
                    .setTypes(String.class, String.class)
                    .addCacheEntryListenerConfiguration(
                            new MutableCacheEntryListenerConfiguration<String, String>(
                                    FactoryBuilder.factoryOf(new TestListener(counter)), null, true, true
                            )
                    );

            final Cache<String, String> cache = cacheManager.createCache("test", config);

            final CountDownLatch[] latches = new CountDownLatch[10];
            for (int o = 0; o < latches.length; o++) {
                final int index = o;
                latches[o] = new CountDownLatch(1);
                new Thread() {
                    public void run() {
                        for (int i = 0; i < 100000; i++) {
                            String key = String.valueOf(i % 10);
                            String value = UUID.randomUUID().toString();
                            System.out.println("key=" + key + ", value=" + value);
                            cache.put(key, value);
                        }
                        latches[index].countDown();
                    }
                }.start();
            }

            for (int o = 0; o < latches.length; o++) {
                latches[o].await();
            }

            System.out.println("Listener invoked locally: " + counter.get());

        } finally {
            HazelcastClient.shutdownAll();
            Hazelcast.shutdownAll();
        }
    }

    public static class TestListener
            implements CacheEntryCreatedListener<String, String>,
                       CacheEntryUpdatedListener<String, String>,
                       CacheEntryRemovedListener<String, String>,
                       CacheEntryExpiredListener<String, String>,
                       Serializable {

        private final AtomicInteger counter;

        public TestListener(AtomicInteger counter) {
            this.counter = counter;
        }

        @Override
        public void onCreated(Iterable<CacheEntryEvent<? extends String, ? extends String>> cacheEntryEvents)
                throws CacheEntryListenerException {

            for (CacheEntryEvent cacheEntryEvent : cacheEntryEvents) {
                System.out.println(cacheEntryEvent);
                counter.incrementAndGet();
            }
        }

        @Override
        public void onExpired(Iterable<CacheEntryEvent<? extends String, ? extends String>> cacheEntryEvents)
                throws CacheEntryListenerException {

            for (CacheEntryEvent cacheEntryEvent : cacheEntryEvents) {
                System.out.println(cacheEntryEvent);
                counter.incrementAndGet();
            }
        }

        @Override
        public void onRemoved(Iterable<CacheEntryEvent<? extends String, ? extends String>> cacheEntryEvents)
                throws CacheEntryListenerException {

            for (CacheEntryEvent cacheEntryEvent : cacheEntryEvents) {
                System.out.println(cacheEntryEvent);
                counter.incrementAndGet();
            }
        }

        @Override
        public void onUpdated(Iterable<CacheEntryEvent<? extends String, ? extends String>> cacheEntryEvents)
                throws CacheEntryListenerException {

            for (CacheEntryEvent cacheEntryEvent : cacheEntryEvents) {
                System.out.println(cacheEntryEvent);
                counter.incrementAndGet();
            }
        }
    }
}
