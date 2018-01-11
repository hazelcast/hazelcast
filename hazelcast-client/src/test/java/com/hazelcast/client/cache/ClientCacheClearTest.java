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

import com.hazelcast.cache.CacheClearTest;
import com.hazelcast.cache.ICache;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.HazelcastClientProxy;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheAddInvalidationListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CacheRemoveEntryListenerCodec;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.impl.ListenerMessageCodec;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionConfig.MaxSizePolicy;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.spi.CachingProvider;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientCacheClearTest extends CacheClearTest {

    private TestHazelcastFactory clientFactory;
    private HazelcastInstance client;

    @Override
    protected TestHazelcastInstanceFactory getInstanceFactory(int instanceCount) {
        clientFactory = new TestHazelcastFactory();
        return clientFactory;
    }

    protected ClientConfig createClientConfig() {
        NearCacheConfig nearCacheConfig = new NearCacheConfig("myCache")
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setCacheLocalEntries(false)
                .setEvictionConfig(new EvictionConfig(10000, MaxSizePolicy.ENTRY_COUNT, EvictionPolicy.LFU))
                .setInvalidateOnChange(true)
                .setLocalUpdatePolicy(NearCacheConfig.LocalUpdatePolicy.CACHE_ON_UPDATE)
                .setMaxIdleSeconds(600)
                .setTimeToLiveSeconds(60);

        return new ClientConfig()
                .addNearCacheConfig(nearCacheConfig);
    }

    @Override
    protected void onSetup() {
        super.onSetup();
        ClientConfig clientConfig = createClientConfig();
        client = clientFactory.newHazelcastClient(clientConfig);
    }

    @Test
    public void testClientInvalidationListenerCallCount() {
        ICache<String, String> cache = createCache();
        Map<String, String> entries = createAndFillEntries();

        for (Map.Entry<String, String> entry : entries.entrySet()) {
            cache.put(entry.getKey(), entry.getValue());
        }

        // Verify that put works
        for (Map.Entry<String, String> entry : entries.entrySet()) {
            String key = entry.getKey();
            String expectedValue = entries.get(key);
            String actualValue = cache.get(key);
            assertEquals(expectedValue, actualValue);
        }

        final AtomicInteger counter = new AtomicInteger(0);

        CacheConfig config = cache.getConfiguration(CacheConfig.class);

        registerInvalidationListener(new EventHandler() {
            @Override
            public void handle(Object event) {
                counter.getAndIncrement();
            }

            @Override
            public void beforeListenerRegister() {

            }

            @Override
            public void onListenerRegister() {

            }
        }, config.getNameWithPrefix());

        cache.clear();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertEquals(1, counter.get());
            }
        }, 2);

        // Make sure that the callback is not called for a while
        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertTrue(counter.get() <= 1);
            }
        }, 3);

    }

    @Test
    public void testClientInvalidationListenerCallCountWhenServerCacheClearUsed() {
        ICache<String, String> cache = createCache();
        Map<String, String> entries = createAndFillEntries();

        for (Map.Entry<String, String> entry : entries.entrySet()) {
            cache.put(entry.getKey(), entry.getValue());
        }

        // Verify that put works
        for (Map.Entry<String, String> entry : entries.entrySet()) {
            String key = entry.getKey();
            String expectedValue = entries.get(key);
            String actualValue = cache.get(key);
            assertEquals(expectedValue, actualValue);
        }

        final AtomicInteger counter = new AtomicInteger(0);

        CacheConfig config = cache.getConfiguration(CacheConfig.class);

        registerInvalidationListener(new EventHandler() {
            @Override
            public void handle(Object event) {
                counter.getAndIncrement();
            }

            @Override
            public void beforeListenerRegister() {

            }

            @Override
            public void onListenerRegister() {

            }
        }, config.getNameWithPrefix());

        ICache<Object, Object> serverCache = getHazelcastInstance().getCacheManager().getCache(config.getName());
        serverCache.clear();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertEquals(1, counter.get());
            }
        }, 2);

        // Make sure that the callback is not called for a while
        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertTrue(counter.get() <= 1);
            }
        }, 3);

    }

    @Override
    protected void onTearDown() {
        super.onTearDown();
        // Client factory is already shutdown at this test's super class (`CachePutAllTest`)
        // because it is returned instance factory from overridden `getInstanceFactory` method.
        client = null;
    }

    @Override
    protected CachingProvider getCachingProvider() {
        return HazelcastClientCachingProvider.createCachingProvider(client);
    }

    private void registerInvalidationListener(EventHandler handler, String nameWithPrefix) {
        ListenerMessageCodec listenerCodec = createInvalidationListenerCodec(nameWithPrefix);
        HazelcastClientProxy hzClient = (HazelcastClientProxy) client;
        final HazelcastClientInstanceImpl clientInstance = hzClient.client;
        clientInstance.getListenerService().registerListener(listenerCodec, handler);
    }

    private ListenerMessageCodec createInvalidationListenerCodec(final String nameWithPrefix) {
        return new ListenerMessageCodec() {
            @Override
            public ClientMessage encodeAddRequest(boolean localOnly) {
                return CacheAddInvalidationListenerCodec.encodeRequest(nameWithPrefix, localOnly);
            }

            @Override
            public String decodeAddResponse(ClientMessage clientMessage) {
                return CacheAddInvalidationListenerCodec.decodeResponse(clientMessage).response;
            }

            @Override
            public ClientMessage encodeRemoveRequest(String realRegistrationId) {
                return CacheRemoveEntryListenerCodec.encodeRequest(nameWithPrefix, realRegistrationId);
            }

            @Override
            public boolean decodeRemoveResponse(ClientMessage clientMessage) {
                return CacheRemoveEntryListenerCodec.decodeResponse(clientMessage).response;
            }
        };
    }

}
