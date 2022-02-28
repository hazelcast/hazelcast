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

package com.hazelcast.cache.jsr;

import org.jsr107.tck.event.CacheEntryListenerClient;
import org.jsr107.tck.event.CacheEntryListenerServer;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import javax.cache.Cache;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListenerException;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;

// Base class for member and client side CacheListenerTest
public abstract class AbstractCacheListenerTest extends org.jsr107.tck.event.CacheListenerTest {

    @Rule
    public TestName testName = new TestName();

    // this field is private in the TCK test; when running our overridden test we use this field
    // otherwise the cacheEntryListenerServer is started by superclass
    private CacheEntryListenerServer cacheEntryListenerServer;

    private final Logger logger = Logger.getLogger(getClass().getName());

    @Override
    @After
    public void onAfterEachTest() {
        if (!testName.getMethodName().startsWith("testFilteredListener")) {
            super.onAfterEachTest();
            return;
        }

        //destroy the cache
        String cacheName = cache.getName();
        cache.getCacheManager().destroyCache(cacheName);

        //close the server
        cacheEntryListenerServer.close();
        cacheEntryListenerServer = null;

        cache = null;
    }

    @Override
    protected MutableConfiguration<Long, String> extraSetup(MutableConfiguration<Long, String> configuration) {
        if (!testName.getMethodName().startsWith("testFilteredListener")) {
            return super.extraSetup(configuration);
        }

        cacheEntryListenerServer = new CacheEntryListenerServer<Long, String>(10011, Long.class, String.class);
        try {
            cacheEntryListenerServer.open();
        } catch (IOException e) {
            e.printStackTrace();
        }

        //establish and open a CacheEntryListenerServer to handle cache
        //cache entry events from a CacheEntryListenerClient
        listener = new MyCacheEntryListener<Long, String>(oldValueRequired);
        cacheEntryListenerServer.addCacheEventListener(listener);

        //establish a CacheEntryListenerClient that a Cache can use for CacheEntryListening
        //(via the CacheEntryListenerServer)
        CacheEntryListenerClient<Long, String> clientListener =
                new CacheEntryListenerClient<Long, String>(cacheEntryListenerServer.getInetAddress(),
                        cacheEntryListenerServer.getPort());
        listenerConfiguration = new MutableCacheEntryListenerConfiguration<Long, String>(
                FactoryBuilder.factoryOf(clientListener),
                null,
                oldValueRequired,
                true);
        return configuration.addCacheEntryListenerConfiguration(listenerConfiguration);
    }

    @Override
    @Test
    public void testFilteredListener() {
        // remove standard listener.
        cacheEntryListenerServer.removeCacheEventListener(this.listener);
        cache.deregisterCacheEntryListener(this.listenerConfiguration);

        CacheEntryListenerClient<Long, String> clientListener =
                new CacheEntryListenerClient<Long, String>(cacheEntryListenerServer.getInetAddress(),
                        cacheEntryListenerServer.getPort());

        MyCacheEntryListener<Long, String> filteredListener = new MyCacheEntryListener<Long, String>(oldValueRequired);
        CacheEntryListenerConfiguration<Long, String> listenerConfiguration =
                new MutableCacheEntryListenerConfiguration<Long, String>(
                        FactoryBuilder.factoryOf(clientListener),
                        FactoryBuilder.factoryOf(new MyCacheEntryEventFilter()),
                        oldValueRequired, true);
        cache.registerCacheEntryListener(listenerConfiguration);
        cacheEntryListenerServer.addCacheEventListener(filteredListener);

        assertEquals(0, filteredListener.getCreated());
        assertEquals(0, filteredListener.getUpdated());
        assertEquals(0, filteredListener.getRemoved());

        cache.put(1L, "Sooty");
        assertEquals(1, filteredListener.getCreated());
        assertEquals(0, filteredListener.getUpdated());
        assertEquals(0, filteredListener.getRemoved());

        Map<Long, String> entries = new HashMap<Long, String>();
        entries.put(2L, "Lucky");
        entries.put(3L, "Bryn");
        cache.putAll(entries);
        assertEquals(2, filteredListener.getCreated());
        assertEquals(0, filteredListener.getUpdated());
        assertEquals(0, filteredListener.getRemoved());

        cache.put(1L, "Zyn");
        assertEquals(2, filteredListener.getCreated());
        assertEquals(0, filteredListener.getUpdated());
        assertEquals(0, filteredListener.getRemoved());

        cache.remove(2L);
        assertEquals(2, filteredListener.getCreated());
        assertEquals(0, filteredListener.getUpdated());
        assertEquals(1, filteredListener.getRemoved());

        cache.replace(1L, "Fred");
        assertEquals(2, filteredListener.getCreated());
        assertEquals(1, filteredListener.getUpdated());
        assertEquals(1, filteredListener.getRemoved());

        cache.replace(3L, "Bryn", "Sooty");
        assertEquals(2, filteredListener.getCreated());
        assertEquals(2, filteredListener.getUpdated());
        assertEquals(1, filteredListener.getRemoved());

        cache.get(1L);
        assertEquals(2, filteredListener.getCreated());
        assertEquals(2, filteredListener.getUpdated());
        assertEquals(1, filteredListener.getRemoved());

        //containsKey is not a read for filteredListener purposes.
        cache.containsKey(1L);
        assertEquals(2, filteredListener.getCreated());
        assertEquals(2, filteredListener.getUpdated());
        assertEquals(1, filteredListener.getRemoved());

        //iterating should cause read events on non-expired entries
        for (Cache.Entry<Long, String> entry : cache) {
            String value = entry.getValue();
            logger.info(value);
        }
        assertEquals(2, filteredListener.getCreated());
        assertEquals(2, filteredListener.getUpdated());
        assertEquals(1, filteredListener.getRemoved());

        cache.getAndPut(1L, "Pistachio");
        assertEquals(2, filteredListener.getCreated());
        assertEquals(3, filteredListener.getUpdated());
        assertEquals(1, filteredListener.getRemoved());

        Set<Long> keys = new HashSet<Long>();
        keys.add(1L);
        cache.getAll(keys);
        assertEquals(2, filteredListener.getCreated());
        assertEquals(3, filteredListener.getUpdated());
        assertEquals(1, filteredListener.getRemoved());

        cache.getAndReplace(1L, "Prince");
        assertEquals(2, filteredListener.getCreated());
        assertEquals(4, filteredListener.getUpdated());
        assertEquals(1, filteredListener.getRemoved());

        cache.getAndRemove(1L);
        assertEquals(2, filteredListener.getCreated());
        assertEquals(4, filteredListener.getUpdated());
        assertEquals(2, filteredListener.getRemoved());

        assertEquals(2, filteredListener.getCreated());
        assertEquals(4, filteredListener.getUpdated());
        assertEquals(2, filteredListener.getRemoved());
    }

    private static class MyCacheEntryEventFilter implements CacheEntryEventFilter<Long, String>, Serializable {
        @Override
        public boolean evaluate(CacheEntryEvent<? extends Long, ? extends String> event) throws CacheEntryListenerException {
            return event.getValue() == null
                    || event.getValue().contains("a")
                    || event.getValue().contains("e")
                    || event.getValue().contains("i")
                    || event.getValue().contains("o")
                    || event.getValue().contains("u");
        }
    }
}
