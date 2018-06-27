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

package com.hazelcast.client.cache.impl.nearcache;

import com.hazelcast.client.cache.impl.HazelcastClientCacheManager;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.client.cache.impl.NearCachedClientCacheProxy;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.spi.CachingProvider;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.internal.nearcache.NearCacheTestUtils.getBaseConfig;
import static com.hazelcast.internal.nearcache.impl.invalidation.RepairingTask.MAX_TOLERATED_MISS_COUNT;
import static com.hazelcast.spi.properties.GroupProperty.CACHE_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS;
import static java.lang.Integer.parseInt;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests that the Near Cache doesn't lose invalidations.
 * <p>
 * Issue: https://github.com/hazelcast/hazelcast/issues/4671
 * <p>
 * Thanks Lukas Blunschi for the original test (https://github.com/lukasblu).
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({SlowTest.class, ParallelTest.class})
public class ClientCacheNearCacheStaleReadTest extends HazelcastTestSupport {

    private static final int NUM_GETTERS = 7;
    private static final int MAX_RUNTIME = 30;
    private static final String KEY = "key123";

    private static final ILogger LOGGER = Logger.getLogger(ClientCacheNearCacheStaleReadTest.class);

    private AtomicInteger valuePut = new AtomicInteger(0);
    private AtomicBoolean stop = new AtomicBoolean(false);
    private AtomicInteger assertionViolationCount = new AtomicInteger(0);
    private AtomicBoolean failed = new AtomicBoolean(false);

    private HazelcastInstance member;
    private HazelcastInstance client;
    private Cache<String, String> cache;

    @Before
    public void setUp() {
        String cacheName = randomMapName();
        TestHazelcastFactory factory = new TestHazelcastFactory();

        Config config = getConfig()
                .setProperty(CACHE_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS.getName(), "2");

        ClientConfig clientConfig = getClientConfig(cacheName)
                .setProperty(MAX_TOLERATED_MISS_COUNT.getName(), "0");

        member = factory.newHazelcastInstance(config);
        client = factory.newHazelcastClient(clientConfig);

        CachingProvider provider = HazelcastClientCachingProvider.createCachingProvider(client);
        HazelcastClientCacheManager cacheManager = (HazelcastClientCacheManager) provider.getCacheManager();

        CacheConfig<String, String> cacheConfig = createCacheConfig(cacheName);
        cache = cacheManager.createCache(cacheName, cacheConfig);
    }

    @After
    public void tearDown() {
        client.shutdown();
        member.shutdown();
    }

    @Test
    public void testNoLostInvalidationsEventually() {
        testNoLostInvalidationsStrict(false);
    }

    @Test
    public void testNoLostInvalidationsStrict() {
        testNoLostInvalidationsStrict(true);
    }

    private void testNoLostInvalidationsStrict(boolean strict) {
        // run test
        runTestInternal();

        if (!strict) {
            // test eventually consistent
            sleepSeconds(2);
        }
        int valuePutLast = valuePut.get();
        String valueMapStr = cache.get(KEY);
        int valueMap = parseInt(valueMapStr);

        // fail if not eventually consistent
        String msg = null;
        if (valueMap < valuePutLast) {
            msg = "Near Cache did *not* become consistent. (valueMap = " + valueMap + ", valuePut = " + valuePutLast + ").";

            // flush Near Cache and re-fetch value
            flushClientNearCache(cache);
            String valueMap2Str = cache.get(KEY);
            int valueMap2 = parseInt(valueMap2Str);

            // test again
            if (valueMap2 < valuePutLast) {
                msg += " Unexpected inconsistency! (valueMap2 = " + valueMap2 + ", valuePut = " + valuePutLast + ").";
            } else {
                msg += " Flushing the Near Cache cleared the inconsistency. (valueMap2 = " + valueMap2
                        + ", valuePut = " + valuePutLast + ").";
            }
        }

        // stop instance
        client.getLifecycleService().terminate();

        // fail after stopping instance
        if (msg != null) {
            LOGGER.warning(msg);
            fail(msg);
        }

        // fail if strict is required and assertion was violated
        if (strict && assertionViolationCount.get() > 0) {
            msg = "Assertion violated " + assertionViolationCount.get() + " times.";
            LOGGER.warning(msg);
            fail(msg);
        }
    }

    protected CacheConfig<String, String> createCacheConfig(String cacheName) {
        return new CacheConfig<String, String>()
                .setName(cacheName)
                .setInMemoryFormat(InMemoryFormat.BINARY);
    }

    @Override
    protected Config getConfig() {
        return getBaseConfig();
    }

    protected ClientConfig getClientConfig(String cacheName) {
        NearCacheConfig nearCacheConfig = getNearCacheConfig(cacheName);

        return new ClientConfig()
                .addNearCacheConfig(nearCacheConfig);
    }

    protected NearCacheConfig getNearCacheConfig(String cacheName) {
        return new NearCacheConfig(cacheName)
                .setInMemoryFormat(InMemoryFormat.BINARY)
                .setLocalUpdatePolicy(LocalUpdatePolicy.INVALIDATE);
    }

    /**
     * Flush Near Cache from client map with Near Cache.
     * <p>
     * Warning: this uses Hazelcast internals which might change from one version to the other.
     */
    private void flushClientNearCache(Cache cache) {
        ((NearCachedClientCacheProxy) cache).getNearCache().clear();
    }

    private void runTestInternal() {
        // start 1 putter thread (put0)
        Thread threadPut = new Thread(new PutRunnable(), "put0");
        threadPut.start();

        // wait for putter thread to start before starting getter threads
        sleepMillis(300);

        // start numGetters getter threads (get0-numGetters)
        List<Thread> threads = new ArrayList<Thread>();
        for (int i = 0; i < NUM_GETTERS; i++) {
            Thread thread = new Thread(new GetRunnable(), "get" + i);
            threads.add(thread);
        }
        for (Thread thread : threads) {
            thread.start();
        }

        // stop after maxRuntime seconds
        int i = 0;
        while (!stop.get() && i++ < MAX_RUNTIME) {
            sleepMillis(1000);
        }
        if (!stop.get()) {
            LOGGER.info("Problem did not occur within " + MAX_RUNTIME + "s.");
        }
        stop.set(true);
        assertJoinable(threadPut);
        for (Thread thread : threads) {
            assertJoinable(thread);
        }
    }

    private class PutRunnable implements Runnable {

        @Override
        public void run() {
            LOGGER.info(Thread.currentThread().getName() + " started.");
            int i = 0;
            while (!stop.get()) {
                i++;

                // put new value and update last state
                // note: the value in the map/Near Cache is *always* larger or equal to valuePut
                // assertion: valueMap >= valuePut
                cache.put(KEY, String.valueOf(i));
                valuePut.set(i);

                // check if we see our last update
                String valueMapStr = cache.get(KEY);
                if (valueMapStr == null) {
                    continue;
                }
                int valueMap = parseInt(valueMapStr);
                if (valueMap != i) {
                    assertionViolationCount.incrementAndGet();
                    LOGGER.warning("Assertion violated! (valueMap = " + valueMap + ", i = " + i + ")");

                    // sleep to ensure Near Cache invalidation is really lost
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        LOGGER.warning("Interrupted: " + e.getMessage());
                    }

                    // test again and stop if really lost
                    valueMapStr = cache.get(KEY);
                    valueMap = parseInt(valueMapStr);
                    if (valueMap != i) {
                        LOGGER.warning("Near Cache invalidation lost! (valueMap = " + valueMap + ", i = " + i + ")");
                        failed.set(true);
                        stop.set(true);
                    }
                }
            }
            LOGGER.info(Thread.currentThread().getName() + " performed " + i + " operations.");
        }
    }

    private class GetRunnable implements Runnable {

        @Override
        public void run() {
            LOGGER.info(Thread.currentThread().getName() + " started.");
            int n = 0;
            while (!stop.get()) {
                n++;

                // blindly get the value (to trigger the issue) and parse the value (to get some CPU load)
                String valueMapStr = cache.get(KEY);
                int i = parseInt(valueMapStr);
                assertEquals("" + i, valueMapStr);
            }
            LOGGER.info(Thread.currentThread().getName() + " performed " + n + " operations.");
        }
    }
}
