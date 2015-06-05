/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.nearcache;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static java.lang.Runtime.getRuntime;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
public class NearCacheConcurrentInvalidationTest extends HazelcastTestSupport {
    private static final int numGetters = getRuntime().availableProcessors() - 1;
    private static final int maxRuntime = 10;
    private static final String mapName = "testMap" + NearCacheConcurrentInvalidationTest.class.getSimpleName();
    private static final String key = "key123";

    private ConcurrentMap<String, String> map;
    private HazelcastInstance hz;

    private final CountDownLatch problemDetectedLatch = new CountDownLatch(1);
    private volatile int lastPut;
    private volatile boolean keepGoing = true;

    @Before public void setUp() {
        final Config config = new Config();
        final NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setCacheLocalEntries(true); // needed because we test with single node
        nearCacheConfig.setInMemoryFormat(InMemoryFormat.OBJECT);
        final MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.setNearCacheConfig(nearCacheConfig);
        hz = createHazelcastInstance(config);
        map = hz.getMap(mapName);
    }

    // Reproduces https://github.com/hazelcast/hazelcast/issues/4671
    @Test
    public void putThenGet_withConcurrentGetting() throws Exception {
        workTheMap();
        assertCorrectness();
    }

    private void workTheMap() throws Exception {
        final ExecutorService threadPool = Executors.newFixedThreadPool(1 + numGetters);
        threadPool.submit(new Putter());
        for (int i = 0; i < numGetters; i++) {
            threadPool.submit(new Getter());
        }
        if (!problemDetectedLatch.await(maxRuntime, TimeUnit.SECONDS)) {
            System.out.println("Problem did not occur within " + maxRuntime + "s.");
        }
        keepGoing = false;
        threadPool.shutdown();
        threadPool.awaitTermination(10, TimeUnit.SECONDS);
    }

    private void assertCorrectness() throws Exception {
        final int expected = lastPut;
        final int actual = Integer.parseInt(map.get(key));
        if (actual < expected) {
            flushNearCache();
            final int actual2 = Integer.parseInt(map.get(key));
            fail(String.format(
                    "Near cache failed to become consistent: actual = %d, expected = %d." +
                    " Flushing the near cache %s: actual2 = %d.",
                    actual, expected,
                    actual2 < expected ? "didn't help" : "cleared the inconsistency",
                    actual2));
        }
    }

    private class Putter implements Runnable {
        @Override public void run() {
            for (int i = 0; keepGoing; i++) {
                map.put(key, String.valueOf(i));
                lastPut = i;
                int actual = Integer.parseInt(map.get(key));
                if (actual != i) {
                    System.err.format("Assertion failed: actual = %d, expected = %d\n", actual, i);
                    // sleep to ensure near cache invalidation is really lost
                    sleepMillis(100);
                    // test again and stop if really lost
                    actual = Integer.parseInt(map.get(key));
                    if (actual != i) {
                        System.err.format("Near cache invalidation lost: actual = %d, expected = %d\n", actual, i);
                        problemDetectedLatch.countDown();
                        break;
                    }
                }
            }
        }
    }

    private class Getter implements Runnable {
        @Override public void run() {
            while (keepGoing) {
                map.get(key);
                LockSupport.parkNanos(1);
            }
        }
    }

    private void flushNearCache() throws Exception {
        final MapService mapService = getNodeEngineImpl(hz).getService(MapService.SERVICE_NAME);
        mapService.getMapServiceContext().getNearCacheProvider().clearNearCache(mapName);
    }
}
