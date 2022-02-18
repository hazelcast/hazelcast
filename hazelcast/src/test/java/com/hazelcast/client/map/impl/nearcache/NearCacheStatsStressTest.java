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

package com.hazelcast.client.map.impl.nearcache;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.IMap;
import com.hazelcast.nearcache.NearCacheStats;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.nearcache.NearCache.UpdateSemantic.READ_UPDATE;
import static com.hazelcast.internal.nearcache.NearCacheRecord.NOT_RESERVED;
import static com.hazelcast.internal.util.RandomPicker.getInt;
import static com.hazelcast.test.Accessors.getSerializationService;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class NearCacheStatsStressTest extends HazelcastTestSupport {

    private static final int KEY_SPACE = 1000;

    private final TestHazelcastFactory factory = new TestHazelcastFactory();
    private final AtomicBoolean stop = new AtomicBoolean(false);

    private InternalSerializationService ss;
    private NearCache<Object, Object> nearCache;

    @Before
    public void setUp() throws Exception {
        HazelcastInstance server = factory.newHazelcastInstance();
        ss = getSerializationService(server);

        String mapName = "test";

        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setName(mapName);
        nearCacheConfig.setInvalidateOnChange(true);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.addNearCacheConfig(nearCacheConfig);

        HazelcastInstance client = factory.newHazelcastClient(clientConfig);

        IMap map = client.getMap(mapName);
        nearCache = ((NearCachedClientMapProxy) map).getNearCache();
    }

    @After
    public void tearDown() throws Exception {
        factory.shutdownAll();
    }

    @Test
    public void stress_stats_by_doing_put_and_remove() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        pool.execute(new Put());
        pool.execute(new Remove());

        sleepSeconds(3);
        stop.set(true);

        pool.shutdown();
        if (pool.awaitTermination(10, SECONDS)) {
            NearCacheStats nearCacheStats = nearCache.getNearCacheStats();
            long ownedEntryCount = nearCacheStats.getOwnedEntryCount();
            long memoryCost = nearCacheStats.getOwnedEntryMemoryCost();
            int size = nearCache.size();

            assertTrue("ownedEntryCount=" + ownedEntryCount + ", size=" + size, ownedEntryCount >= 0);
            assertTrue("memoryCost=" + memoryCost + ", size=" + size, memoryCost >= 0);
            assertEquals("ownedEntryCount=" + ownedEntryCount + ", size=" + size, size, ownedEntryCount);
        } else {
            fail("pool.awaitTermination reached timeout before termination");
        }
    }

    class Put implements Runnable {
        @Override
        public void run() {
            while (!stop.get()) {
                Object key = getInt(KEY_SPACE);
                Data keyData = ss.toData(key);
                long reservationId = nearCache.tryReserveForUpdate(key, keyData, READ_UPDATE);
                if (reservationId != NOT_RESERVED) {
                    nearCache.tryPublishReserved(key, keyData, reservationId, false);
                }
            }
        }
    }

    class Remove implements Runnable {
        @Override
        public void run() {
            while (!stop.get()) {
                Object key = getInt(KEY_SPACE);
                nearCache.invalidate(key);
            }
        }
    }
}
