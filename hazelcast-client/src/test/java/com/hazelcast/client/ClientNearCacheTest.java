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

package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.nearcache.ClientNearCache;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.Repeat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.concurrent.Future;

import static com.hazelcast.test.HazelcastTestSupport.*;
import static org.junit.Assert.*;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ClientNearCacheTest {

    private static final int MAX_CACHE_SIZE = 100;
    private static final int MAX_TTL_SECONDS = 3;
    private static final int MAX_IDLE_SECONDS = 1;

    private static final String mapWithBasicCash = "mapWithBasicCash";
    private static final String mapWithMaxSizeCash = "mapWithMaxSizeCash";
    private static final String mapWithTTLCash = "mapWithTTLCash";
    private static final String mapWithIdleCash = "mapWithIdleCash";
    private static final String mapWithInvalidateCash = "mapWithInvalidateCash";

    private static HazelcastInstance h1;
    private static HazelcastInstance h2;
    private static HazelcastInstance client;

    @BeforeClass
    public static void setup() throws Exception {
        h1 = Hazelcast.newHazelcastInstance();
        h2 = Hazelcast.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();

        NearCacheConfig basicConfig = new NearCacheConfig();
        basicConfig.setName(mapWithBasicCash + "*");
        basicConfig.setInvalidateOnChange(false);
        basicConfig.setInMemoryFormat(InMemoryFormat.OBJECT);
        clientConfig.addNearCacheConfig(basicConfig);

        NearCacheConfig maxSizeConfig = new NearCacheConfig();
        maxSizeConfig.setName(mapWithMaxSizeCash + "*");
        maxSizeConfig.setInvalidateOnChange(false);
        maxSizeConfig.setMaxSize(MAX_CACHE_SIZE);
        clientConfig.addNearCacheConfig(maxSizeConfig);

        NearCacheConfig ttlConfig = new NearCacheConfig();
        ttlConfig.setName(mapWithTTLCash + "*");
        ttlConfig.setInvalidateOnChange(false);
        ttlConfig.setTimeToLiveSeconds(MAX_TTL_SECONDS);
        clientConfig.addNearCacheConfig(ttlConfig);

        NearCacheConfig idleConfig = new NearCacheConfig();
        idleConfig.setName(mapWithIdleCash + "*");
        idleConfig.setInvalidateOnChange(false);
        idleConfig.setMaxIdleSeconds(MAX_IDLE_SECONDS);
        clientConfig.addNearCacheConfig(idleConfig);

        NearCacheConfig invalidateConfig = new NearCacheConfig();
        invalidateConfig.setName(mapWithInvalidateCash + "*");
        invalidateConfig.setInvalidateOnChange(true);
        clientConfig.addNearCacheConfig(invalidateConfig);

        client = HazelcastClient.newHazelcastClient(clientConfig);
    }

    @AfterClass
    public static void cleanup() throws Exception {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testNearCacheFasterThanGoingToTheCluster() {
        final IMap map = client.getMap(mapWithBasicCash + randomString());

        final int size = 2007;
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }

        long begin = System.currentTimeMillis();
        for (int i = 0; i < size; i++) {
            map.get(i);
        }
        long readFromClusterTime = System.currentTimeMillis() - begin;

        begin = System.currentTimeMillis();
        for (int i = 0; i < size; i++) {
            map.get(i);
        }
        long readFromCacheTime = System.currentTimeMillis() - begin;

        assertTrue("readFromCacheTime > readFromClusterTime", readFromCacheTime < readFromClusterTime);
    }

    @Test
    public void testGetAllChecksNearCacheFirst() throws Exception {
        final IMap map = client.getMap(mapWithBasicCash + randomString());
        final HashSet keys = new HashSet();

        final int size = 1003;
        for (int i = 0; i < size; i++) {
            map.put(i, i);
            keys.add(i);
        }
        //populate near cache
        for (int i = 0; i < size; i++) {
            map.get(i);
        }
        //getAll generates the near cache hits
        map.getAll(keys);

        NearCacheStats stats = map.getLocalMapStats().getNearCacheStats();
        assertEquals(size, stats.getOwnedEntryCount());
        assertEquals(size, stats.getHits());
    }

    @Test
    public void testGetAllPopulatesNearCache() throws Exception {
        final IMap map = client.getMap(mapWithBasicCash + randomString());
        final HashSet keys = new HashSet();

        final int size = 1214;
        for (int i = 0; i < size; i++) {
            map.put(i, i);
            keys.add(i);
        }
        //getAll populates near cache
        map.getAll(keys);

        NearCacheStats stats = map.getLocalMapStats().getNearCacheStats();
        assertEquals(size, stats.getOwnedEntryCount());
    }

    @Test
    public void testGetAsync() throws Exception {
        final IMap map = client.getMap(mapWithBasicCash + randomString());

        int size = 1009;
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }
        //populate near cache
        for (int i = 0; i < size; i++) {
            map.get(i);
        }
        //generate near cache hits with async call
        for (int i = 0; i < size; i++) {
            Future async = map.getAsync(i);
            async.get();
        }
        NearCacheStats stats = map.getLocalMapStats().getNearCacheStats();
        assertEquals(size, stats.getOwnedEntryCount());
        assertEquals(size, stats.getHits());
    }

    @Test
    public void testGetAsyncPopulatesNearCache() throws Exception {
        final IMap map = client.getMap(mapWithBasicCash + randomString());

        int size = 1239;
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }
        //populate near cache
        for (int i = 0; i < size; i++) {
            Future async = map.getAsync(i);
            async.get();
        }
        //generate near cache hits
        for (int i = 0; i < size; i++) {
            map.get(i);
        }
        NearCacheStats stats = map.getLocalMapStats().getNearCacheStats();
        assertEquals(size, stats.getOwnedEntryCount());
    }

    @Test
    public void testNearCachePopulatedAndHitsGenerated() throws Exception {
        final IMap map = client.getMap(mapWithBasicCash + randomString());

        final int size = 1278;
        for (int i = 0; i < size; i++) {
            map.put(i, i);
            map.get(i);  //populate near cache
            map.get(i);  //generate near cache hits
        }

        NearCacheStats stats = map.getLocalMapStats().getNearCacheStats();
        System.out.println("stats = " + stats);
        assertEquals(size, stats.getOwnedEntryCount());
        assertEquals(size, stats.getHits());
    }

    @Test
    public void testNearCachePopulatedAndHitsGenerated2() throws Exception {
        final IMap map = client.getMap(mapWithBasicCash + randomString());

        final int size = 1278;
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }
        for (int i = 0; i < size; i++) {
            map.get(i);  //populate near cache
        }
        for (int i = 0; i < size; i++) {
            map.get(i);  //generate near cache hits
        }

        NearCacheStats stats = map.getLocalMapStats().getNearCacheStats();
        System.out.println("stats = " + stats);
        assertEquals(size, stats.getOwnedEntryCount());
        assertEquals(size, stats.getHits());
    }

    @Test
    public void testIssue2009() throws Exception {
        final IMap map = client.getMap(mapWithBasicCash + randomString());
        NearCacheStats stats = map.getLocalMapStats().getNearCacheStats();
        assertNotNull(stats);
    }

    @Test
    public void testGetNearCacheStatsBeforePopulation() {
        final IMap map = client.getMap(mapWithBasicCash + randomString());
        final int size = 101;
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }
        final NearCacheStats stats = map.getLocalMapStats().getNearCacheStats();
        assertNotNull(stats);
    }

    @Test
    public void testNearCacheMisses() {
        final IMap map = client.getMap(mapWithBasicCash + randomString());

        final int size = 1321;
        for (int i = 0; i < size; i++) {
            map.get("NotThere" + i);
        }
        NearCacheStats stats = map.getLocalMapStats().getNearCacheStats();
        assertEquals(size, stats.getMisses());
        assertEquals(size, stats.getOwnedEntryCount());
    }

    @Test
    public void testMapRemove_WithNearCache() {
        final IMap map = client.getMap(mapWithInvalidateCash + randomString());

        final int size = 1113;
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }
        for (int i = 0; i < size; i++) {
            map.get(i);
        }
        for (int i = 0; i < size; i++) {
            map.remove(i);
        }

        NearCacheStats stats = map.getLocalMapStats().getNearCacheStats();
        assertEquals(size, stats.getMisses());
        assertEquals(0, stats.getOwnedEntryCount());
    }

    @Test
    public void testNearCacheMaxSize() {
        final IMap map = client.getMap(mapWithMaxSizeCash + randomString());

        for (int i = 0; i < MAX_CACHE_SIZE + 1; i++) {
            map.put(i, i);
        }
        //populate near cache
        for (int i = 0; i < MAX_CACHE_SIZE + 1; i++) {
            map.get(i);
        }

        final int evictionSize = (int) (MAX_CACHE_SIZE * (ClientNearCache.EVICTION_PERCENTAGE / 100.0));
        final int remainingSize = MAX_CACHE_SIZE - evictionSize;

        HazelcastTestSupport.assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                final NearCacheStats stats = map.getLocalMapStats().getNearCacheStats();
                assertEquals(remainingSize, stats.getOwnedEntryCount());
            }
        });
    }

    @Test
    public void testNearCacheTTLCleanup() {
        final IMap map = client.getMap(mapWithTTLCash + randomString());

        final int size = 133;
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }
        //populate near cache
        for (int i = 0; i < size; i++) {
            map.get(i);
        }

        sleepSeconds(ClientNearCache.TTL_CLEANUP_INTERVAL_MILLS / 1000);
        map.get(0);

        final int expectedSize = 1;
        HazelcastTestSupport.assertTrueEventually(new AssertTask() {
            public void run() throws Exception {
                final NearCacheStats stats = map.getLocalMapStats().getNearCacheStats();
                assertEquals(expectedSize, stats.getOwnedEntryCount());
            }
        });
    }

    @Test
    public void testNearCacheIdleRecordsEvicted() {
        final IMap map = client.getMap(mapWithIdleCash + randomString());

        final int size = 147;
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }
        //populate near cache
        for (int i = 0; i < size; i++) {
            map.get(i);
        }
        //generate near cache hits
        for (int i = 0; i < size; i++) {
            map.get(i);
        }

        NearCacheStats stats = map.getLocalMapStats().getNearCacheStats();
        long hitsBeforeIdleExpire = stats.getHits();

        sleepSeconds(MAX_IDLE_SECONDS + 1);

        for (int i = 0; i < size; i++) {
            map.get(i);
        }
        stats = map.getLocalMapStats().getNearCacheStats();

        assertEquals("as the hits are not equal, the entries were not cleared from near cash after MaxIdleSeconds", hitsBeforeIdleExpire, stats.getHits(), size);
    }

    @Test
    public void testNearCacheInvalidateOnChange() {
        final String mapName = mapWithInvalidateCash + randomString();
        final IMap nodeMap = h1.getMap(mapName);
        final IMap clientMap = client.getMap(mapName);

        final int size = 118;
        for (int i = 0; i < size; i++) {
            nodeMap.put(i, i);
        }
        //populate near cache
        for (int i = 0; i < size; i++) {
            clientMap.get(i);
        }

        NearCacheStats stats = clientMap.getLocalMapStats().getNearCacheStats();
        long OwnedEntryCountBeforeInvalidate = stats.getOwnedEntryCount();

        //invalidate near cache from cluster
        for (int i = 0; i < size; i++) {
            nodeMap.put(i, i);
        }

        assertEquals(size, OwnedEntryCountBeforeInvalidate);

        assertTrueEventually(new AssertTask() {
            public void run() throws Exception {
                NearCacheStats stats = clientMap.getLocalMapStats().getNearCacheStats();
                assertEquals(0, stats.getOwnedEntryCount());
            }
        });
    }

    @Test
    public void testNearCacheContainsKey_afterRemove() {
        final IMap map = client.getMap(mapWithInvalidateCash + randomString());
        final Object key = "key";

        map.put(key, "value");
        map.get(key);
        map.remove(key);

        assertFalse(map.containsKey(key));
    }
}