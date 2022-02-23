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

package com.hazelcast.internal.management;

import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.monitor.impl.MemberStateImpl;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.cache.CacheUtil.getDistributedObjectName;
import static com.hazelcast.test.Accessors.getHazelcastInstanceImpl;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TimedMemberStateTest extends HazelcastTestSupport {

    private static final String CACHE_WITH_STATS_PREFIX = "cache-with-stats-";
    private static final String CACHE_WITHOUT_STATS_PREFIX = "other-cache-";

    private TimedMemberStateFactory timedMemberStateFactory;
    private TimedMemberState timedMemberState;
    private HazelcastInstance hz;

    @Before
    public void setUp() {
        Config config = smallInstanceConfig();
        config.addCacheConfig(new CacheSimpleConfig()
                                  .setName(CACHE_WITH_STATS_PREFIX + "*")
                                  .setStatisticsEnabled(true));
        config.addCacheConfig(new CacheSimpleConfig()
                                  .setName(CACHE_WITHOUT_STATS_PREFIX + "*"));
        hz = createHazelcastInstance(config);
        timedMemberStateFactory = new TimedMemberStateFactory(getHazelcastInstanceImpl(hz));
        timedMemberState = createState();
    }

    @After
    public void tearDown() {
        // explicit cleanup is required because the MBean server is static so registrations
        // will be left over when test's HazelcastInstance shuts down
        destroyAllDistributedObjects(hz);
    }

    @Test
    public void testClone() throws CloneNotSupportedException {
        TimedMemberState cloned = timedMemberState.clone();

        assertNotNull(cloned);
        assertEquals("ClusterName", cloned.getClusterName());
        assertEquals(1827731, cloned.getTime());
        assertNotNull(cloned.getMemberState());
        assertTrue(cloned.isSslEnabled());
        assertTrue(cloned.isLite());
        assertFalse(cloned.isScriptingEnabled());
        assertFalse(cloned.isConsoleEnabled());
        assertTrue(cloned.isMcDataAccessEnabled());
        assertNotNull(cloned.toString());
    }

    @Test
    public void testSerialization() {
        JsonObject serialized = timedMemberState.toJson();
        TimedMemberState deserialized = new TimedMemberState();
        deserialized.fromJson(serialized);

        assertNotNull(deserialized);
        assertEquals("ClusterName", deserialized.getClusterName());
        assertEquals(1827731, deserialized.getTime());
        assertNotNull(deserialized.getMemberState());
        assertTrue(deserialized.isSslEnabled());
        assertTrue(deserialized.isLite());
        assertFalse(deserialized.isScriptingEnabled());
        assertFalse(deserialized.isConsoleEnabled());
        assertTrue(deserialized.isMcDataAccessEnabled());
        assertNotNull(deserialized.toString());
    }

    @Test
    public void testReplicatedMapGetStats() {
        NodeEngineImpl nodeEngine = getNodeEngineImpl(hz);
        hz.getReplicatedMap("replicatedMap");
        ReplicatedMapService replicatedMapService = nodeEngine.getService(ReplicatedMapService.SERVICE_NAME);
        assertNotNull(replicatedMapService.getStats().get("replicatedMap"));
    }

    @Test
    public void testCacheGetStats() {
        NodeEngineImpl nodeEngine = getNodeEngineImpl(hz);
        hz.getCacheManager().getCache(CACHE_WITH_STATS_PREFIX + "1");
        CacheService cacheService = nodeEngine.getService(CacheService.SERVICE_NAME);
        assertNotNull(cacheService.getStats()
                          .get(getDistributedObjectName(CACHE_WITH_STATS_PREFIX + "1")));
    }

    @Test
    public void testOnlyCachesWithStatsEnabled_areReportedInTimedMemberState() {
        // create 100 caches with stats enabled
        for (int i = 0; i < 100; i++) {
            hz.getCacheManager().getCache(CACHE_WITH_STATS_PREFIX + i);
        }
        // create 50 caches with stats disabled
        for (int i = 0; i < 50; i++) {
            ICache cacheWithoutStats = hz.getCacheManager().getCache(CACHE_WITHOUT_STATS_PREFIX + i);
            // explicitly request local stats -> this registers an empty stats object in CacheService
            cacheWithoutStats.getLocalCacheStatistics();
        }

        MemberStateImpl memberState = createState().getMemberState();
        for (int i = 0; i < 100; i++) {
            assertContains(memberState.getCachesWithStats(), getDistributedObjectName(CACHE_WITH_STATS_PREFIX + i));
            assertNotContains(memberState.getCachesWithStats(), getDistributedObjectName(CACHE_WITHOUT_STATS_PREFIX + i));
        }
    }

    private TimedMemberState createState() {
        TimedMemberState state = timedMemberStateFactory.createTimedMemberState();
        state.setClusterName("ClusterName");
        state.setTime(1827731);
        state.setSslEnabled(true);
        state.setLite(true);
        state.setScriptingEnabled(false);
        return state;
    }
}
