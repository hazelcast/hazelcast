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

package com.hazelcast.map.impl.nearcache.invalidation;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.nearcache.NearCacheRecordStore;
import com.hazelcast.internal.nearcache.impl.DefaultNearCache;
import com.hazelcast.internal.nearcache.impl.invalidation.MetaDataContainer;
import com.hazelcast.internal.nearcache.impl.invalidation.StaleReadDetector;
import com.hazelcast.internal.nearcache.impl.store.AbstractNearCacheRecordStore;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.proxy.NearCachedMapProxyImpl;
import com.hazelcast.nearcache.NearCacheStats;
import com.hazelcast.partition.Partition;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.internal.nearcache.impl.NearCacheTestUtils.getBaseConfig;
import static com.hazelcast.internal.nearcache.impl.invalidation.RepairingTask.MAX_TOLERATED_MISS_COUNT;
import static com.hazelcast.internal.nearcache.impl.invalidation.RepairingTask.MIN_RECONCILIATION_INTERVAL_SECONDS;
import static com.hazelcast.internal.nearcache.impl.invalidation.RepairingTask.RECONCILIATION_INTERVAL_SECONDS;
import static com.hazelcast.internal.nearcache.impl.invalidation.StaleReadDetector.ALWAYS_FRESH;
import static com.hazelcast.spi.properties.ClusterProperty.MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS;
import static com.hazelcast.spi.properties.ClusterProperty.MAP_INVALIDATION_MESSAGE_BATCH_SIZE;
import static java.lang.String.format;
import static java.lang.String.valueOf;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MemberMapReconciliationTest extends HazelcastTestSupport {

    private static final String MAP_NAME = "MemberMapReconciliationTest";
    private static final int RECONCILIATION_INTERVAL_SECS = 3;

    @Parameters(name = "mapInMemoryFormat:{0} nearCacheInMemoryFormat:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {BINARY, BINARY},
                {BINARY, OBJECT},

                {OBJECT, BINARY},
                {OBJECT, OBJECT},
        });
    }

    @Parameter
    public InMemoryFormat mapInMemoryFormat;

    @Parameter(1)
    public InMemoryFormat nearCacheInMemoryFormat;

    private final TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory();

    private Config config;

    private HazelcastInstance server;
    private IMap<Integer, Integer> serverMap;
    private IMap<Integer, Integer> nearCachedServerMap;

    @Before
    public void setUp() {
        NearCacheConfig nearCacheConfig = new NearCacheConfig(MAP_NAME)
                .setInMemoryFormat(nearCacheInMemoryFormat)
                .setInvalidateOnChange(true)
                .setCacheLocalEntries(true);

        MapConfig mapConfig = new MapConfig(MAP_NAME)
                .setInMemoryFormat(mapInMemoryFormat)
                .setNearCacheConfig(nearCacheConfig);

        // we want to test that reconciliation doesn't cause any premature
        // removal of entries by falsely assuming some entries as stale
        config = getConfig()
                .setProperty(MAX_TOLERATED_MISS_COUNT.getName(), "0")
                .setProperty(RECONCILIATION_INTERVAL_SECONDS.getName(), valueOf(RECONCILIATION_INTERVAL_SECS))
                .setProperty(MIN_RECONCILIATION_INTERVAL_SECONDS.getName(), valueOf(RECONCILIATION_INTERVAL_SECS))
                .setProperty(MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS.getName(), valueOf(Integer.MAX_VALUE))
                .setProperty(MAP_INVALIDATION_MESSAGE_BATCH_SIZE.getName(), valueOf(Integer.MAX_VALUE))
                .addMapConfig(mapConfig);

        server = factory.newHazelcastInstance(config);
        serverMap = server.getMap(MAP_NAME);
        nearCachedServerMap = nearCachedMapFromNewServer();
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void test_reconciliation_does_not_cause_premature_removal() {
        int total = 100;
        for (int i = 0; i < total; i++) {
            serverMap.put(i, i);
        }

        for (int i = 0; i < total; i++) {
            nearCachedServerMap.get(i);
        }

        final IMap<Integer, Integer> nearCachedMapFromNewServer = nearCachedMapFromNewServer();

        warmUpPartitions(factory.getAllHazelcastInstances());
        waitAllForSafeState(factory.getAllHazelcastInstances());
        waitForNearCacheInvalidationMetadata(nearCachedMapFromNewServer, server);

        for (int i = 0; i < total; i++) {
            nearCachedMapFromNewServer.get(i);
        }

        NearCacheStats nearCacheStats = nearCachedMapFromNewServer.getLocalMapStats().getNearCacheStats();
        assertStats(MAP_NAME, nearCacheStats, total, 0, total);

        sleepSeconds(2 * RECONCILIATION_INTERVAL_SECS);

        for (int i = 0; i < total; i++) {
            nearCachedMapFromNewServer.get(i);
        }

        assertStats(MAP_NAME, nearCacheStats, total, total, total);
    }

    @Override
    protected Config getConfig() {
        return getBaseConfig();
    }

    private IMap<Integer, Integer> nearCachedMapFromNewServer() {
        HazelcastInstance server = factory.newHazelcastInstance(config);
        IMap<Integer, Integer> map = server.getMap(MAP_NAME);

        assertInstanceOf(NearCachedMapProxyImpl.class, map);

        return map;
    }

    private void waitForNearCacheInvalidationMetadata(final IMap<Integer, Integer> nearCachedMapFromNewServer,
                                                      final HazelcastInstance server) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                final DefaultNearCache nearCache = getNearCache((NearCachedMapProxyImpl) nearCachedMapFromNewServer);

                NearCacheRecordStore nearCacheRecordStore = nearCache.getNearCacheRecordStore();
                StaleReadDetector staleReadDetector = getStaleReadDetector(nearCacheRecordStore);

                // we first assert that the stale detector is not the initial one, since the metadata that the records are
                // initialized with on putting records into the record store is queried from the stale detector
                assertNotSame(ALWAYS_FRESH, staleReadDetector);

                // wait until all partition's metadata is filled properly, since creating records from on initial metadata
                // may lead to stale reads if the metadata gets updated between record creation and stale read check
                for (Partition partition : server.getPartitionService().getPartitions()) {
                    MetaDataContainer metaDataContainer = staleReadDetector.getMetaDataContainer(partition.getPartitionId());

                    assertNotNull(metaDataContainer.getUuid());
                }
            }
        });
    }

    protected StaleReadDetector getStaleReadDetector(NearCacheRecordStore nearCacheRecordStore) {
        return ((AbstractNearCacheRecordStore) nearCacheRecordStore).getStaleReadDetector();
    }

    private static DefaultNearCache getNearCache(NearCachedMapProxyImpl nearCachedMapFromNewServer) {
        return (DefaultNearCache) nearCachedMapFromNewServer.getNearCache().unwrap(DefaultNearCache.class);
    }

    public static void assertStats(String name, NearCacheStats nearCacheStats,
                                   int ownedEntryCount, int expectedHits, int expectedMisses) {
        String msg = "NearCache for map `%s`, got wrong %s [%s]";
        assertEquals(format(msg, name, "ownedEntryCount", nearCacheStats),
                ownedEntryCount, nearCacheStats.getOwnedEntryCount());
        assertEquals(format(msg, name, "expectedHits", nearCacheStats),
                expectedHits, nearCacheStats.getHits());
        assertEquals(format(msg, name, "expectedMisses", nearCacheStats),
                expectedMisses, nearCacheStats.getMisses());
    }
}
