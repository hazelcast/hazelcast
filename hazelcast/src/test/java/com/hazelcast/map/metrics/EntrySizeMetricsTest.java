/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.metrics;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.WanCustomPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.IMap;
import com.hazelcast.map.LocalMapStats;
import com.hazelcast.map.MapLoader;
import com.hazelcast.map.MapStoreAdapter;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.impl.WanDummyPublisher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;
import java.util.logging.LogRecord;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MAP_METRIC_LAST_OVERSIZED_ENTRY_SIZE;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MAP_METRIC_OVERSIZED_ENTRIES_SEEN;
import static com.hazelcast.map.metrics.MetricTestUtils.assertAttributeEquals;
import static com.hazelcast.map.metrics.MetricTestUtils.buildMapMetricName;
import static com.hazelcast.test.Accessors.getSerializationService;
import static org.assertj.core.api.Assertions.assertThat;

@ParallelJVMTest
@QuickTest
class EntrySizeMetricsTest
        extends HazelcastTestSupport {

    private static final String MAP_NAME = "testMap";
    private static final String WAN_REPLICATION_NAME = "testWan";
    private static final int THRESHOLD_BYTES = 1000;
    private static final int ENTRY_COUNT = 20;
    private static final byte[] SMALL_VALUE = new byte[10];
    private static final byte[] LARGE_VALUE = new byte[2 * THRESHOLD_BYTES];

    @ParameterizedTest
    @EnumSource(value = InMemoryFormat.class, names = {"BINARY", "OBJECT"})
    void testEntryProcessorWithWan_countsOversizedEntryOnce(InMemoryFormat inMemoryFormat) {
        HazelcastInstance hz = createHazelcastInstance(withWan(config(inMemoryFormat)));
        IMap<String, byte[]> map = hz.getMap(MAP_NAME);
        map.put("key", SMALL_VALUE);

        map.executeOnKey("key", entry -> entry.setValue(LARGE_VALUE));

        LocalMapStats stats = map.getLocalMapStats();
        assertThat(stats.getOversizedEntriesSeen()).isEqualTo(1);
        assertThat(stats.getLastOversizedEntrySize()).isEqualTo(entrySize(hz, "key", LARGE_VALUE));
    }

    // OBJECT write-behind without coalescing serializes stored values, so they are measured when stored
    @ParameterizedTest(name = "offload={0}")
    @ValueSource(booleans = {true, false})
    void testObjectEntryProcessorWithWanAndWriteBehind_countsOversizedEntryOnce(boolean offload) {
        Config config = withWan(config(OBJECT));
        config.getMapConfig(MAP_NAME).setMapStoreConfig(new MapStoreConfig()
                .setImplementation(new MapStoreAdapter<String, byte[]>())
                .setWriteDelaySeconds(1)
                .setWriteCoalescing(false)
                .setOffload(offload));
        HazelcastInstance hz = createHazelcastInstance(config);
        IMap<String, byte[]> map = hz.getMap(MAP_NAME);
        map.put("key", SMALL_VALUE);

        map.executeOnKey("key", entry -> entry.setValue(LARGE_VALUE));
        assertThat(map.getLocalMapStats().getOversizedEntriesSeen()).isEqualTo(1);

        map.executeOnKey("newKey", entry -> entry.setValue(LARGE_VALUE));
        LocalMapStats stats = map.getLocalMapStats();
        assertThat(stats.getOversizedEntriesSeen()).isEqualTo(2);
        assertThat(stats.getLastOversizedEntrySize()).isEqualTo(entrySize(hz, "newKey", LARGE_VALUE));
    }

    // Not a requirement but a limitation: OBJECT entry processor results are only
    // measured when already serialized, which without WAN they are not
    @Test
    void testObjectEntryProcessorWithoutWan_isNotMeasured() {
        HazelcastInstance hz = createHazelcastInstance(config(OBJECT));
        IMap<String, byte[]> map = hz.getMap(MAP_NAME);
        map.put("key", SMALL_VALUE);

        map.executeOnKey("key", entry -> entry.setValue(LARGE_VALUE));

        LocalMapStats stats = map.getLocalMapStats();
        assertThat(stats.getOversizedEntriesSeen()).isZero();
        assertThat(stats.getLastOversizedEntrySize()).isZero();
    }

    @ParameterizedTest
    @EnumSource(value = InMemoryFormat.class, names = {"BINARY", "OBJECT"})
    void testLoadAll_countsOversizedEntries(InMemoryFormat inMemoryFormat) {
        Config config = config(inMemoryFormat);
        config.getMapConfig(MAP_NAME).setMapStoreConfig(new MapStoreConfig()
                .setImplementation(new LargeValueLoader())
                .setInitialLoadMode(MapStoreConfig.InitialLoadMode.LAZY));
        HazelcastInstance hz = createHazelcastInstance(config);
        IMap<String, byte[]> map = hz.getMap(MAP_NAME);

        map.loadAll(Set.of("key1", "key2"), true);

        LocalMapStats stats = map.getLocalMapStats();
        assertThat(stats.getOversizedEntriesSeen()).isEqualTo(2);
        assertThat(stats.getLastOversizedEntrySize()).isEqualTo(entrySize(hz, "key1", LARGE_VALUE));
    }

    @Test
    void testReadThroughLoad_countsOversizedEntry() {
        Config config = config(BINARY);
        config.getMapConfig(MAP_NAME).setMapStoreConfig(new MapStoreConfig()
                .setImplementation(new LargeValueLoader())
                .setInitialLoadMode(MapStoreConfig.InitialLoadMode.LAZY));
        HazelcastInstance hz = createHazelcastInstance(config);
        IMap<String, byte[]> map = hz.getMap(MAP_NAME);

        map.get("notInMemory");

        assertThat(map.getLocalMapStats().getOversizedEntriesSeen()).isEqualTo(1);
    }

    @Test
    void testBackups_areNotReported() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        Config config = config(BINARY);
        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        putLargeEntries(hz1, ENTRY_COUNT);

        assertThat(oversizedEntriesSeen(hz1) + oversizedEntriesSeen(hz2)).isEqualTo(ENTRY_COUNT);
    }

    @ParameterizedTest
    @EnumSource(value = InMemoryFormat.class, names = {"BINARY", "OBJECT"})
    void testBackupPromotion_keepsStatistics(InMemoryFormat inMemoryFormat) {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        Config config = config(inMemoryFormat);
        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        putLargeEntries(hz1, ENTRY_COUNT);

        hz2.getLifecycleService().terminate();
        assertClusterSizeEventually(1, hz1);
        waitAllForSafeState(hz1);

        LocalMapStats stats = hz1.getMap(MAP_NAME).getLocalMapStats();
        assertThat(stats.getOversizedEntriesSeen()).isEqualTo(ENTRY_COUNT);
        assertThat(stats.getLastOversizedEntrySize()).isEqualTo(entrySize(hz1, "key10", LARGE_VALUE));
    }

    @Test
    void testMigration_movesStatistics() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        Config config = config(BINARY);
        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        putLargeEntries(hz1, ENTRY_COUNT);

        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        waitAllForSafeState(hz1, hz2);

        assertThat(oversizedEntriesSeen(hz1) + oversizedEntriesSeen(hz2)).isEqualTo(ENTRY_COUNT);
        assertThat(oversizedEntriesSeen(hz2)).isPositive();
        assertThat(hz2.getMap(MAP_NAME).getLocalMapStats().getLastOversizedEntrySize())
                .isIn(entrySize(hz2, "key0", LARGE_VALUE), entrySize(hz2, "key10", LARGE_VALUE));
    }

    @Test
    void testStatisticsDisabled_noTracking() {
        Config config = config(BINARY);
        config.getMapConfig(MAP_NAME).setStatisticsEnabled(false);
        HazelcastInstance hz = createHazelcastInstance(config);
        IMap<String, byte[]> map = hz.getMap(MAP_NAME);

        map.put("key", LARGE_VALUE);

        assertNoTracking(hz);
    }

    @Test
    void testDefaultThreshold_countsEntriesAboveOneMegabyte() {
        HazelcastInstance hz = createHazelcastInstance(smallInstanceConfig());
        IMap<String, byte[]> map = hz.getMap(MAP_NAME);
        byte[] oneMegabyte = new byte[(int) MapConfig.DEFAULT_OVERSIZED_ENTRY_THRESHOLD_BYTES];

        map.put("small", SMALL_VALUE);
        map.put("large", oneMegabyte);

        assertThat(map.getLocalMapStats().getOversizedEntriesSeen()).isEqualTo(1);
    }

    @Test
    void testThresholdDisabled_noTracking() {
        Config config = config(BINARY);
        config.getMapConfig(MAP_NAME).setOversizedEntryThresholdBytes(0);
        HazelcastInstance hz = createHazelcastInstance(config);
        IMap<String, byte[]> map = hz.getMap(MAP_NAME);

        map.put("key", LARGE_VALUE);

        assertNoTracking(hz);
    }

    @Test
    void testMetricsArePublished() {
        HazelcastInstance hz = createHazelcastInstance(MetricTestUtils.setRapidMetricsCollection(config(BINARY)));
        IMap<String, byte[]> map = hz.getMap(MAP_NAME);

        map.put("key", LARGE_VALUE);

        assertAttributeEquals(1L, buildMapMetricName(hz, MAP_NAME), MAP_METRIC_OVERSIZED_ENTRIES_SEEN);
        assertAttributeEquals(entrySize(hz, "key", LARGE_VALUE), buildMapMetricName(hz, MAP_NAME),
                MAP_METRIC_LAST_OVERSIZED_ENTRY_SIZE);
    }

    @Test
    void testWarning_loggedOnceWithMapAndSizes() {
        HazelcastInstance hz = createHazelcastInstance(config(BINARY));
        List<String> warnings = captureOversizedEntryWarnings(hz);
        IMap<String, byte[]> map = hz.getMap(MAP_NAME);

        map.put("key1", LARGE_VALUE);
        map.put("key2", LARGE_VALUE);

        assertThat(warnings).singleElement().asString()
                .contains("oversized entry of " + entrySize(hz, "key1", LARGE_VALUE) + " bytes")
                .contains("map '" + MAP_NAME + "'")
                .contains("threshold of " + THRESHOLD_BYTES + " bytes");
    }

    @Test
    void testWarning_notLoggedForSmallEntries() {
        HazelcastInstance hz = createHazelcastInstance(config(BINARY));
        List<String> warnings = captureOversizedEntryWarnings(hz);

        hz.getMap(MAP_NAME).put("key", SMALL_VALUE);

        assertThat(warnings).isEmpty();
    }

    @Test
    void testWarning_notLoggedForBackups() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        Config config = config(BINARY);
        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        List<String> warnings = captureOversizedEntryWarnings(hz1, hz2);

        hz1.getMap(MAP_NAME).put("key", LARGE_VALUE);

        assertThat(oversizedEntriesSeen(hz1) + oversizedEntriesSeen(hz2)).isEqualTo(1);
        assertThat(warnings).hasSize(1);
    }

    private static List<String> captureOversizedEntryWarnings(HazelcastInstance... instances) {
        List<String> warnings = new CopyOnWriteArrayList<>();
        for (HazelcastInstance instance : instances) {
            instance.getLoggingService().addLogListener(Level.WARNING, event -> {
                LogRecord logRecord = event.getLogRecord();
                if (logRecord.getLoggerName().endsWith("EntrySizeMutationObserver")) {
                    warnings.add(logRecord.getMessage());
                }
            });
        }
        return warnings;
    }

    private Config config(InMemoryFormat inMemoryFormat) {
        return smallInstanceConfig()
                .addMapConfig(new MapConfig(MAP_NAME).setInMemoryFormat(inMemoryFormat)
                        .setOversizedEntryThresholdBytes(THRESHOLD_BYTES));
    }

    private static void putLargeEntries(HazelcastInstance hz, int count) {
        IMap<String, byte[]> map = hz.getMap(MAP_NAME);
        for (int i = 0; i < count; i++) {
            map.put("key" + i, LARGE_VALUE);
        }
    }

    private static long oversizedEntriesSeen(HazelcastInstance hz) {
        return hz.getMap(MAP_NAME).getLocalMapStats().getOversizedEntriesSeen();
    }

    private static Config withWan(Config config) {
        config.addWanReplicationConfig(new WanReplicationConfig()
                .setName(WAN_REPLICATION_NAME)
                .addCustomPublisherConfig(new WanCustomPublisherConfig()
                        .setPublisherId("dummyPublisher")
                        .setClassName(WanDummyPublisher.class.getName())));
        config.getMapConfig(MAP_NAME).setWanReplicationRef(new WanReplicationRef()
                .setName(WAN_REPLICATION_NAME)
                .setMergePolicyClassName(PassThroughMergePolicy.class.getName()));
        return config;
    }

    private static long entrySize(HazelcastInstance hz, String key, byte[] value) {
        SerializationService serializationService = getSerializationService(hz);
        return (long) serializationService.toData(key).totalSize() + serializationService.toData(value).totalSize();
    }

    private static void assertNoTracking(HazelcastInstance hz) {
        LocalMapStats stats = hz.getMap(MAP_NAME).getLocalMapStats();
        assertThat(stats.getOversizedEntriesSeen()).isZero();
        assertThat(stats.getLastOversizedEntrySize()).isZero();
        assertThat(stats.getLastOversizedEntryTime()).isZero();
    }

    private static class LargeValueLoader implements MapLoader<String, byte[]> {

        @Override
        public byte[] load(String key) {
            return LARGE_VALUE;
        }

        @Override
        public Map<String, byte[]> loadAll(Collection<String> keys) {
            Map<String, byte[]> values = new HashMap<>();
            for (String key : keys) {
                values.put(key, LARGE_VALUE);
            }
            return values;
        }

        @Override
        public Iterable<String> loadAllKeys() {
            return null;
        }
    }
}
