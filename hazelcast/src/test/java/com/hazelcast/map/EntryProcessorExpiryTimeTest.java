/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Offloadable;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.backup.MapBackupAccessor;
import com.hazelcast.test.backup.TestBackupUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.map.EntryProcessorExpiryTimeTest.RUN_METHOD.EXECUTE_ON_ENTRIES;
import static com.hazelcast.map.EntryProcessorExpiryTimeTest.RUN_METHOD.EXECUTE_ON_KEY;
import static com.hazelcast.map.EntryProcessorExpiryTimeTest.RUN_METHOD.EXECUTE_ON_KEYS;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EntryProcessorExpiryTimeTest extends HazelcastTestSupport {

    @Parameterized.Parameter
    public InMemoryFormat inMemoryFormat;

    @Parameterized.Parameter(1)
    public boolean forceOffload;

    @Parameterized.Parameters(name = "inMemoryFormat: {0}, forceOffload: {1}")
    public static Collection<Object[]> data() {
        return asList(new Object[][]{
                {BINARY, false},
                {OBJECT, false},
                {OBJECT, true}
        });
    }

    private final String mapName = "ep-test-map";

    @Override
    public Config getConfig() {
        Config config = smallInstanceConfig();
        config.getMetricsConfig().setEnabled(false);
        config.setProperty(MapServiceContext.FORCE_OFFLOAD_ALL_OPERATIONS.getName(),
                String.valueOf(forceOffload));
        config.getMapConfig(mapName)
                .setInMemoryFormat(inMemoryFormat)
                .setPerEntryStatsEnabled(true)
                .setTimeToLiveSeconds(100);
        return config;
    }

    @Test
    public void executeOnKey_sets_expiry_time_when_creating_new_entry() {
        test(Collections.singleton(1), false,
                new SetValueWithoutChangingExpiryTime<>(2), false, EXECUTE_ON_KEY);
    }

    @Test
    public void executeOnKeys_sets_expiry_time_when_creating_new_entries() {
        test(new HashSet<>(Arrays.asList(1, 2, 3, 4, 5, 6, 7)), false,
                new SetValueWithoutChangingExpiryTime<>(2), false, EXECUTE_ON_KEYS);
    }

    @Test
    public void executeOnKey_does_not_change_expiry_time_when_updating_entry() {
        test(Collections.singleton(1), true,
                new SetValueWithoutChangingExpiryTime<>(2), false, EXECUTE_ON_KEY);
    }

    @Test
    public void executeOnKeys_does_not_change_expiry_time_when_updating_entries() {
        test(new HashSet<>(Arrays.asList(1, 2, 3, 4, 5, 6, 7)), true,
                new SetValueWithoutChangingExpiryTime<>(2), false, EXECUTE_ON_KEYS);
    }

    @Test
    public void executeOnEntries_does_not_change_expiry_time_when_updating_entries() {
        test(new HashSet<>(Arrays.asList(1, 2, 3, 4, 5, 6, 7)), true,
                new SetValueWithoutChangingExpiryTime<>(2), false, EXECUTE_ON_ENTRIES);
    }

    @Test
    public void executeOnKey_sets_expiry_time_when_creating_new_entry_with_offloadable_EP() {
        test(Collections.singleton(1), false,
                new SetValueWithoutChangingExpiryTimeOffloadable<>(2), false, EXECUTE_ON_KEY);
    }

    @Test
    public void executeOnKeys_sets_expiry_time_when_creating_new_entries_with_offloadable_EP() {
        test(new HashSet<>(Arrays.asList(1, 2, 3, 4, 5, 6, 7)), false,
                new SetValueWithoutChangingExpiryTimeOffloadable<>(2), false, EXECUTE_ON_KEYS);
    }

    @Test
    public void executeOnKey_does_not_change_expiry_time_when_updating_entry_with_offloadable_EP() {
        test(Collections.singleton(1), true,
                new SetValueWithoutChangingExpiryTimeOffloadable<>(2), false, EXECUTE_ON_KEY);
    }

    @Test
    public void executeOnKeys_does_not_change_expiry_time_when_updating_entries_with_offloadable_EP() {
        test(new HashSet<>(Arrays.asList(1, 2, 3, 4, 5, 6, 7)), true,
                new SetValueWithoutChangingExpiryTimeOffloadable<>(2), false, EXECUTE_ON_KEYS);
    }

    @Test
    public void executeOnEntries_does_not_change_expiry_time_when_updating_entries_with_offloadable_EP() {
        test(new HashSet<>(Arrays.asList(1, 2, 3, 4, 5, 6, 7)), true,
                new SetValueWithoutChangingExpiryTimeOffloadable<>(2), false, EXECUTE_ON_ENTRIES);
    }

    @Test
    public void first_setValueWithoutChangingExpiryTime_then_setValue_shifts_expiry_time() {
        test(new HashSet<>(Arrays.asList(1, 2, 3, 4, 5, 6, 7)), true,
                new First_SetValueWithoutChangingExpiryTime_Then_SetValue<>(2), true, EXECUTE_ON_KEYS);
    }

    @Test
    public void first_setValue_then_setValueWithoutChangingExpiryTime_does_not_shift_expiry_time() {
        test(new HashSet<>(Arrays.asList(1, 2, 3, 4, 5, 6, 7)), true,
                new First_SetValue_Then_SetValueWithoutChangingExpiryTime(2), false, EXECUTE_ON_ENTRIES);
    }

    private void test(Set<Integer> keySet, boolean update,
                      EntryProcessor entryProcessor, boolean expectShiftExpiry, RUN_METHOD runMethod) {
        HazelcastInstance[] hazelcastInstances = createHazelcastInstances(getConfig(), 3);
        HazelcastInstance instance = hazelcastInstances[2];
        waitAllForSafeState(hazelcastInstances);
        warmUpPartitions(Arrays.asList(hazelcastInstances));

        MapBackupAccessor mapBackupAccessor = (MapBackupAccessor) TestBackupUtils
                .newMapAccessor(hazelcastInstances, mapName, 1);

        IMap<Integer, Integer> instance1Map = instance.getMap(mapName);
        Map<Integer, EntryView> cacheEntryViewPerKey1 = null;

        // when update is true, first create entries
        if (update) {
            for (Integer key : keySet) {
                instance1Map.set(key, 1, 100, TimeUnit.SECONDS);
            }
            cacheEntryViewPerKey1 = cacheEntryViewPerKey(instance1Map, keySet);

            sleepAtLeastSeconds(1);
        }

        switch (runMethod) {
            case EXECUTE_ON_KEY:
                Integer key = keySet.iterator().next();
                instance1Map.executeOnKey(key, entryProcessor);
                break;
            case EXECUTE_ON_KEYS:
                instance1Map.executeOnKeys(keySet, entryProcessor);
                break;
            case EXECUTE_ON_ENTRIES:
                instance1Map.executeOnEntries(entryProcessor);
                break;
            default:
                throw new UnsupportedOperationException("Not known run method: " + runMethod);
        }

        Map<Integer, EntryView> cacheEntryViewPerKey2 = cacheEntryViewPerKey(instance1Map, keySet);

        if (update) {
            for (Integer key : keySet) {
                long expirationTime1 = cacheEntryViewPerKey1.get(key).getExpirationTime();
                long expirationTime2 = cacheEntryViewPerKey2.get(key).getExpirationTime();
                long lastUpdateTime1 = cacheEntryViewPerKey1.get(key).getLastUpdateTime();
                long lastUpdateTime2 = cacheEntryViewPerKey2.get(key).getLastUpdateTime();

                if (!expectShiftExpiry) {
                    assertTrue(format("key: %d ==> lastUpdateTime1: %d, lastUpdateTime2: %d",
                            key, lastUpdateTime1, lastUpdateTime2), lastUpdateTime1 < lastUpdateTime2);
                    assertEquals(format("key: %d ==> expirationTime1: %d, expirationTime2: %d",
                            key, expirationTime1, expirationTime2), expirationTime1, expirationTime2);

                    assertTrueEventually(() -> {
                        long expirationTimeOnBackup1 = mapBackupAccessor.getExpiryTime(key);
                        assertEquals(format("key: %d ==> expirationTime1: %d, expirationTimeOnBackup1: %d",
                                key, expirationTime1, expirationTimeOnBackup1), expirationTime1, expirationTimeOnBackup1);
                    });
                } else {
                    assertTrue(format("key: %d ==> lastUpdateTime1: %d, lastUpdateTime2: %d",
                            key, lastUpdateTime1, lastUpdateTime2), lastUpdateTime1 < lastUpdateTime2);
                    assertTrue(format("key: %d ==> expirationTime1: %d, expirationTime2: %d",
                            key, expirationTime1, expirationTime2), expirationTime1 < expirationTime2);

                    assertTrueEventually(() -> {
                        long expirationTimeOnBackup1 = mapBackupAccessor.getExpiryTime(key);
                        assertTrue(format("key: %d ==> expirationTime1: %d, expirationTimeOnBackup1: %d",
                                key, expirationTime1, expirationTimeOnBackup1), expirationTime1 < expirationTimeOnBackup1);
                    });
                }
            }
        } else {
            for (Integer key : keySet) {
                long expirationTime1 = cacheEntryViewPerKey2.get(key).getExpirationTime();
                long lastUpdateTime1 = cacheEntryViewPerKey2.get(key).getLastUpdateTime();

                assertTrue(format("key: %d ==> lastUpdateTime1: %d", key, lastUpdateTime1), lastUpdateTime1 > 0);
                assertTrue(format("key: %d ==> expirationTime1: %d", key, expirationTime1), expirationTime1 > 0);

                assertTrueEventually(() -> {
                    long expirationTimeOnBackup1 = mapBackupAccessor.getExpiryTime(key);
                    assertTrue(format("key: %d ==> expirationTimeOnBackup1: %d", key, expirationTimeOnBackup1),
                            expirationTimeOnBackup1 > 0);
                });
            }
        }
    }

    private static Map<Integer, EntryView> cacheEntryViewPerKey(IMap<Integer, Integer> instance1Map,
                                                                Set<Integer> keySet) {
        Map<Integer, EntryView> entryViewHashMap = new HashMap<>();
        for (Integer key : keySet) {
            entryViewHashMap.put(key, instance1Map.getEntryView(key));
        }

        return entryViewHashMap;
    }

    private static class SetValueWithoutChangingExpiryTime<K, V>
            implements EntryProcessor<K, V, V> {

        private final V newValue;

        SetValueWithoutChangingExpiryTime(V newValue) {
            this.newValue = newValue;
        }

        @Override
        public V process(Map.Entry<K, V> entry) {
            return ((ExtendedMapEntry<K, V>) entry).setValueWithoutChangingExpiryTime(newValue);
        }
    }

    private static class SetValueWithoutChangingExpiryTimeOffloadable<V>
            extends SetValueWithoutChangingExpiryTime implements Offloadable {

        SetValueWithoutChangingExpiryTimeOffloadable(V newValue) {
            super(newValue);
        }

        @Override
        public String getExecutorName() {
            return OFFLOADABLE_EXECUTOR;
        }
    }

    private static class First_SetValueWithoutChangingExpiryTime_Then_SetValue<K, V>
            implements EntryProcessor<K, V, V> {

        private final V newValue;

        First_SetValueWithoutChangingExpiryTime_Then_SetValue(V newValue) {
            this.newValue = newValue;
        }

        @Override
        public V process(Map.Entry<K, V> entry) {
            ExtendedMapEntry<K, V> extendedMapEntry = (ExtendedMapEntry<K, V>) entry;
            V v = extendedMapEntry.setValueWithoutChangingExpiryTime(newValue);
            v = extendedMapEntry.setValue(newValue);
            return v;
        }
    }

    private static class First_SetValue_Then_SetValueWithoutChangingExpiryTime<K, V>
            implements EntryProcessor<K, V, V> {

        private final V newValue;

        First_SetValue_Then_SetValueWithoutChangingExpiryTime(V newValue) {
            this.newValue = newValue;
        }

        @Override
        public V process(Map.Entry<K, V> entry) {
            ExtendedMapEntry<K, V> extendedMapEntry = (ExtendedMapEntry<K, V>) entry;
            V v = extendedMapEntry.setValue(newValue);
            v = extendedMapEntry.setValueWithoutChangingExpiryTime(newValue);
            return v;
        }
    }

    enum RUN_METHOD {
        EXECUTE_ON_KEY,
        EXECUTE_ON_KEYS,
        EXECUTE_ON_ENTRIES,
    }
}
