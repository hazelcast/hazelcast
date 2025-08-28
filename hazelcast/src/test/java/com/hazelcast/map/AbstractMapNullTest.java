/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.aggregation.impl.CountAggregator;
import com.hazelcast.config.IndexType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.ConsumerEx;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.projection.Projections;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastTestSupport;
import org.assertj.core.api.JUnitSoftAssertions;
import org.junit.Rule;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public abstract class AbstractMapNullTest extends HazelcastTestSupport {

    protected static final String MAP_NAME = "map";
    private Class<? extends Exception> expectedExceptionClass = NullPointerException.class;

    @Rule
    public final JUnitSoftAssertions softly = new JUnitSoftAssertions();

    @Test
    public void testNullability() {
        testNullability(null, null);
    }

    @Test
    public void testNullabilityEmptyHeapData() {
        // NPE would be also fine, it may be thrown when more validation is added to proxies - HZG-446
        expectedExceptionClass = IllegalArgumentException.class;
        testNullability(new HeapData(), new HeapData());
    }

    @Test
    public void testNullabilityNullHeapData() {
        // NPE would be also fine, it may be thrown when more validation is added to proxies - HZG-446
        expectedExceptionClass = IllegalArgumentException.class;
        for (int i = 0; i < 100; i++) {
            testNullability(new HeapData(new byte[HeapData.HEAP_DATA_OVERHEAD + i]),
                    new HeapData(new byte[HeapData.HEAP_DATA_OVERHEAD + i]));
            softly.assertAll();
        }
    }

    protected void testNullability(@Nullable Object key, @Nullable Object value) {
        EntryAddedListener<Object, Object> sampleMapListener = event -> {
        };
        Predicate<Object, Object> samplePredicate = Predicates.alwaysTrue();
        TimeUnit sampleTimeUnit = TimeUnit.SECONDS;

        assertThrowsNPE(m -> m.putAll(null));
        assertThrowsNPE(m -> m.putAll(mapOf(key, "")));
        assertThrowsNPE(m -> m.putAll(mapOf("", value)));
        assertThrowsNPE(m -> m.putAllAsync(null));
        assertThrowsNPE(m -> m.putAllAsync(mapOf(key, "")));
        assertThrowsNPE(m -> m.putAllAsync(mapOf("", value)));
        assertThrowsNPE(m -> m.containsKey(key));
        assertThrowsNPE(m -> m.containsValue(value));
        assertThrowsNPE(m -> m.get(key));
        assertThrowsNPE(m -> m.put(key, ""));
        assertThrowsNPE(m -> m.put("", value));
        assertThrowsNPE(m -> m.remove(key));
        assertThrowsNPE(m -> m.remove(key, ""));
        assertThrowsNPE(m -> m.remove("", value));
        assertThrowsNPE(m -> m.removeAll(null));
        assertThrowsNPE(m -> m.delete(key));
        assertThrowsNPE(m -> m.loadAll(null, true));
        assertThrowsNPE(m -> m.loadAll(setOf(key), true));
        assertThrowsNPE(m -> m.getAsync(key));

        assertThrowsNPE(m -> m.putAsync(key, ""));
        assertThrowsNPE(m -> m.putAsync("", value));

        assertThrowsNPE(m -> m.putAsync(key, "", -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.putAsync("", value, -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.putAsync("", "", -1, null));
        assertThrowsNPE(m -> m.putAsync(key, "", -1, sampleTimeUnit, -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.putAsync("", value, -1, sampleTimeUnit, -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.putAsync("", "", -1, null, -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.putAsync("", "", -1, sampleTimeUnit, -1, null));

        assertThrowsNPE(m -> m.setAsync(key, ""));
        assertThrowsNPE(m -> m.setAsync("", value));
        assertThrowsNPE(m -> m.setAsync(key, "", -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.setAsync("", value, -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.setAsync("", "", -1, null));
        assertThrowsNPE(m -> m.setAsync(key, "", -1, sampleTimeUnit, -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.setAsync("", value, -1, sampleTimeUnit, -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.setAsync("", "", -1, null, -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.setAsync("", "", -1, sampleTimeUnit, -1, null));

        assertThrowsNPE(m -> m.tryRemove(key, -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.tryRemove("", -1, null));

        assertThrowsNPE(m -> m.tryPut(key, "", -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.tryPut("", value, -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.tryPut("", "", -1, null));

        assertThrowsNPE(m -> m.put(key, "", -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.put("", value, -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.put("", "", -1, null));

        assertThrowsNPE(m -> m.putTransient(key, "", -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.putTransient("", value, -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.putTransient("", "", -1, null));
        assertThrowsNPE(m -> m.putTransient(key, "", -1, sampleTimeUnit, -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.putTransient("", value, -1, sampleTimeUnit, -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.putTransient("", "", -1, null, -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.putTransient("", "", -1, sampleTimeUnit, -1, null));

        assertThrowsNPE(m -> m.putIfAbsent(key, ""));
        assertThrowsNPE(m -> m.putIfAbsent("", value));
        assertThrowsNPE(m -> m.putIfAbsent(key, "", -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.putIfAbsent("", value, -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.putIfAbsent("", "", -1, null));
        assertThrowsNPE(m -> m.putIfAbsent(key, "", -1, sampleTimeUnit, -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.putIfAbsent("", value, -1, sampleTimeUnit, -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.putIfAbsent("", "", -1, null, -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.putIfAbsent("", "", -1, sampleTimeUnit, -1, null));

        assertThrowsNPE(m -> m.replace(key, "", ""));
        assertThrowsNPE(m -> m.replace("", value, ""));
        assertThrowsNPE(m -> m.replace("", "", value));
        assertThrowsNPE(m -> m.replace(key, ""));
        assertThrowsNPE(m -> m.replace("", value));

        assertThrowsNPE(m -> m.set(key, ""));
        assertThrowsNPE(m -> m.set("", value));
        assertThrowsNPE(m -> m.set(key, "", -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.set("", value, -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.set("", "", -1, null));
        assertThrowsNPE(m -> m.set(key, "", -1, sampleTimeUnit, -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.set("", value, -1, sampleTimeUnit, -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.set("", "", -1, null, -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.set("", "", -1, sampleTimeUnit, -1, null));

        assertThrowsNPE(m -> m.setAll(null));
        assertThrowsNPE(m -> m.setAll(mapOf(key, "")));
        assertThrowsNPE(m -> m.setAll(mapOf("", value)));
        assertThrowsNPE(m -> m.setAllAsync(null));
        assertThrowsNPE(m -> m.setAllAsync(mapOf(key, "")));
        assertThrowsNPE(m -> m.setAllAsync(mapOf("", value)));

        assertThrowsNPE(m -> m.lock(key));
        assertThrowsNPE(m -> m.lock(key, -1, sampleTimeUnit));

        assertThrowsNPE(m -> m.tryLock(key));
        assertThrowsNPE(m -> m.tryLock(key, -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.tryLock(key, -1, sampleTimeUnit, -1, sampleTimeUnit));

        assertThrowsNPE(m -> m.unlock(key));
        assertThrowsNPE(m -> m.forceUnlock(key));
        if (isNotClient()) {
            assertThrowsNPE(m -> m.addLocalEntryListener(null));
            assertThrowsNPE(m -> m.addLocalEntryListener(null, samplePredicate, true));
            assertThrowsNPE(m -> m.addLocalEntryListener(sampleMapListener, null, true));
            assertThrowsNPE(m -> m.addLocalEntryListener(sampleMapListener, null, "", true));
            assertThrowsNPE(m -> m.localKeySet(null));
        }

        assertThrowsNPE(m -> m.addInterceptor(null));
        assertThrowsNPE(m -> m.removeInterceptor(null));
        assertThrowsNPE(m -> m.addEntryListener(null, "", true));
        assertThrowsNPE(m -> m.addEntryListener(null, (Object) null, true));
        assertThrowsNPE(m -> m.addEntryListener(null, samplePredicate, true));
        assertThrowsNPE(m -> m.addEntryListener(sampleMapListener, null, true));
        assertThrowsNPE(m -> m.addEntryListener(null, samplePredicate, "", true));
        assertThrowsNPE(m -> m.addEntryListener(sampleMapListener, null, "", true));
        assertThrowsNPE(m -> m.removeEntryListener(null));
        assertThrowsNPE(m -> m.addPartitionLostListener(null));
        assertThrowsNPE(m -> m.removePartitionLostListener(null));
        assertThrowsNPE(m -> m.getEntryView(key));
        assertThrowsNPE(m -> m.evict(key));
        assertThrowsNPE(m -> m.keySet(null));
        assertThrowsNPE(m -> m.entrySet(null));
        assertThrowsNPE(m -> m.values(null));
        assertThrows(NullPointerException.class, m -> m.addIndex(null, "attribute"));
        assertThrows(NullPointerException.class, m -> m.addIndex(IndexType.SORTED, (String[]) null));
        assertThrows(NullPointerException.class, m -> m.addIndex(null));
        assertThrowsNPE(m -> m.aggregate(null));
        assertThrowsNPE(m -> m.aggregate(null, samplePredicate));
        assertThrowsNPE(m -> m.aggregate(new CountAggregator<>(), null));
        assertThrowsNPE(m -> m.project(null, samplePredicate));
        assertThrowsNPE(m -> m.project(Projections.identity(), null));
        assertThrowsNPE(m -> m.getQueryCache(null));
        assertThrowsNPE(m -> m.getQueryCache(null, samplePredicate, true));
        assertThrowsNPE(m -> m.getQueryCache("cache", null, true));
        assertThrowsNPE(m -> m.getQueryCache(null, sampleMapListener, samplePredicate, true));
        assertThrowsNPE(m -> m.getQueryCache("cache", null, samplePredicate, true));
        assertThrowsNPE(m -> m.getQueryCache("cache", sampleMapListener, null, true));
        assertThrowsNPE(m -> m.setTtl(key, -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.setTtl("", -1, null));
    }

    private Set<Object> setOf(@Nullable Object key) {
        var set = new HashSet<>();
        set.add(key);
        return set;
    }

    private static HashMap<Object, Object> mapOf(@Nullable Object key, @Nullable Object value) {
        var map = new HashMap<>();
        map.put(key, value);
        return map;
    }

    protected void assertThrowsNPE(ConsumerEx<IMap<Object, Object>> method) {
        assertThrows(expectedExceptionClass, method);
    }

    private void assertThrows(Class<? extends Exception> expectedExceptionClass,
                              ConsumerEx<IMap<Object, Object>> method) {
        softly.assertThatThrownBy(() ->  method.accept(getDriver().getMap(MAP_NAME)))
                .isInstanceOfAny(NullPointerException.class, expectedExceptionClass);
    }

    protected abstract boolean isNotClient();

    protected abstract HazelcastInstance getDriver();
}
