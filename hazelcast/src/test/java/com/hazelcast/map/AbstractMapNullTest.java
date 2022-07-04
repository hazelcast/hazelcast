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

package com.hazelcast.map;

import com.hazelcast.aggregation.impl.CountAggregator;
import com.hazelcast.config.IndexType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.projection.Projections;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.Assert.fail;

public abstract class AbstractMapNullTest extends HazelcastTestSupport {

    private static final String MAP_NAME = "map";

    @Test
    public void testNullability() {
        EntryAddedListener sampleMapListener = event -> {
        };
        Predicate<Object, Object> samplePredicate = Predicates.alwaysTrue();
        TimeUnit sampleTimeUnit = TimeUnit.SECONDS;

        assertThrowsNPE(m -> m.putAll(null));
        assertThrowsNPE(m -> m.containsKey(null));
        assertThrowsNPE(m -> m.containsValue(null));
        assertThrowsNPE(m -> m.get(null));
        assertThrowsNPE(m -> m.put(null, ""));
        assertThrowsNPE(m -> m.put("", null));
        assertThrowsNPE(m -> m.remove(null));
        assertThrowsNPE(m -> m.remove(null, ""));
        assertThrowsNPE(m -> m.remove("", null));
        assertThrowsNPE(m -> m.removeAll(null));
        assertThrowsNPE(m -> m.delete(null));
        assertThrowsNPE(m -> m.loadAll(null, true));
        assertThrowsNPE(m -> m.getAsync(null));

        assertThrowsNPE(m -> m.putAsync(null, ""));
        assertThrowsNPE(m -> m.putAsync("", null));

        assertThrowsNPE(m -> m.putAsync(null, "", -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.putAsync("", null, -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.putAsync("", "", -1, null));
        assertThrowsNPE(m -> m.putAsync(null, "", -1, sampleTimeUnit, -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.putAsync("", null, -1, sampleTimeUnit, -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.putAsync("", "", -1, null, -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.putAsync("", "", -1, sampleTimeUnit, -1, null));

        assertThrowsNPE(m -> m.setAsync(null, ""));
        assertThrowsNPE(m -> m.setAsync("", null));
        assertThrowsNPE(m -> m.setAsync(null, "", -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.setAsync("", null, -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.setAsync("", "", -1, null));
        assertThrowsNPE(m -> m.setAsync(null, "", -1, sampleTimeUnit, -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.setAsync("", null, -1, sampleTimeUnit, -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.setAsync("", "", -1, null, -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.setAsync("", "", -1, sampleTimeUnit, -1, null));

        assertThrowsNPE(m -> m.tryRemove(null, -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.tryRemove("", -1, null));

        assertThrowsNPE(m -> m.tryPut(null, "", -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.tryPut("", null, -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.tryPut("", "", -1, null));

        assertThrowsNPE(m -> m.put(null, "", -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.put("", null, -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.put("", "", -1, null));

        assertThrowsNPE(m -> m.putTransient(null, "", -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.putTransient("", null, -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.putTransient("", "", -1, null));
        assertThrowsNPE(m -> m.putTransient(null, "", -1, sampleTimeUnit, -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.putTransient("", null, -1, sampleTimeUnit, -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.putTransient("", "", -1, null, -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.putTransient("", "", -1, sampleTimeUnit, -1, null));

        assertThrowsNPE(m -> m.putIfAbsent(null, ""));
        assertThrowsNPE(m -> m.putIfAbsent("", null));
        assertThrowsNPE(m -> m.putIfAbsent(null, "", -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.putIfAbsent("", null, -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.putIfAbsent("", "", -1, null));
        assertThrowsNPE(m -> m.putIfAbsent(null, "", -1, sampleTimeUnit, -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.putIfAbsent("", null, -1, sampleTimeUnit, -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.putIfAbsent("", "", -1, null, -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.putIfAbsent("", "", -1, sampleTimeUnit, -1, null));

        assertThrowsNPE(m -> m.replace(null, "", ""));
        assertThrowsNPE(m -> m.replace("", null, ""));
        assertThrowsNPE(m -> m.replace("", "", null));
        assertThrowsNPE(m -> m.replace(null, ""));
        assertThrowsNPE(m -> m.replace("", null));

        assertThrowsNPE(m -> m.set(null, ""));
        assertThrowsNPE(m -> m.set("", null));
        assertThrowsNPE(m -> m.set(null, "", -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.set("", null, -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.set("", "", -1, null));
        assertThrowsNPE(m -> m.set(null, "", -1, sampleTimeUnit, -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.set("", null, -1, sampleTimeUnit, -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.set("", "", -1, null, -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.set("", "", -1, sampleTimeUnit, -1, null));

        assertThrowsNPE(m -> m.setAll(null));
        assertThrowsNPE(m -> m.setAllAsync(null));

        assertThrowsNPE(m -> m.lock(null));
        assertThrowsNPE(m -> m.lock(null, -1, sampleTimeUnit));

        assertThrowsNPE(m -> m.tryLock(null));
        assertThrowsNPE(m -> m.tryLock(null, -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.tryLock(null, -1, sampleTimeUnit, -1, sampleTimeUnit));

        assertThrowsNPE(m -> m.unlock(null));
        assertThrowsNPE(m -> m.forceUnlock(null));
        if (isNotClient()) {
            assertThrowsNPE(m -> m.addLocalEntryListener((MapListener) null));
            assertThrowsNPE(m -> m.addLocalEntryListener((MapListener) null, samplePredicate, true));
            assertThrowsNPE(m -> m.addLocalEntryListener(sampleMapListener, null, true));
            assertThrowsNPE(m -> m.addLocalEntryListener(sampleMapListener, null, "", true));
            assertThrowsNPE(m -> m.localKeySet(null));
        }

        assertThrowsNPE(m -> m.addInterceptor(null));
        assertThrowsNPE(m -> m.removeInterceptor(null));
        assertThrowsNPE(m -> m.addEntryListener((MapListener) null, "", true));
        assertThrowsNPE(m -> m.addEntryListener((MapListener) null, (Object) null, true));
        assertThrowsNPE(m -> m.addEntryListener((MapListener) null, samplePredicate, true));
        assertThrowsNPE(m -> m.addEntryListener(sampleMapListener, null, true));
        assertThrowsNPE(m -> m.addEntryListener((MapListener) null, samplePredicate, "", true));
        assertThrowsNPE(m -> m.addEntryListener(sampleMapListener, null, "", true));
        assertThrowsNPE(m -> m.removeEntryListener(null));
        assertThrowsNPE(m -> m.addPartitionLostListener(null));
        assertThrowsNPE(m -> m.removePartitionLostListener(null));
        assertThrowsNPE(m -> m.getEntryView(null));
        assertThrowsNPE(m -> m.evict(null));
        assertThrowsNPE(m -> m.keySet(null));
        assertThrowsNPE(m -> m.entrySet(null));
        assertThrowsNPE(m -> m.values(null));
        assertThrows(NullPointerException.class, m -> m.addIndex(null, "attribute"));
        assertThrows(NullPointerException.class, m -> m.addIndex(IndexType.SORTED, null));
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
        assertThrowsNPE(m -> m.setTtl(null, -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.setTtl("", -1, null));
    }

    protected void assertThrowsNPE(ConsumerEx<IMap<Object, Object>> method) {
        assertThrows(NullPointerException.class, method);
    }

    private void assertThrows(Class<? extends Exception> expectedExceptionClass,
                              ConsumerEx<IMap<Object, Object>> method) {
        try {
            method.accept(getDriver().getMap(MAP_NAME));
            fail("Expected " + expectedExceptionClass
                    + " but there was no exception!");
        } catch (Exception e) {
            Assert.assertSame(expectedExceptionClass, e.getClass());
        }
    }

    @FunctionalInterface
    public interface ConsumerEx<T> extends Consumer<T> {
        void acceptEx(T t) throws Exception;

        @Override
        default void accept(T t) {
            try {
                acceptEx(t);
            } catch (Exception e) {
                ExceptionUtil.sneakyThrow(e);
            }
        }
    }

    protected abstract boolean isNotClient();

    protected abstract HazelcastInstance getDriver();
}
