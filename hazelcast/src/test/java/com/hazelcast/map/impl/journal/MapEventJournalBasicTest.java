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

package com.hazelcast.map.impl.journal;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.journal.EventJournalDataStructureAdapter;
import com.hazelcast.map.MapStore;
import com.hazelcast.journal.AbstractEventJournalBasicTest;
import com.hazelcast.journal.EventJournalTestContext;
import com.hazelcast.map.EventJournalMapEvent;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.util.MapUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.hazelcast.config.MapStoreConfig.InitialLoadMode.EAGER;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapEventJournalBasicTest<K, V> extends AbstractEventJournalBasicTest<EventJournalMapEvent> {

    private static final String NON_EXPIRING_MAP = "mappy";
    private static final String EXPIRING_MAP = "expiring";

    public final AtomicBoolean isPredicateContextInjected = new AtomicBoolean(false);
    public final AtomicBoolean isProjectionContextInjected = new AtomicBoolean(false);
    @Override
    protected Config getConfig() {
        Config config = super.getConfig();
        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                .setEnabled(true)
                .setInitialLoadMode(EAGER)
                .setImplementation(new CustomMapStore());

        config.getMapConfig(NON_EXPIRING_MAP)
              .setInMemoryFormat(getInMemoryFormat())
              .setMapStoreConfig(mapStoreConfig);

        config.getMapConfig(EXPIRING_MAP).setTimeToLiveSeconds(1)
              .setInMemoryFormat(getInMemoryFormat());
        config.setManagedContext(obj -> {
            if (obj instanceof PredicateWithContext) {
                setPredicate(true);
            }
            if (obj instanceof ProjectionWithContext) {
                setProjection(true);
            }
            return obj;
        });
        return config;
    }

    protected InMemoryFormat getInMemoryFormat() {
        return MapConfig.DEFAULT_IN_MEMORY_FORMAT;
    }

    @Override
    protected EventJournalTestContext<K, V, EventJournalMapEvent<K, V>> createContext() {
        return new EventJournalTestContext<K, V, EventJournalMapEvent<K, V>>(
                new EventJournalMapDataStructureAdapter<K, V>(getRandomInstance().<K, V>getMap(NON_EXPIRING_MAP)),
                new EventJournalMapDataStructureAdapter<K, V>(getRandomInstance().<K, V>getMap(EXPIRING_MAP)),
                new EventJournalMapEventAdapter<K, V>()
        );
    }

    @Test
    public void testPredicateAndProjectionContextInjection() {
        final EventJournalTestContext<K, V, EventJournalMapEvent<K, V>> context = createContext();
        readFromEventJournal((EventJournalDataStructureAdapter) context.dataAdapter, 0,
                1, 1, new PredicateWithContext(), new ProjectionWithContext());
        assertAtomicEventually("Predicate Must be injected with ManagedContext", true, getPredicate(), 30);
        assertAtomicEventually("Projection must injected with ManagedContext", true, getProjection(), 30);
    }

    public void setPredicate(boolean value) {
        this.isPredicateContextInjected.set(value);
    }

    public void setProjection(boolean value) {
        this.isProjectionContextInjected.set(value);
    }

    public AtomicBoolean getProjection() {
        return this.isProjectionContextInjected;
    }

    public AtomicBoolean getPredicate() {
        return this.isPredicateContextInjected;
    }

    public static class CustomMapStore implements MapStore<Object, Object> {

        @Override
        public void store(Object key, Object value) {
            // NOP
        }

        @Override
        public void storeAll(Map<Object, Object> map) {
            // NOP
        }

        @Override
        public void delete(Object key) {
            // NOP
        }

        @Override
        public void deleteAll(Collection<Object> keys) {
            // NOP
        }

        @Override
        public Object load(Object key) {
            return key;
        }

        @Override
        public Map<Object, Object> loadAll(Collection<Object> keys) {
            Map<Object, Object> map = MapUtil.createHashMap(keys.size());
            for (Object key : keys) {
                map.put(key, key);
            }
            return map;
        }

        @Override
        public Iterable<Object> loadAllKeys() {
            return Collections.emptySet();
        }
    }
    public static class PredicateWithContext implements Predicate, Serializable {
        @Override
        public boolean test(Object o) {
            return true;
        }
    }
    public static class ProjectionWithContext implements Function , Serializable {
        @Override
        public Object apply(Object o) {
            return null;
        }
    }
}
