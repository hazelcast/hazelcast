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

package com.hazelcast.map.impl.mapstore;

import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.map.MapLoader;
import com.hazelcast.map.MapStore;
import com.hazelcast.map.MapStoreFactory;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class StoreConstructorTest extends HazelcastTestSupport {

    /**
     * Verifies the fix for a defect where a {@link MapStoreFactory} instantiated from
     * {@link MapStoreConfig#getFactoryClassName()} never went through {@link ManagedContext#initialize(Object)},
     * so container-managed injection (HazelcastInstanceAware, or an external Spring/Guice ManagedContext)
     * was silently skipped for the factory.
     */
    @Test
    public void createStore_appliesManagedContext_toFactoryInstantiatedFromClassName() {
        RecordingManagedContext managedContext = new RecordingManagedContext();
        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                .setFactoryClassName(RecordingMapStoreFactory.class.getName());

        Object store = StoreConstructor.createStore("test-map", mapStoreConfig,
                getClass().getClassLoader(), managedContext);

        assertTrue("factory instantiated from class name should have been passed to ManagedContext#initialize",
                managedContext.initializedObjects.stream().anyMatch(o -> o instanceof RecordingMapStoreFactory));
        assertSame(RecordingMapStoreFactory.PRODUCED_STORE, store);
    }

    @Test
    public void createStore_appliesManagedContext_toFactoryProvidedAsImplementation() {
        RecordingManagedContext managedContext = new RecordingManagedContext();
        RecordingMapStoreFactory factory = new RecordingMapStoreFactory();
        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                .setFactoryImplementation(factory);

        Object store = StoreConstructor.createStore("test-map", mapStoreConfig,
                getClass().getClassLoader(), managedContext);

        assertSame(factory, managedContext.initializedObjects.stream()
                .filter(o -> o instanceof RecordingMapStoreFactory)
                .findFirst()
                .orElse(null));
        assertSame(RecordingMapStoreFactory.PRODUCED_STORE, store);
    }

    @Test
    public void createStore_appliesManagedContext_toStoreProvidedAsImplementation() {
        RecordingManagedContext managedContext = new RecordingManagedContext();
        var storeImpl = new RecordingMapStore();
        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                                            .setImplementation(storeImpl);

        Object store = StoreConstructor.createStore("test-map", mapStoreConfig,
                                                    getClass().getClassLoader(), managedContext);

        assertSame(storeImpl, managedContext.initializedObjects.stream()
                                                             .filter(o -> o instanceof RecordingMapStore)
                                                             .findFirst()
                                                             .orElse(null));
        assertSame(store, managedContext.initializedObjects.stream()
                                                             .filter(o -> o instanceof RecordingMapStore)
                                                             .findFirst()
                                                             .orElse(null));
    }

    @Test
    public void createStore_appliesManagedContext_toStoreProvidedAsClassName() {
        RecordingManagedContext managedContext = new RecordingManagedContext();
        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                                            .setClassName(RecordingMapStore.class.getName());

        Object store = StoreConstructor.createStore("test-map", mapStoreConfig,
                                                    getClass().getClassLoader(), managedContext);

        assertSame(store, managedContext.initializedObjects.stream()
                                                             .filter(o -> o instanceof RecordingMapStore)
                                                             .findFirst()
                                                             .orElse(null));
    }

    /** No-op except for recording every object passed to {@link #initialize(Object)}, in call order. */
    private static class RecordingManagedContext implements ManagedContext {

        final java.util.List<Object> initializedObjects = new java.util.ArrayList<>();

        @Override
        public Object initialize(Object obj) {
            initializedObjects.add(obj);
            return obj;
        }
    }

    /** Public no-arg constructor required for {@code ClassLoaderUtil.newInstance} reflection-based instantiation. */
    public static class RecordingMapStoreFactory implements MapStoreFactory<Object, Object> {

        static final MapLoader<Object, Object> PRODUCED_STORE = new MapLoader<>() {
            @Override
            public Object load(Object key) {
                return null;
            }

            @Override
            public Map<Object, Object> loadAll(Collection<Object> keys) {
                return null;
            }

            @Override
            public Iterable<Object> loadAllKeys() {
                return null;
            }
        };

        @Override
        public MapLoader<Object, Object> newMapStore(String mapName, Properties properties) {
            return PRODUCED_STORE;
        }
    }

    public static class RecordingMapStore implements MapStore<Object, Object> {

        @Override
        public void store(Object key, Object value) {
        }

        @Override
        public void storeAll(Map<Object, Object> map) {
        }

        @Override
        public void delete(Object key) {
        }

        @Override
        public void deleteAll(Collection<Object> keys) {
        }

        @Override
        public Object load(Object key) {
            return null;
        }

        @Override
        public Map<Object, Object> loadAll(Collection<Object> keys) {
            return Map.of();
        }

        @Override
        public Iterable<Object> loadAllKeys() {
            return null;
        }
    }
}
