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
import com.hazelcast.map.MapStoreFactory;
import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.internal.util.ExceptionUtil;

import java.util.Properties;

import static com.hazelcast.internal.util.StringUtil.isNullOrEmpty;

/**
 * Creates a map-store object from one of store-factory, store-class or store impl.
 */
final class StoreConstructor {

    private StoreConstructor() {
    }

    static Object createStore(String name, MapStoreConfig mapStoreConfig, ClassLoader classLoader,
                               ManagedContext managedContext) {
        // 1. Try to create store from `store factory` class.
        Object store = getStoreFromFactoryOrNull(name, mapStoreConfig, classLoader, managedContext);

        // 2. Try to get store from `store impl.` object.
        if (store == null) {
            store = getStoreFromImplementationOrNull(mapStoreConfig);
        }
        // 3. Try to create store from `store impl.` class.
        if (store == null) {
            store = getStoreFromClassOrNull(mapStoreConfig, classLoader);
        }
        return store;
    }

    private static Object getStoreFromFactoryOrNull(String name, MapStoreConfig mapStoreConfig, ClassLoader classLoader,
                                                      ManagedContext managedContext) {
        MapStoreFactory factory = (MapStoreFactory) mapStoreConfig.getFactoryImplementation();
        if (factory == null) {
            final String factoryClassName = mapStoreConfig.getFactoryClassName();
            if (isNullOrEmpty(factoryClassName)) {
                return null;
            }

            try {
                factory = ClassLoaderUtil.newInstance(classLoader, factoryClassName);
            } catch (Exception e) {
                throw ExceptionUtil.rethrow(e);
            }
        }
        if (factory == null) {
            return null;
        }

        // Apply container-managed injection (e.g. HazelcastInstanceAware, or Spring/Guice wiring
        // via an external ManagedContext) to the factory itself, the same way it is applied to
        // other user-pluggable, config-instantiated objects such as listeners.
        factory = (MapStoreFactory) managedContext.initialize(factory);

        final Properties properties = mapStoreConfig.getProperties();
        return factory.newMapStore(name, properties);
    }

    private static Object getStoreFromImplementationOrNull(MapStoreConfig mapStoreConfig) {
        return mapStoreConfig.getImplementation();
    }

    private static Object getStoreFromClassOrNull(MapStoreConfig mapStoreConfig, ClassLoader classLoader) {
        Object store;
        String mapStoreClassName = mapStoreConfig.getClassName();
        try {
            store = ClassLoaderUtil.newInstance(classLoader, mapStoreClassName);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        return store;
    }
}
