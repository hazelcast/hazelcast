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

package com.hazelcast.datastore.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.ExternalDataStoreConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.datastore.ExternalDataStoreFactory;
import com.hazelcast.datastore.ExternalDataStoreService;
import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.internal.nio.IOUtil;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;

public class ExternalDataStoreServiceImpl implements ExternalDataStoreService {
    private final Map<String, ExternalDataStoreFactory<?>> dataStoreFactories = new ConcurrentHashMap<>();

    public ExternalDataStoreServiceImpl(Config config, ClassLoader classLoader) {
        for (Map.Entry<String, ExternalDataStoreConfig> entry : config.getExternalDataStoreConfigs().entrySet()) {
            dataStoreFactories.put(entry.getKey(), createFactory(entry.getValue(), classLoader));
        }
    }

    private ExternalDataStoreFactory<?> createFactory(ExternalDataStoreConfig config, ClassLoader classLoader) {
        String className = config.getClassName();
        try {
            ExternalDataStoreFactory<?> externalDataStoreFactory = ClassLoaderUtil.newInstance(classLoader, className);
            externalDataStoreFactory.init(config);
            return externalDataStoreFactory;
        } catch (ClassCastException e) {
            throw new HazelcastException("External data store '" + config.getName() + "' misconfigured: "
                    + "'" + className + "' must implement '"
                    + ExternalDataStoreFactory.class.getName() + "'", e);

        } catch (ClassNotFoundException e) {
            throw new HazelcastException("External data store '" + config.getName() + "' misconfigured: "
                    + "class '" + className + "' not found", e);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @Override
    public ExternalDataStoreFactory<?> getExternalDataStoreFactory(String name) {
        ExternalDataStoreFactory<?> externalDataStoreFactory = dataStoreFactories.get(name);
        if (externalDataStoreFactory == null) {
            throw new HazelcastException("External data store factory '" + name + "' not found");
        }
        return externalDataStoreFactory;
    }

    public void shutDown(boolean terminate) {
        if (terminate) {
            return;
        }

        for (ExternalDataStoreFactory<?> dataStoreFactory : dataStoreFactories.values()) {
            Object dataStore = dataStoreFactory.getDataStore();
            if (dataStore instanceof Closeable) {
                IOUtil.closeResource(((Closeable) dataStore));
            }
        }
    }
}
