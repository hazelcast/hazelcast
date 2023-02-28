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

package com.hazelcast.datalink.impl;

import com.hazelcast.config.DataLinkConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.datalink.DataLink;
import com.hazelcast.datalink.DataLinkRegistration;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.util.ServiceLoader;
import com.hazelcast.logging.ILogger;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.datalink.impl.DataLinkServiceImpl.DataLinkSource.CONFIG;
import static com.hazelcast.datalink.impl.DataLinkServiceImpl.DataLinkSource.SQL;
import static com.hazelcast.datalink.impl.DataLinkServiceImpl.DataLinkSourcePair.pair;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;

public class DataLinkServiceImpl implements InternalDataLinkService {

    private final Map<String, Class<? extends DataLink>> typeToDataLinkClass = new HashMap<>();
    private final Map<String, DataLinkSourcePair> dataLinks = new ConcurrentHashMap<>();

    private final ClassLoader classLoader;
    private final ILogger logger;

    public DataLinkServiceImpl(Node node, ClassLoader classLoader) {
        this.classLoader = classLoader;
        this.logger = node.getLogger(DataLinkServiceImpl.class);
        processDataLinkRegistrations(classLoader);

        for (Map.Entry<String, DataLinkConfig> entry : node.getConfig().getDataLinkConfigs().entrySet()) {
            DataLink dataLink = createDataLinkInstance(entry.getValue());
            dataLinks.put(entry.getKey(), pair(dataLink, CONFIG));
        }
    }

    private void processDataLinkRegistrations(ClassLoader classLoader) {
        try {
            ServiceLoader.iterator(
                    DataLinkRegistration.class,
                    DataLinkRegistration.class.getName(),
                    classLoader
            ).forEachRemaining(registration -> typeToDataLinkClass.put(registration.type(), registration.clazz()));
        } catch (Exception e) {
            throw new HazelcastException("Could not register DataLinks", e);
        }
    }

    @Override
    public void createConfigDataLink(DataLinkConfig config) {
        String name = config.getName();
        DataLinkSourcePair currentDataLink = dataLinks.get(name);
        if (currentDataLink != null && currentDataLink.source == SQL) {
            DataLink dataLink = createDataLinkInstance(config);
            replaceDataLinkPair(name, currentDataLink, dataLink);
        } else {
            put(name, config, CONFIG);
        }
    }

    private void replaceDataLinkPair(String name, DataLinkSourcePair currentDataLink, DataLink dataLink) {
        boolean replaced = dataLinks.replace(name, currentDataLink, pair(dataLink, CONFIG));
        if (replaced) {
            currentDataLink.close();
        } else {
            logger.warning("Failed to replace DataLink " + name);
        }
    }

    private void put(String name, DataLinkConfig config, DataLinkSource source) {
        dataLinks.compute(name,
                (key, current) -> {
                    if (current != null) {
                        throw new HazelcastException("Data link '" + name + "' already exists");
                    } else {
                        return pair(createDataLinkInstance(config), source);
                    }
                });
    }

    @Override
    public void createSqlDataLink(String name, String type, Map<String, String> options) {
        if (dataLinks.containsKey(name)) {
            throw new HazelcastException("Data link '" + name + "' already exists");
        }
        put(name, toConfig(name, type, options), SQL);
    }

    private DataLinkConfig toConfig(String name, String type, Map<String, String> options) {
        Properties properties = new Properties();
        properties.putAll(options);
        DataLinkConfig config = new DataLinkConfig(name)
                .setClassName(type)
                .setProperties(properties);
        return config;
    }

    private DataLink createDataLinkInstance(DataLinkConfig config) {
        logger.finest("Creating '" + config.getName() + "' data link");
        String type = config.getClassName();
        try {
            Class<? extends DataLink> dataLinkClass;
            if (typeToDataLinkClass.containsKey(type)) {
                dataLinkClass = typeToDataLinkClass.get(type);
            } else {
                // TODO get rid of this branch when change className -> type in config
                dataLinkClass = getDataLinkClass(type);
            }
            Constructor<? extends DataLink> constructor = dataLinkClass.getConstructor(DataLinkConfig.class);
            return constructor.newInstance(config);
        } catch (ClassCastException e) {
            throw new HazelcastException("Data link '" + config.getName() + "' misconfigured: "
                    + "'" + type + "' must implement '"
                    + DataLink.class.getName() + "'", e);

        } catch (ClassNotFoundException e) {
            throw new HazelcastException("Data link '" + config.getName() + "' misconfigured: "
                    + "class '" + type + "' not found", e);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    private <T> Class<T> getDataLinkClass(String className) throws ClassNotFoundException {
        if (classLoader != null) {
            return (Class<T>) classLoader.loadClass(className);
        } else {
            return (Class<T>) DataLinkServiceImpl.class.getClassLoader().loadClass(className);
        }
    }

    @Override
    public void replaceSqlDataLink(String dataLinkName, String type, Map<String, String> options) {
        DataLinkSourcePair currentDataLink = dataLinks.get(dataLinkName);
        if (currentDataLink != null && currentDataLink.source == SQL) {
            DataLinkConfig config = toConfig(dataLinkName, type, options);

            DataLink dataLink = createDataLinkInstance(config);
            replaceDataLinkPair(dataLinkName, currentDataLink, dataLink);
        }
    }

    @Override
    public <T extends DataLink> T getDataLink(String name, Class<T> clazz) {
        DataLinkSourcePair dataLink = dataLinks.get(name);
        if (dataLink == null) {
            throw new HazelcastException("Data link '" + name + "' not found");
        }
        if (clazz.isInstance(dataLink.instance)) {
            T instance = clazz.cast(dataLink.instance);
            instance.retain();
            return instance;
        } else {
            throw new HazelcastException("Data link '" + name + "' must be an instance of " + clazz);
        }
    }

    @Override
    public boolean existsDataLink(String name) {
        return dataLinks.containsKey(name);
    }

    @Override
    public void removeDataLink(String name) {
        DataLinkSourcePair dataLink = dataLinks.get(name);
        if (dataLink == null) {
            return;
        }
        if (CONFIG.equals(dataLink.source)) {
            throw new HazelcastException("Data link '" + name + "' is configured via Config and can't be removed");
        }
        DataLinkSourcePair removed = dataLinks.remove(name);
        if (removed != null) {
            removed.close();
        }
    }

    @Override
    public void shutdown() {
        for (Map.Entry<String, DataLinkSourcePair> entry : dataLinks.entrySet()) {
            logger.finest("Closing '" + entry.getKey() + "' data link");
            DataLinkSourcePair dataLink = entry.getValue();
            try {
                dataLink.instance.close();
            } catch (Exception e) {
                logger.warning("Closing '" + entry.getKey() + "' data link failed", e);
            }
        }
    }

    enum DataLinkSource {
        CONFIG, SQL
    }

    static class DataLinkSourcePair {
        final DataLink instance;
        final DataLinkSource source;

        DataLinkSourcePair(DataLink instance, DataLinkSource source) {
            this.instance = instance;
            this.source = source;
        }

        static DataLinkSourcePair pair(DataLink dataLink, DataLinkSource source) {
            return new DataLinkSourcePair(dataLink, source);
        }

        void close() {
            try {
                instance.close();
            } catch (Exception e) {
                throw new HazelcastException("Failed to close data link " + instance.getName(), e);
            }
        }
    }
}
