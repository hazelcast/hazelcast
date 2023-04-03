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

import javax.annotation.Nonnull;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;

import static com.hazelcast.datalink.impl.DataLinkServiceImpl.DataLinkSource.CONFIG;
import static com.hazelcast.datalink.impl.DataLinkServiceImpl.DataLinkSource.SQL;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;

public class DataLinkServiceImpl implements InternalDataLinkService {

    private final Map<String, Class<? extends DataLink>> typeToDataLinkClass = new HashMap<>();
    private final Map<Class<? extends DataLink>, String> dataLinkClassToType = new HashMap<>();

    private final Map<String, DataLinkEntry> dataLinks = new ConcurrentHashMap<>();

    private final ClassLoader classLoader;
    private final ILogger logger;

    public DataLinkServiceImpl(Node node, ClassLoader classLoader) {
        this.classLoader = classLoader;
        this.logger = node.getLogger(getClass());
        processDataLinkRegistrations(classLoader);

        for (DataLinkConfig config : node.getConfig().getDataLinkConfigs().values()) {
            put(config, CONFIG);
        }
    }

    private void processDataLinkRegistrations(ClassLoader classLoader) {
        try {
            ServiceLoader.iterator(
                    DataLinkRegistration.class,
                    DataLinkRegistration.class.getName(),
                    classLoader
            ).forEachRemaining(registration -> {
                typeToDataLinkClass.put(registration.type().toLowerCase(Locale.ROOT), registration.clazz());
                dataLinkClassToType.put(registration.clazz(), registration.type());
            });
        } catch (Exception e) {
            throw new HazelcastException("Could not register DataLinks", e);
        }
    }

    @Override
    public void createConfigDataLink(DataLinkConfig config) {
        put(config, CONFIG);
    }

    private void put(DataLinkConfig config, DataLinkSource source) {
        dataLinks.compute(config.getName(),
                (key, current) -> {
                    if (current != null) {
                        if (current.source == CONFIG) {
                            throw new HazelcastException("Cannot replace a data link created from configuration");
                        }
                        if (current.instance.getConfig().equals(config)) {
                            return current;
                        }
                        // close the old DataLink
                        logger.fine("Asynchronously closing the old datalink: " + config.getName());
                        ForkJoinPool.commonPool().execute(() -> {
                            try {
                                current.instance.release();
                            } catch (Throwable e) {
                                logger.severe("Error when closing data link '" + config.getName()
                                        + "', ignoring it: " + e, e);
                            }
                        });
                    }
                    return new DataLinkEntry(createDataLinkInstance(config), source);
                });
    }

    @Override
    public void replaceSqlDataLink(String name, String type, boolean shared, Map<String, String> options) {
        put(toConfig(name, type, shared, options), SQL);
    }

    @Override
    public boolean existsConfigDataLink(String name) {
        DataLinkEntry dl = dataLinks.get(name);
        return dl != null && dl.source == CONFIG;
    }

    @Override
    public boolean existsSqlDataLink(String name) {
        DataLinkEntry dl = dataLinks.get(name);
        return dl != null && dl.source == SQL;
    }

    // package-private for testing purposes
    DataLinkConfig toConfig(String name, String type, boolean shared, Map<String, String> options) {
        Properties properties = new Properties();
        properties.putAll(options);
        return new DataLinkConfig(name)
                .setType(type)
                .setShared(shared)
                .setProperties(properties);
    }

    private DataLink createDataLinkInstance(DataLinkConfig config) {
        logger.finest("Creating '" + config.getName() + "' data link");
        String type = config.getType();
        try {
            Class<? extends DataLink> dataLinkClass = typeToDataLinkClass.get(type.toLowerCase(Locale.ROOT));
            if (dataLinkClass == null) {
                throw new HazelcastException("Data link '" + config.getName() + "' misconfigured: "
                        + "unknown type '" + type + "'");
            }
            Constructor<? extends DataLink> constructor = dataLinkClass.getConstructor(DataLinkConfig.class);
            return constructor.newInstance(config);
        } catch (ClassCastException e) {
            throw new HazelcastException("Data link '" + config.getName() + "' misconfigured: "
                    + "'" + type + "' must implement '"
                    + DataLink.class.getName() + "'", e);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @Override
    public String typeForDataLink(String name) {
        DataLinkEntry dataLink = dataLinks.get(name);
        if (dataLink == null) {
            throw new HazelcastException("DataLink with name '" + name + "' does not exist");
        }
        String type = dataLinkClassToType.get(dataLink.instance.getClass());
        if (type == null) {
            throw new HazelcastException("DataLink type for class '" + dataLink.getClass() + "' is not known");
        }
        return type;
    }

    @Override
    public Class<? extends DataLink> classForDataLinkType(String type) {
        Class<? extends DataLink> dataLinkClass = typeToDataLinkClass.get(type);
        if (dataLinkClass == null) {
            throw new HazelcastException("DataLink type '" + type + "' is not known");
        }
        return dataLinkClass;
    }

    @Override
    @Nonnull
    public <T extends DataLink> T getAndRetainDataLink(String name, Class<T> clazz) {
        DataLinkEntry dataLink = dataLinks.computeIfPresent(name, (k, v) -> {
            if (!clazz.isInstance(v.instance)) {
                throw new HazelcastException("Data link '" + name + "' must be an instance of " + clazz);
            }
            v.instance.retain();
            return v;
        });

        if (dataLink == null) {
            throw new HazelcastException("Data link '" + name + "' not found");
        }
        //noinspection unchecked
        return (T) dataLink.instance;
    }

    @Override
    public void removeDataLink(String name) {
        dataLinks.computeIfPresent(name, (k, v) -> {
            if (CONFIG.equals(v.source)) {
                throw new HazelcastException("Data link '" + name + "' is configured via Config and can't be removed");
            }
            v.instance.release();
            return null;
        });
    }

    public List<DataLink> getConfigCreatedDataLinks() {
        return dataLinks.values()
                        .stream()
                        .filter(dl -> dl.source == CONFIG)
                        .map(dl -> dl.instance)
                        .collect(Collectors.toList());
    }

    public List<DataLink> getSqlCreatedDataLinks() {
        return dataLinks.values()
                        .stream()
                        .filter(dl -> dl.source == SQL)
                        .map(dl -> dl.instance)
                        .collect(Collectors.toList());
    }

    @Override
    public void shutdown() {
        for (Map.Entry<String, DataLinkEntry> entry : dataLinks.entrySet()) {
            logger.finest("Closing '" + entry.getKey() + "' data link");
            DataLinkEntry dataLink = entry.getValue();
            try {
                dataLink.instance.destroy();
            } catch (Exception e) {
                logger.warning("Closing '" + entry.getKey() + "' data link failed", e);
            }
        }
    }

    public enum DataLinkSource {
        CONFIG, SQL
    }

    public static class DataLinkEntry {
        private final DataLink instance;
        private final DataLinkSource source;

        DataLinkEntry(DataLink instance, DataLinkSource source) {
            this.instance = instance;
            this.source = source;
        }

        public DataLink getInstance() {
            return instance;
        }

        public DataLinkSource getSource() {
            return source;
        }
    }
}
