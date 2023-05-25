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

package com.hazelcast.dataconnection.impl;

import com.hazelcast.config.DataConnectionConfig;
import com.hazelcast.config.DataConnectionConfigValidator;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.dataconnection.DataConnection;
import com.hazelcast.dataconnection.DataConnectionRegistration;
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

import static com.hazelcast.dataconnection.impl.DataConnectionServiceImpl.DataConnectionSource.CONFIG;
import static com.hazelcast.dataconnection.impl.DataConnectionServiceImpl.DataConnectionSource.SQL;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;

public class DataConnectionServiceImpl implements InternalDataConnectionService {

    private final Map<String, Class<? extends DataConnection>> typeToDataConnectionClass = new HashMap<>();
    private final Map<Class<? extends DataConnection>, String> dataConnectionClassToType = new HashMap<>();

    private final Map<String, DataConnectionEntry> dataConnections = new ConcurrentHashMap<>();
    private final ILogger logger;

    public DataConnectionServiceImpl(Node node, ClassLoader classLoader) {
        this.logger = node.getLogger(getClass());
        processDataConnectionRegistrations(classLoader);

        for (DataConnectionConfig config : node.getConfig().getDataConnectionConfigs().values()) {
            put(config, CONFIG);
        }
    }

    private void processDataConnectionRegistrations(ClassLoader classLoader) {
        try {
            ServiceLoader.iterator(
                    DataConnectionRegistration.class,
                    DataConnectionRegistration.class.getName(),
                    classLoader
            ).forEachRemaining(registration -> {
                typeToDataConnectionClass.put(registration.type().toLowerCase(Locale.ROOT), registration.clazz());
                dataConnectionClassToType.put(registration.clazz(), registration.type().toLowerCase(Locale.ROOT));
            });
        } catch (Exception e) {
            throw new HazelcastException("Could not register data connections", e);
        }
    }

    @Override
    public void createConfigDataConnection(DataConnectionConfig config) {
        put(config, CONFIG);
    }

    private void put(DataConnectionConfig config, DataConnectionSource source) {
        DataConnectionConfigValidator.validate(config);
        dataConnections.compute(config.getName(),
                (key, current) -> {
                    if (current != null) {
                        if (current.source == CONFIG) {
                            throw new HazelcastException("Cannot replace a data connection created from configuration");
                        }
                        if (current.instance.getConfig().equals(config)) {
                            return current;
                        }
                        // close the old DataConnection
                        logger.fine("Asynchronously closing the old data connection: " + config.getName());
                        ForkJoinPool.commonPool().execute(() -> {
                            try {
                                current.instance.release();
                            } catch (Throwable e) {
                                logger.severe("Error when closing data connection '" + config.getName()
                                        + "', ignoring it: " + e, e);
                            }
                        });
                    }
                    return new DataConnectionEntry(createDataConnectionInstance(config), source);
                });
    }

    @Override
    public void createOrReplaceSqlDataConnection(String name, String type, boolean shared, Map<String, String> options) {
        put(toConfig(name, type, shared, options), SQL);
    }

    @Override
    public boolean existsConfigDataConnection(String name) {
        DataConnectionEntry dl = dataConnections.get(name);
        return dl != null && dl.source == CONFIG;
    }

    @Override
    public boolean existsSqlDataConnection(String name) {
        DataConnectionEntry dl = dataConnections.get(name);
        return dl != null && dl.source == SQL;
    }

    // package-private for testing purposes
    DataConnectionConfig toConfig(String name, String type, boolean shared, Map<String, String> options) {
        Properties properties = new Properties();
        properties.putAll(options);
        return new DataConnectionConfig(name)
                .setType(type)
                .setShared(shared)
                .setProperties(properties);
    }

    private DataConnection createDataConnectionInstance(DataConnectionConfig config) {
        logger.finest("Creating '" + config.getName() + "' data connection");
        String type = config.getType();
        try {
            Class<? extends DataConnection> dataConnectionClass = typeToDataConnectionClass.get(type.toLowerCase(Locale.ROOT));
            if (dataConnectionClass == null) {
                throw new HazelcastException("Data connection '" + config.getName() + "' misconfigured: "
                        + "unknown type '" + type + "'");
            }
            Constructor<? extends DataConnection> constructor = dataConnectionClass.getConstructor(DataConnectionConfig.class);
            return constructor.newInstance(config);
        } catch (ClassCastException e) {
            throw new HazelcastException("Data connection '" + config.getName() + "' misconfigured: "
                    + "'" + type + "' must implement '"
                    + DataConnection.class.getName() + "'", e);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @Override
    public String typeForDataConnection(String name) {
        DataConnectionEntry dataConnection = dataConnections.get(name);
        if (dataConnection == null) {
            throw new HazelcastException("Data connection '" + name + "' not found");
        }
        String type = dataConnectionClassToType.get(dataConnection.instance.getClass());
        if (type == null) {
            throw new HazelcastException("Data connection type for class '" + dataConnection.getClass() + "' is not known");
        }
        return type;
    }

    @Override
    public Class<? extends DataConnection> classForDataConnectionType(String type) {
        String caseInsensitiveType = type.toLowerCase(Locale.ROOT);
        Class<? extends DataConnection> dataConnectionClass = typeToDataConnectionClass.get(caseInsensitiveType);
        if (dataConnectionClass == null) {
            throw new HazelcastException("Data connection type '" + type + "' is not known");
        }
        return dataConnectionClass;
    }

    @Override
    @Nonnull
    public <T extends DataConnection> T getAndRetainDataConnection(String name, Class<T> clazz) {
        DataConnectionEntry dataConnection = dataConnections.computeIfPresent(name, (k, v) -> {
            if (!clazz.isInstance(v.instance)) {
                throw new HazelcastException("Data connection '" + name + "' must be an instance of " + clazz);
            }
            v.instance.retain();
            return v;
        });

        if (dataConnection == null) {
            throw new HazelcastException("Data connection '" + name + "' not found");
        }
        //noinspection unchecked
        return (T) dataConnection.instance;
    }

    @Override
    public void removeDataConnection(String name) {
        dataConnections.computeIfPresent(name, (k, v) -> {
            if (CONFIG.equals(v.source)) {
                throw new HazelcastException("Data connection '" + name + "' is configured via Config "
                        + "and can't be removed");
            }
            v.instance.release();
            return null;
        });
    }

    public List<DataConnection> getConfigCreatedDataConnections() {
        return dataConnections.values()
                .stream()
                .filter(dl -> dl.source == CONFIG)
                .map(dl -> dl.instance)
                .collect(Collectors.toList());
    }

    public List<DataConnection> getSqlCreatedDataConnections() {
        return dataConnections.values()
                .stream()
                .filter(dl -> dl.source == SQL)
                .map(dl -> dl.instance)
                .collect(Collectors.toList());
    }

    @Override
    public void shutdown() {
        for (Map.Entry<String, DataConnectionEntry> entry : dataConnections.entrySet()) {
            logger.finest("Closing '" + entry.getKey() + "' data connection");
            DataConnectionEntry dataConnection = entry.getValue();
            try {
                dataConnection.instance.destroy();
            } catch (Exception e) {
                logger.warning("Closing '" + entry.getKey() + "' data connection failed", e);
            }
        }
    }

    public enum DataConnectionSource {
        CONFIG, SQL
    }

    public static class DataConnectionEntry {
        private final DataConnection instance;
        private final DataConnectionSource source;

        DataConnectionEntry(DataConnection instance, DataConnectionSource source) {
            this.instance = instance;
            this.source = source;
        }

        public DataConnection getInstance() {
            return instance;
        }

        public DataConnectionSource getSource() {
            return source;
        }
    }
}
