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
import com.hazelcast.datalink.DataLinkFactory;
import com.hazelcast.datalink.DataLinkService;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.logging.ILogger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;

public class DataLinkServiceImpl implements DataLinkService {
    private final Map<String, DataLinkFactory<?>> dataLinkFactories = new ConcurrentHashMap<>();
    private final ClassLoader classLoader;
    private final Node node;
    private final ILogger logger;

    public DataLinkServiceImpl(Node node, ClassLoader classLoader) {
        this.classLoader = classLoader;
        this.node = node;
        this.logger = node.getLogger(DataLinkServiceImpl.class);
        for (Map.Entry<String, DataLinkConfig> entry : node.getConfig().getDataLinkConfigs().entrySet()) {
            dataLinkFactories.put(entry.getKey(), createFactory(entry.getValue()));
        }
    }

    private <DS> DataLinkFactory<DS> createFactory(DataLinkConfig config) {
        logger.finest("Creating '" + config.getName() + "' data link factory");
        String className = config.getClassName();
        try {
            DataLinkFactory<DS> dataLinkFactory = ClassLoaderUtil.newInstance(classLoader, className);
            dataLinkFactory.init(config);
            return dataLinkFactory;
        } catch (ClassCastException e) {
            throw new HazelcastException("Data link '" + config.getName() + "' misconfigured: "
                    + "'" + className + "' must implement '"
                    + DataLinkFactory.class.getName() + "'", e);

        } catch (ClassNotFoundException e) {
            throw new HazelcastException("Data link '" + config.getName() + "' misconfigured: "
                    + "class '" + className + "' not found", e);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @Override
    public boolean testConnection(DataLinkConfig config) throws Exception {
        try (DataLinkFactory<Object> factory = createFactory(config)) {
            return factory.testConnection();
        }
    }

    @Override
    public <DS> DataLinkFactory<DS> getDataLinkFactory(String name) {
        DataLinkConfig dataLinkConfig = node.getConfig().getDataLinkConfigs().get(name);
        if (dataLinkConfig == null) {
            throw new HazelcastException("Data link factory '" + name + "' not found");
        }
        return (DataLinkFactory<DS>) dataLinkFactories
                .computeIfAbsent(name, n -> createFactory(dataLinkConfig));
    }

    @Override
    public void close() {
        for (Map.Entry<String, DataLinkFactory<?>> entry : dataLinkFactories.entrySet()) {
            try {
                logger.finest("Closing '" + entry.getKey() + "' data link factory");
                DataLinkFactory<?> dataLinkFactory = entry.getValue();
                dataLinkFactory.close();
            } catch (Exception e) {
                logger.warning("Closing '" + entry.getKey() + "' data link factory failed", e);
            }
        }
    }
}
