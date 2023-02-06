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

import com.hazelcast.config.ExternalDataLinkConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.datalink.ExternalDataLinkFactory;
import com.hazelcast.datalink.ExternalDataLinkService;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.logging.ILogger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;

public class ExternalDataLinkServiceImpl implements ExternalDataLinkService {
    private final Map<String, ExternalDataLinkFactory<?>> dataLinkFactories = new ConcurrentHashMap<>();
    private final ClassLoader classLoader;
    private final Node node;
    private final ILogger logger;

    public ExternalDataLinkServiceImpl(Node node, ClassLoader classLoader) {
        this.classLoader = classLoader;
        this.node = node;
        this.logger = node.getLogger(ExternalDataLinkServiceImpl.class);
        for (Map.Entry<String, ExternalDataLinkConfig> entry : node.getConfig().getExternalDataLinkConfigs().entrySet()) {
            dataLinkFactories.put(entry.getKey(), createFactory(entry.getValue()));
        }
    }

    private <DS> ExternalDataLinkFactory<DS> createFactory(ExternalDataLinkConfig config) {
        logger.finest("Creating '" + config.getName() + "' external data link factory");
        String className = config.getClassName();
        try {
            ExternalDataLinkFactory<DS> externalDataLinkFactory = ClassLoaderUtil.newInstance(classLoader, className);
            externalDataLinkFactory.init(config);
            return externalDataLinkFactory;
        } catch (ClassCastException e) {
            throw new HazelcastException("External data link '" + config.getName() + "' misconfigured: "
                    + "'" + className + "' must implement '"
                    + ExternalDataLinkFactory.class.getName() + "'", e);

        } catch (ClassNotFoundException e) {
            throw new HazelcastException("External data link '" + config.getName() + "' misconfigured: "
                    + "class '" + className + "' not found", e);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @Override
    public boolean testConnection(ExternalDataLinkConfig config) throws Exception {
        try (ExternalDataLinkFactory<Object> factory = createFactory(config)) {
            return factory.testConnection();
        }
    }

    @Override
    public <DS> ExternalDataLinkFactory<DS> getExternalDataLinkFactory(String name) {
        ExternalDataLinkConfig externalDataLinkConfig = node.getConfig().getExternalDataLinkConfigs().get(name);
        if (externalDataLinkConfig == null) {
            throw new HazelcastException("External data link factory '" + name + "' not found");
        }
        return (ExternalDataLinkFactory<DS>) dataLinkFactories
                .computeIfAbsent(name, n -> createFactory(externalDataLinkConfig));
    }

    @Override
    public void close() {
        for (Map.Entry<String, ExternalDataLinkFactory<?>> entry : dataLinkFactories.entrySet()) {
            try {
                logger.finest("Closing '" + entry.getKey() + "' external data link factory");
                ExternalDataLinkFactory<?> dataLinkFactory = entry.getValue();
                dataLinkFactory.close();
            } catch (Exception e) {
                logger.warning("Closing '" + entry.getKey() + "' external datastore factory failed", e);
            }
        }
    }
}
