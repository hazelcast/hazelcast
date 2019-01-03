/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.task.dynamicconfig;

import com.hazelcast.config.QueueStoreConfig;
import com.hazelcast.core.QueueStore;
import com.hazelcast.core.QueueStoreFactory;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;

import java.util.Properties;

/**
 * Adapter for using {@link QueueStoreConfig} in client protocol.
 */
public class QueueStoreConfigHolder {

    private final String className;
    private final String factoryClassName;
    private final Data implementation;
    private final Data factoryImplementation;
    private final Properties properties;
    private final boolean enabled;

    public QueueStoreConfigHolder(String className, String factoryClassName, Data implementation,
                                  Data factoryImplementation, Properties properties, boolean enabled) {
        this.className = className;
        this.factoryClassName = factoryClassName;
        this.implementation = implementation;
        this.factoryImplementation = factoryImplementation;
        this.properties = properties;
        this.enabled = enabled;
    }

    public String getClassName() {
        return className;
    }

    public String getFactoryClassName() {
        return factoryClassName;
    }

    public Data getImplementation() {
        return implementation;
    }

    public Data getFactoryImplementation() {
        return factoryImplementation;
    }

    public Properties getProperties() {
        return properties;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public QueueStoreConfig asQueueStoreConfig(SerializationService serializationService) {
        QueueStoreConfig config = new QueueStoreConfig();
        config.setClassName(className);
        config.setEnabled(enabled);
        config.setFactoryClassName(factoryClassName);
        config.setProperties(properties);
        QueueStore storeImplementation = serializationService.toObject(implementation);
        QueueStoreFactory storeFactoryImplementation = serializationService.toObject(factoryImplementation);
        config.setStoreImplementation(storeImplementation);
        config.setFactoryImplementation(storeFactoryImplementation);
        return config;
    }

    public static QueueStoreConfigHolder of(QueueStoreConfig queueStoreConfig,
                                            SerializationService serializationService) {
        if (queueStoreConfig == null) {
            return null;
        }

        if (queueStoreConfig.getClassName() == null && queueStoreConfig.getFactoryClassName() == null
                && queueStoreConfig.getStoreImplementation() == null
                && queueStoreConfig.getFactoryImplementation() == null
                && queueStoreConfig.isEnabled()) {
            throw new IllegalArgumentException("One of className, factoryClassName, storeImplementation, "
                    + "factoryImplementation has to be not null");
        }

        return new QueueStoreConfigHolder(queueStoreConfig.getClassName(),
                queueStoreConfig.getFactoryClassName(),
                serializationService.toData(queueStoreConfig.getStoreImplementation()),
                serializationService.toData(queueStoreConfig.getFactoryImplementation()),
                queueStoreConfig.getProperties(), queueStoreConfig.isEnabled());
    }

}
