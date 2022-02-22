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

package com.hazelcast.client.impl.protocol.task.dynamicconfig;

import com.hazelcast.client.impl.protocol.util.PropertiesUtil;
import com.hazelcast.config.RingbufferStoreConfig;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.ringbuffer.RingbufferStore;
import com.hazelcast.ringbuffer.RingbufferStoreFactory;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;

import java.util.Map;

/**
 * Adapter for using {@link com.hazelcast.config.RingbufferStoreConfig} in client protocol.
 */
public class RingbufferStoreConfigHolder {

    private final String className;
    private final String factoryClassName;
    private final Data implementation;
    private final Data factoryImplementation;
    private final Map<String, String> properties;
    private final boolean enabled;

    public RingbufferStoreConfigHolder(String className, String factoryClassName, Data implementation,
                                       Data factoryImplementation, Map<String, String> properties,
                                       boolean enabled) {
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

    public Map<String, String> getProperties() {
        return properties;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public RingbufferStoreConfig asRingbufferStoreConfig(SerializationService serializationService) {
        RingbufferStoreConfig config = new RingbufferStoreConfig();
        if (!StringUtil.isNullOrEmptyAfterTrim(className)) {
            config.setClassName(className);
        }
        config.setEnabled(enabled);
        if (!StringUtil.isNullOrEmptyAfterTrim(factoryClassName)) {
            config.setFactoryClassName(factoryClassName);
        }
        config.setProperties(PropertiesUtil.fromMap(properties));
        RingbufferStore storeImplementation = serializationService.toObject(implementation);
        if (storeImplementation != null) {
            config.setStoreImplementation(storeImplementation);
        }
        RingbufferStoreFactory storeFactoryImplementation = serializationService.toObject(factoryImplementation);
        if (storeFactoryImplementation != null) {
            config.setFactoryImplementation(storeFactoryImplementation);
        }
        return config;
    }

    public static RingbufferStoreConfigHolder of(RingbufferStoreConfig ringbufferStoreConfig,
                                                 SerializationService serializationService) {
        if (ringbufferStoreConfig.getClassName() == null && ringbufferStoreConfig.getFactoryClassName() == null
                && ringbufferStoreConfig.getStoreImplementation() == null
                && ringbufferStoreConfig.getFactoryImplementation() == null
                && ringbufferStoreConfig.isEnabled()) {
            throw new IllegalArgumentException("One of className, factoryClassName, storeImplementation, "
                    + "factoryImplementation has to be not null");
        }

        return new RingbufferStoreConfigHolder(ringbufferStoreConfig.getClassName(),
                ringbufferStoreConfig.getFactoryClassName(),
                serializationService.toData(ringbufferStoreConfig.getStoreImplementation()),
                serializationService.toData(ringbufferStoreConfig.getFactoryImplementation()),
                PropertiesUtil.toMap(ringbufferStoreConfig.getProperties()), ringbufferStoreConfig.isEnabled());
    }

}
