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
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MapStoreConfig.InitialLoadMode;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.StringUtil;

import java.util.Map;

/**
 * Client protocol adapter for {@link com.hazelcast.config.MapStoreConfig}
 */
public class MapStoreConfigHolder {

    private boolean enabled;
    private boolean writeCoalescing;
    private String className;
    private String factoryClassName;
    private int writeDelaySeconds;
    private int writeBatchSize;
    private Data implementation;
    private Data factoryImplementation;
    private Map<String, String> properties;
    private String initialLoadMode;

    public MapStoreConfigHolder() {
    }

    public MapStoreConfigHolder(boolean enabled, boolean writeCoalescing, int writeDelaySeconds, int writeBatchSize,
                                String className, Data implementation, String factoryClassName,
                                Data factoryImplementation, Map<String, String> properties, String initialLoadMode) {
        this.enabled = enabled;
        this.writeCoalescing = writeCoalescing;
        this.className = className;
        this.factoryClassName = factoryClassName;
        this.writeDelaySeconds = writeDelaySeconds;
        this.writeBatchSize = writeBatchSize;
        this.implementation = implementation;
        this.factoryImplementation = factoryImplementation;
        this.properties = properties;
        this.initialLoadMode = initialLoadMode;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean isWriteCoalescing() {
        return writeCoalescing;
    }

    public void setWriteCoalescing(boolean writeCoalescing) {
        this.writeCoalescing = writeCoalescing;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getFactoryClassName() {
        return factoryClassName;
    }

    public void setFactoryClassName(String factoryClassName) {
        this.factoryClassName = factoryClassName;
    }

    public int getWriteDelaySeconds() {
        return writeDelaySeconds;
    }

    public void setWriteDelaySeconds(int writeDelaySeconds) {
        this.writeDelaySeconds = writeDelaySeconds;
    }

    public int getWriteBatchSize() {
        return writeBatchSize;
    }

    public void setWriteBatchSize(int writeBatchSize) {
        this.writeBatchSize = writeBatchSize;
    }

    public Data getImplementation() {
        return implementation;
    }

    public void setImplementation(Data implementation) {
        this.implementation = implementation;
    }

    public Data getFactoryImplementation() {
        return factoryImplementation;
    }

    public void setFactoryImplementation(Data factoryImplementation) {
        this.factoryImplementation = factoryImplementation;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public String getInitialLoadMode() {
        return initialLoadMode;
    }

    public void setInitialLoadMode(String initialLoadMode) {
        this.initialLoadMode = initialLoadMode;
    }

    public MapStoreConfig asMapStoreConfig(SerializationService serializationService) {
        MapStoreConfig config = new MapStoreConfig();
        if (!StringUtil.isNullOrEmptyAfterTrim(className)) {
            config.setClassName(className);
        }
        config.setEnabled(enabled);
        if (!StringUtil.isNullOrEmptyAfterTrim(factoryClassName)) {
            config.setFactoryClassName(factoryClassName);
        }
        config.setInitialLoadMode(InitialLoadMode.valueOf(initialLoadMode));
        if (properties != null) {
            config.setProperties(PropertiesUtil.fromMap(properties));
        }
        config.setWriteBatchSize(writeBatchSize);
        config.setWriteCoalescing(writeCoalescing);
        config.setWriteDelaySeconds(writeDelaySeconds);
        Object implementation = serializationService.toObject(this.implementation);
        if (implementation != null) {
            config.setImplementation(implementation);
        }
        Object factoryImplementation = serializationService.toObject(this.factoryImplementation);
        if (factoryImplementation != null) {
            config.setFactoryImplementation(factoryImplementation);
        }
        return config;
    }

    public static MapStoreConfigHolder of(MapStoreConfig config, SerializationService serializationService) {
        if (config == null) {
            return null;
        }
        MapStoreConfigHolder holder = new MapStoreConfigHolder();
        holder.setClassName(config.getClassName());
        holder.setEnabled(config.isEnabled());
        holder.setFactoryClassName(config.getFactoryClassName());
        holder.setFactoryImplementation(serializationService.toData(config.getFactoryImplementation()));
        holder.setImplementation(serializationService.toData(config.getImplementation()));
        holder.setInitialLoadMode(config.getInitialLoadMode().name());
        holder.setProperties(PropertiesUtil.toMap(config.getProperties()));
        holder.setWriteBatchSize(config.getWriteBatchSize());
        holder.setWriteCoalescing(config.isWriteCoalescing());
        holder.setWriteDelaySeconds(config.getWriteDelaySeconds());
        return holder;
    }
}
