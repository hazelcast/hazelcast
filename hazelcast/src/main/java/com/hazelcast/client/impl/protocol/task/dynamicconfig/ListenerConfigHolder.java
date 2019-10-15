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

import com.hazelcast.cache.impl.event.CachePartitionLostListener;
import com.hazelcast.collection.ItemListener;
import com.hazelcast.config.CachePartitionLostListenerConfig;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.ItemListenerConfig;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.MapPartitionLostListenerConfig;
import com.hazelcast.config.SplitBrainProtectionListenerConfig;
import com.hazelcast.core.EntryListener;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.listener.MapPartitionLostListener;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionListener;

import java.util.EventListener;

@SuppressWarnings("checkstyle:cyclomaticcomplexity")
public class ListenerConfigHolder {

    public static final int TYPE_LISTENER_CONFIG = 0;
    public static final int TYPE_ITEM_LISTENER_CONFIG = 1;
    public static final int TYPE_ENTRY_LISTENER_CONFIG = 2;
    public static final int TYPE_SPLIT_BRAIN_PROTECTION_LISTENER_CONFIG = 3;
    public static final int TYPE_CACHE_PARTITION_LOST_LISTENER_CONFIG = 4;
    public static final int TYPE_MAP_PARTITION_LOST_LISTENER_CONFIG = 5;

    private final String className;
    private final Data listenerImplementation;
    private final boolean includeValue;
    private final boolean local;
    private final int listenerType;

    public ListenerConfigHolder(int listenerType, String className) {
        this(listenerType, null, className, true, false);
    }

    public ListenerConfigHolder(int listenerType, Data listenerImplementation) {
        this(listenerType, listenerImplementation, null, true, false);
    }

    public ListenerConfigHolder(int listenerType, Data listenerImplementation, String className,
                                boolean includeValue, boolean local) {
        this.listenerType = listenerType;
        this.className = className;
        this.listenerImplementation = listenerImplementation;
        this.includeValue = includeValue;
        this.local = local;
    }

    public String getClassName() {
        return className;
    }

    public Data getListenerImplementation() {
        return listenerImplementation;
    }

    public int getListenerType() {
        return listenerType;
    }

    public boolean isIncludeValue() {
        return includeValue;
    }

    public boolean isLocal() {
        return local;
    }

    public <T extends ListenerConfig> T asListenerConfig(SerializationService serializationService) {
        validate();
        ListenerConfig listenerConfig = null;
        if (className != null) {
            switch (listenerType) {
                case TYPE_LISTENER_CONFIG:
                    listenerConfig = new ListenerConfig(className);
                    break;
                case TYPE_ITEM_LISTENER_CONFIG:
                    listenerConfig = new ItemListenerConfig(className, includeValue);
                    break;
                case TYPE_ENTRY_LISTENER_CONFIG:
                    listenerConfig = new EntryListenerConfig(className, local, includeValue);
                    break;
                case TYPE_SPLIT_BRAIN_PROTECTION_LISTENER_CONFIG:
                    listenerConfig = new SplitBrainProtectionListenerConfig(className);
                    break;
                case TYPE_CACHE_PARTITION_LOST_LISTENER_CONFIG:
                    listenerConfig = new CachePartitionLostListenerConfig(className);
                    break;
                case TYPE_MAP_PARTITION_LOST_LISTENER_CONFIG:
                    listenerConfig = new MapPartitionLostListenerConfig(className);
                    break;
                default:
                    throw new HazelcastSerializationException("Unrecognized listener type " + listenerConfig);
            }
        } else {
            EventListener eventListener = serializationService.toObject(listenerImplementation);
            switch (listenerType) {
                case TYPE_LISTENER_CONFIG:
                    listenerConfig = new ListenerConfig(eventListener);
                    break;
                case TYPE_ITEM_LISTENER_CONFIG:
                    listenerConfig = new ItemListenerConfig((ItemListener) eventListener, includeValue);
                    break;
                case TYPE_ENTRY_LISTENER_CONFIG:
                    listenerConfig = new EntryListenerConfig((EntryListener) eventListener, local, includeValue);
                    break;
                case TYPE_SPLIT_BRAIN_PROTECTION_LISTENER_CONFIG:
                    listenerConfig = new SplitBrainProtectionListenerConfig((SplitBrainProtectionListener) eventListener);
                    break;
                case TYPE_CACHE_PARTITION_LOST_LISTENER_CONFIG:
                    listenerConfig = new CachePartitionLostListenerConfig((CachePartitionLostListener) eventListener);
                    break;
                case TYPE_MAP_PARTITION_LOST_LISTENER_CONFIG:
                    listenerConfig = new MapPartitionLostListenerConfig((MapPartitionLostListener) eventListener);
                    break;
                default:
                    throw new HazelcastSerializationException("Unrecognized listener type " + listenerConfig);
            }
        }
        return (T) listenerConfig;
    }

    void validate() {
        if (className == null && listenerImplementation == null) {
            throw new IllegalArgumentException("Either class name or listener implementation must be not null");
        }
    }

    public static ListenerConfigHolder of(ListenerConfig config, SerializationService serializationService) {
        int listenerType = listenerTypeOf(config);
        Data implementationData = null;
        if (config.getImplementation() != null) {
            implementationData = serializationService.toData(config.getImplementation());
        }
        return new ListenerConfigHolder(listenerType, implementationData, config.getClassName(), config.isIncludeValue(),
                config.isLocal());
    }

    private static int listenerTypeOf(ListenerConfig config) {
        if (config instanceof ItemListenerConfig) {
            return TYPE_ITEM_LISTENER_CONFIG;
        } else if (config instanceof CachePartitionLostListenerConfig) {
            return TYPE_CACHE_PARTITION_LOST_LISTENER_CONFIG;
        } else if (config instanceof SplitBrainProtectionListenerConfig) {
            return TYPE_SPLIT_BRAIN_PROTECTION_LISTENER_CONFIG;
        } else if (config instanceof EntryListenerConfig) {
            return TYPE_ENTRY_LISTENER_CONFIG;
        } else if (config instanceof MapPartitionLostListenerConfig) {
            return TYPE_MAP_PARTITION_LOST_LISTENER_CONFIG;
        } else {
            return TYPE_LISTENER_CONFIG;
        }
    }
}
