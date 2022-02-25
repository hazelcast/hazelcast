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

import com.hazelcast.cache.impl.event.CachePartitionLostListener;
import com.hazelcast.collection.ItemListener;
import com.hazelcast.config.CachePartitionLostListenerConfig;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.ItemListenerConfig;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.MapPartitionLostListenerConfig;
import com.hazelcast.config.SplitBrainProtectionListenerConfig;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.map.listener.MapPartitionLostListener;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionListener;

import java.util.EventListener;

@SuppressWarnings("checkstyle:cyclomaticcomplexity")
public class ListenerConfigHolder {

    /**
     * Used to specify the type of the listener configuration.
     */
    public enum ListenerConfigType {

        /**
         * Not specific to any data structure or service.
         */
        GENERIC(0),

        /**
         * For {@link ItemListenerConfig}.
         */
        ITEM(1),

        /**
         * For {@link EntryListenerConfig}.
         */
        ENTRY(2),

        /**
         * For {@link SplitBrainProtectionListenerConfig}.
         */
        SPLIT_BRAIN_PROTECTION(3),

        /**
         * For {@link CachePartitionLostListenerConfig}.
         */
        CACHE_PARTITION_LOST(4),

        /**
         * For {@link MapPartitionLostListenerConfig}.
         */
        MAP_PARTITION_LOST(5);

        private static final ListenerConfigType[] CACHED_VALUES = values();

        private final int type;

        ListenerConfigType(int type) {
            this.type = type;
        }

        public int getType() {
            return type;
        }

        public static ListenerConfigType fromType(int type) {
            for (ListenerConfigType configType : CACHED_VALUES) {
                if (configType.type == type) {
                    return configType;
                }
            }

            throw new HazelcastSerializationException("Unrecognized listener type " + type);
        }
    }

    private final String className;
    private final Data listenerImplementation;
    private final boolean includeValue;
    private final boolean local;
    private final ListenerConfigType listenerType;

    public ListenerConfigHolder(int listenerType, Data listenerImplementation, String className,
                                boolean includeValue, boolean local) {
        this(ListenerConfigType.fromType(listenerType), listenerImplementation, className, includeValue, local);
    }

    public ListenerConfigHolder(ListenerConfigType listenerType, Data listenerImplementation, String className,
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
        return listenerType.getType();
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
                case GENERIC:
                    listenerConfig = new ListenerConfig(className);
                    break;
                case ITEM:
                    listenerConfig = new ItemListenerConfig(className, includeValue);
                    break;
                case ENTRY:
                    listenerConfig = new EntryListenerConfig(className, local, includeValue);
                    break;
                case SPLIT_BRAIN_PROTECTION:
                    listenerConfig = new SplitBrainProtectionListenerConfig(className);
                    break;
                case CACHE_PARTITION_LOST:
                    listenerConfig = new CachePartitionLostListenerConfig(className);
                    break;
                case MAP_PARTITION_LOST:
                    listenerConfig = new MapPartitionLostListenerConfig(className);
                    break;
                default:
                    // We shouldn't hit this line as the validity of the type ids are
                    // performed while constructing the listenerType. It is added to
                    // make checkstyle happy.
            }
        } else {
            EventListener eventListener = serializationService.toObject(listenerImplementation);
            switch (listenerType) {
                case GENERIC:
                    listenerConfig = new ListenerConfig(eventListener);
                    break;
                case ITEM:
                    listenerConfig = new ItemListenerConfig((ItemListener) eventListener, includeValue);
                    break;
                case ENTRY:
                    listenerConfig = new EntryListenerConfig((MapListener) eventListener, local, includeValue);
                    break;
                case SPLIT_BRAIN_PROTECTION:
                    listenerConfig = new SplitBrainProtectionListenerConfig((SplitBrainProtectionListener) eventListener);
                    break;
                case CACHE_PARTITION_LOST:
                    listenerConfig = new CachePartitionLostListenerConfig((CachePartitionLostListener) eventListener);
                    break;
                case MAP_PARTITION_LOST:
                    listenerConfig = new MapPartitionLostListenerConfig((MapPartitionLostListener) eventListener);
                    break;
                default:
                    // We shouldn't hit this line as the validity of the type ids are
                    // performed while constructing the listenerType. It is added to make
                    // checkstyle happy.
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
        ListenerConfigType listenerType = listenerTypeOf(config);
        Data implementationData = null;
        if (config.getImplementation() != null) {
            implementationData = serializationService.toData(config.getImplementation());
        }
        return new ListenerConfigHolder(listenerType, implementationData, config.getClassName(), config.isIncludeValue(),
                config.isLocal());
    }

    private static ListenerConfigType listenerTypeOf(ListenerConfig config) {
        if (config instanceof ItemListenerConfig) {
            return ListenerConfigType.ITEM;
        } else if (config instanceof CachePartitionLostListenerConfig) {
            return ListenerConfigType.CACHE_PARTITION_LOST;
        } else if (config instanceof SplitBrainProtectionListenerConfig) {
            return ListenerConfigType.SPLIT_BRAIN_PROTECTION;
        } else if (config instanceof EntryListenerConfig) {
            return ListenerConfigType.ENTRY;
        } else if (config instanceof MapPartitionLostListenerConfig) {
            return ListenerConfigType.MAP_PARTITION_LOST;
        } else {
            return ListenerConfigType.GENERIC;
        }
    }
}
