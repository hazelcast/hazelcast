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

package com.hazelcast.query.impl;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.compact.CompactGenericRecord;
import com.hazelcast.internal.serialization.impl.portable.PortableGenericRecord;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.query.impl.getters.Extractors;

import java.io.IOException;

/**
 * Entry of the Query.
 *
 * @param <K> key
 * @param <V> value
 */
public class CachedQueryEntry<K, V> extends QueryableEntry<K, V> implements IdentifiedDataSerializable {

    protected Data keyData;
    protected Data valueData;

    protected K keyObject;
    protected V valueObject;

    public CachedQueryEntry() {
    }

    public CachedQueryEntry(SerializationService ss, Object key, Object value, Extractors extractors) {
        init(ss, key, value, extractors);
    }

    public CachedQueryEntry(SerializationService ss, Extractors extractors) {
        this.serializationService = (InternalSerializationService) ss;
        this.extractors = extractors;
    }

    public CachedQueryEntry<K, V> init(SerializationService ss, Object key, Object value, Extractors extractors) {
        this.serializationService = (InternalSerializationService) ss;
        this.extractors = extractors;
        return init(key, value);
    }

    @SuppressWarnings("unchecked")
    public CachedQueryEntry<K, V> init(Object key, Object value) {
        if (key == null) {
            throw new IllegalArgumentException("keyData cannot be null");
        }

        if (key instanceof Data) {
            this.keyData = (Data) key;
            this.keyObject = null;
        } else {
            this.keyObject = (K) key;
            this.keyData = null;
        }

        if (value instanceof Data) {
            this.valueData = (Data) value;
            this.valueObject = null;
        } else {
            this.valueObject = (V) value;
            this.valueData = null;
        }

        return this;
    }

    @SuppressWarnings("unchecked")
    public CachedQueryEntry<K, V> initWithObjectKeyValue(Object key, Object value) {
        this.keyObject = (K) key;
        this.keyData = null;
        this.valueObject = (V) value;
        this.valueData = null;

        return this;
    }

    @Override
    public K getKey() {
        if (keyObject == null) {
            keyObject = serializationService.toObject(keyData);
        }
        return keyObject;
    }

    @Override
    public Data getKeyData() {
        return keyData;
    }

    @Override
    public V getValue() {
        if (valueObject == null) {
            valueObject = serializationService.toObject(valueData);
        }
        return valueObject;
    }

    @Override
    public Data getValueData() {
        if (valueData == null) {
            valueData = serializationService.toData(valueObject);
        }
        return valueData;
    }

    @Override
    public K getKeyIfPresent() {
        return keyObject != null ? keyObject : null;
    }

    @Override
    public Data getKeyDataIfPresent() {
        return keyData;
    }

    @SuppressWarnings("unchecked")
    @Override
    public V getValueIfPresent() {
        if (valueObject != null) {
            return valueObject;
        }

        if (record == null) {
            return null;
        }

        Object possiblyNotData = record.getValue();

        return possiblyNotData instanceof Data ? null : (V) possiblyNotData;
    }

    @Override
    public Data getValueDataIfPresent() {
        if (valueData != null) {
            return valueData;
        }

        if (record == null) {
            return null;
        }

        Object possiblyData = record.getValue();

        return possiblyData instanceof Data ? (Data) possiblyData : null;
    }

    public Object getByPrioritizingDataValue() {
        if (valueData != null) {
            return valueData;
        }

        if (valueObject != null) {
            return valueObject;
        }

        return null;
    }

    public Object getByPrioritizingObjectValue() {
        if (valueObject != null) {
            return valueObject;
        }

        if (valueData != null) {
            return valueData;
        }

        return null;
    }

    @Override
    protected Object getTargetObject(boolean key) {
        Object targetObject;
        if (key) {
            // keyData is never null
            if (keyData.isPortable() || keyData.isJson() || keyData.isCompact()) {
                targetObject = keyData;
            } else {
                targetObject = getKey();
            }
        } else {
            if (valueObject == null) {
                targetObject = getTargetObjectFromData();
            } else {
                if (valueObject instanceof PortableGenericRecord
                        || valueObject instanceof CompactGenericRecord) {
                    // These two classes should be able to be handled by respective Getters
                    // see PortableGetter and CompactGetter
                    // We get into this branch when in memory format is Object and
                    // - the cluster does not have PortableFactory configuration for Portable
                    // - the cluster does not related classes for Compact
                    targetObject = getValue();
                } else if (valueObject instanceof Portable
                        || serializationService.isCompactSerializable(valueObject)) {
                    targetObject = getValueData();
                } else {
                    // Note that targetObject can be PortableGenericRecord
                    // and it will be handled with PortableGetter for query.
                    // We get PortableGenericRecord here when in-memory format is OBJECT and
                    // the cluster does not have PortableFactory configuration for the object's factory ID
                    targetObject = getValue();
                }
            }
        }
        return targetObject;
    }

    private Object getTargetObjectFromData() {
        if (valueData == null) {
            // Query Cache depends on this behaviour when its caching of
            // values is off.
            return null;
        } else if (valueData.isPortable() || valueData.isJson() || valueData.isCompact()) {
            return valueData;
        } else {
            return getValue();
        }
    }

    @Override
    public V setValue(V value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CachedQueryEntry that = (CachedQueryEntry) o;
        return keyData.equals(that.keyData);
    }

    @Override
    public int hashCode() {
        return keyData.hashCode();
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(getKey());
        out.writeObject(getValue());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        keyObject = in.readObject();
        valueObject = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        // We are intentionally deserializing CachedQueryEntry as LazyMapEntry
        // LazyMapEntry is actually a subclass of CacheQueryEntry.
        // If this sounds surprising, convoluted or just plain wrong
        // then you are not wrong. Please see commit message for reasoning.
        return MapDataSerializerHook.LAZY_MAP_ENTRY;
    }
}
