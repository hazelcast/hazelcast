/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.query.impl.getters.Extractors;

/**
 * Entry of the Query.
 *
 * @param <K> key
 * @param <V> value
 */
public class CachedQueryEntry<K, V> extends QueryableEntry<K, V> {

    protected Data keyData;
    protected Data valueData;

    protected K keyObject;
    protected V valueObject;

    public CachedQueryEntry() {
    }

    public CachedQueryEntry(InternalSerializationService ss,
                            Data key, Object value, Extractors extractors) {
        init(ss, key, value, extractors);
    }

    @SuppressWarnings("unchecked")
    public CachedQueryEntry<K, V> init(InternalSerializationService ss,
                                       Data key, Object value, Extractors extractors) {
        if (key == null) {
            throw new IllegalArgumentException("keyData cannot be null");
        }
        this.serializationService = ss;
        this.keyData = key;
        this.keyObject = null;

        if (value instanceof Data) {
            this.valueData = (Data) value;
            this.valueObject = null;
        } else {
            this.valueObject = (V) value;
            this.valueData = null;
        }
        this.extractors = extractors;
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
    public V getValue() {
        if (valueObject == null) {
            valueObject = serializationService.toObject(valueData);
        }
        return valueObject;
    }

    @Override
    public Data getKeyData() {
        return keyData;
    }

    @Override
    public Data getValueData() {
        if (valueData == null) {
            valueData = serializationService.toData(valueObject);
        }
        return valueData;
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

    @Override
    protected Object getTargetObject(boolean key) {
        Object targetObject;
        if (key) {
            // keyData is never null
            if (keyData.isPortable() || keyData.isJson()) {
                targetObject = keyData;
            } else {
                targetObject = getKey();
            }
        } else {
            if (valueObject == null) {
                if (valueData.isPortable() || valueData.isJson()) {
                    targetObject = valueData;
                } else {
                    targetObject = getValue();
                }
            } else {
                if (valueObject instanceof Portable) {
                    targetObject = getValueData();
                } else {
                    targetObject = getValue();
                }
            }
        }
        return targetObject;
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

}
