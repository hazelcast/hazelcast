/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * A {@link java.util.Map.Entry Map.Entry} implementation which serializes/de-serializes key and value objects on demand.
 * It is beneficial when you need to prevent unneeded serialization/de-serialization
 * when creating a {@link java.util.Map.Entry Map.Entry}. Mainly targeted to supply a lazy entry to
 * {@link com.hazelcast.map.EntryProcessor#process(Map.Entry)} and
 * {@link com.hazelcast.map.EntryBackupProcessor#processBackup(Map.Entry)}} methods.
 * <p/>
 * <STRONG>Note that this implementation is not synchronized and is not thread-safe.</STRONG>
 *
 * LazyMapEntry itself is serializable as long as the object representations of both key and value are serializable.
 * After serialization objects are resolved using injected SerializationService. De-serialized LazyMapEntry
 * does contain object representation only Data representations and SerializationService is set to null. In other
 * words: It's as usable just as a regular Map.Entry.
 *
 * @see com.hazelcast.map.impl.operation.EntryOperation#createMapEntry(Data, Object)
 */

public class LazyMapEntry implements Map.Entry, Serializable {
    private static final long serialVersionUID = 0L;

    private transient Object keyObject;
    private transient Object valueObject;
    private transient Data keyData;
    private transient Data valueData;
    private transient boolean modified;
    private transient SerializationService serializationService;

    public LazyMapEntry() {
    }

    public LazyMapEntry(Object key, Object value, SerializationService serializationService) {
        init(key, value, serializationService);
    }

    public void init(Object key, Object value, SerializationService serializationService) {
        checkNotNull(key, "key cannot be null");
        checkNotNull(serializationService, "SerializationService cannot be null");

        keyData = null;
        keyObject = null;

        if (key instanceof Data) {
            this.keyData = (Data) key;
        } else {
            this.keyObject = key;
        }

        valueData = null;
        valueObject = null;

        if (value instanceof Data) {
            this.valueData = (Data) value;
        } else {
            this.valueObject = value;
        }

        this.serializationService = serializationService;
    }

    @Override
    public Object getKey() {
        if (keyObject == null) {
            keyObject = serializationService.toObject(keyData);
        }
        return keyObject;
    }


    @Override
    public Object getValue() {
        if (valueObject == null) {
            valueObject = serializationService.toObject(valueData);
        }
        return valueObject;
    }

    @Override
    public Object setValue(Object value) {
        modified = true;
        Object oldValue = getValue();
        this.valueObject = value;
        this.valueData = null;
        return oldValue;
    }

    public Object getKeyData() {
        if (keyData == null) {
            keyData = serializationService.toData(keyObject);
        }
        return keyData;
    }

    public Object getValueData() {
        if (valueData == null) {
            valueData = serializationService.toData(valueObject);
        }
        return valueData;
    }

    public boolean isModified() {
        return modified;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Map.Entry)) {
            return false;
        }
        Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
        return eq(getKey(), e.getKey()) && eq(getValue(), e.getValue());
    }

    private static boolean eq(Object o1, Object o2) {
        return o1 == null ? o2 == null : o1.equals(o2);
    }

    @Override
    public int hashCode() {
        return (getKey() == null ? 0 : getKey().hashCode())
                ^ (getValue() == null ? 0 : getValue().hashCode());
    }

    @Override
    public String toString() {
        return getKey() + "=" + getValue();
    }

    private void readObject(java.io.ObjectInputStream s) throws IOException, ClassNotFoundException {
        s.defaultReadObject();
        keyObject = s.readObject();
        valueObject = s.readObject();
    }

    private void writeObject(java.io.ObjectOutputStream s) throws IOException {
        s.defaultWriteObject();
        s.writeObject(getKey());
        s.writeObject(getValue());
    }
}

