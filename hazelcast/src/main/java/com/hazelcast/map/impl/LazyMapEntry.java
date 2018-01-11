/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.impl.operation.EntryOperator;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.impl.CachedQueryEntry;
import com.hazelcast.query.impl.getters.Extractors;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

/**
 * A {@link java.util.Map.Entry Map.Entry} implementation which serializes/de-serializes key and value objects on demand.
 * It is beneficial when you need to prevent unneeded serialization/de-serialization
 * when creating a {@link java.util.Map.Entry Map.Entry}. Mainly targeted to supply a lazy entry to
 * {@link com.hazelcast.map.EntryProcessor#process(Map.Entry)} and
 * {@link com.hazelcast.map.EntryBackupProcessor#processBackup(Map.Entry)}} methods.
 * <p/>
 * <STRONG>Note that this implementation is not synchronized and is not thread-safe.</STRONG>
 * <p/>
 * LazyMapEntry itself is serializable as long as the object representations of both key and value are serializable.
 * After serialization objects are resolved using injected SerializationService. De-serialized LazyMapEntry
 * does contain object representation only Data representations and SerializationService is set to null. In other
 * words: It's as usable just as a regular Map.Entry.
 *
 * @param <K> key
 * @param <V> value
 * @see EntryOperator#createMapEntry(Data, Object, Boolean)
 */
public class LazyMapEntry<K, V> extends CachedQueryEntry<K, V> implements Serializable, IdentifiedDataSerializable {

    private static final long serialVersionUID = 0L;

    private transient boolean modified;

    public LazyMapEntry() {
    }

    public LazyMapEntry(Data key, Object value, InternalSerializationService serializationService) {
        this(key, value, serializationService, null);
    }

    public LazyMapEntry(Data key, Object value, InternalSerializationService serializationService, Extractors extractors) {
        init(serializationService, key, value, extractors);
    }

    @Override
    public V setValue(V value) {
        modified = true;
        V oldValue = getValue();
        this.valueObject = value;
        this.valueData = null;
        return oldValue;
    }

    /**
     * Similar to calling {@link #setValue} with null but doesn't return old-value hence no extra deserialization.
     */
    public void remove() {
        modified = true;
        valueObject = null;
        valueData = null;
    }

    /**
     * Checks if this entry has null value without any deserialization.
     *
     * @return true if value is null, otherwise returns false.
     */
    public boolean hasNullValue() {
        return valueObject == null && valueData == null;
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

    @SuppressWarnings("unchecked")
    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        keyObject = (K) in.readObject();
        valueObject = (V) in.readObject();
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        out.writeObject(getKey());
        out.writeObject(getValue());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        keyObject = in.readObject();
        valueObject = in.readObject();
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(getKey());
        out.writeObject(getValue());
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.LAZY_MAP_ENTRY;
    }
}
