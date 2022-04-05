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

package com.hazelcast.spi.impl.merge;

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.merge.MergingValue;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.SerializationServiceAware;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;

/**
 * Implementation of {@link MergingValue} for data structures with collections.
 *
 * @param <V> the type of value
 * @since 3.10
 */
@SuppressWarnings("WeakerAccess")
public abstract class AbstractCollectionMergingValueImpl<V, T extends AbstractCollectionMergingValueImpl<V, T>>
        implements MergingValue<Collection<V>>, SerializationServiceAware, IdentifiedDataSerializable {

    private Collection<Object> value;

    private transient SerializationService serializationService;

    public AbstractCollectionMergingValueImpl() {
    }

    public AbstractCollectionMergingValueImpl(SerializationService serializationService) {
        this.serializationService = serializationService;
    }

    @Override
    public Collection<Object> getRawValue() {
        return value;
    }

    @Override
    public Collection<V> getValue() {
        Collection<Object> deserializedValues = new ArrayList<>(value.size());
        for (Object aValue : value) {
            deserializedValues.add(serializationService.toObject(aValue));
        }
        //noinspection unchecked
        return (Collection<V>) deserializedValues;
    }

    public T setValue(Collection<Object> value) {
        this.value = value;
        //noinspection unchecked
        return (T) this;
    }

    @Override
    public void setSerializationService(SerializationService serializationService) {
        this.serializationService = serializationService;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(value.size());
        for (Object aValue : value) {
            IOUtil.writeObject(out, aValue);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        ArrayList<Object> list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            list.add(IOUtil.readObject(in));
        }
        value = list;
    }

    @Override
    public int getFactoryId() {
        return SplitBrainDataSerializerHook.F_ID;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AbstractCollectionMergingValueImpl)) {
            return false;
        }

        AbstractCollectionMergingValueImpl<?, ?> that = (AbstractCollectionMergingValueImpl<?, ?>) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return value != null ? value.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "MergingValue{"
                + "value=" + value
                + '}';
    }
}
