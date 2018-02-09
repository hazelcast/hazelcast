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

package com.hazelcast.spi.impl.merge;

import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.merge.MergingValueHolder;
import com.hazelcast.spi.serialization.SerializationService;

import java.io.IOException;

/**
 * Implementation of {@link MergingValueHolder}.
 *
 * @param <V> the type of value
 * @since 3.10
 */
public class MergingValueHolderImpl<V> implements MergingValueHolder<V>, IdentifiedDataSerializable {

    private V value;

    private transient SerializationService serializationService;

    @Override
    public V getValue() {
        return value;
    }

    @Override
    public Object getDeserializedValue() {
        return serializationService.toObject(value);
    }

    public MergingValueHolderImpl<V> setValue(V value) {
        this.value = value;
        return this;
    }

    @Override
    public void setSerializationService(SerializationService serializationService) {
        this.serializationService = serializationService;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        IOUtil.writeObject(out, value);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        value = IOUtil.readObject(in);
    }

    @Override
    public int getFactoryId() {
        return SplitBrainDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return SplitBrainDataSerializerHook.MERGE_DATA_HOLDER;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MergingValueHolderImpl)) {
            return false;
        }

        MergingValueHolderImpl<?> that = (MergingValueHolderImpl<?>) o;
        return value != null ? value.equals(that.value) : that.value == null;
    }

    @Override
    public int hashCode() {
        return value != null ? value.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "MergeDataHolder{"
                + "value=" + value
                + '}';
    }
}
