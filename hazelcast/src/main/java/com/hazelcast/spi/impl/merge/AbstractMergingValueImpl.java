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

/**
 * Implementation of {@link MergingValue}.
 *
 * @param <V> the type of value
 * @since 3.10
 */
@SuppressWarnings("WeakerAccess")
public abstract class AbstractMergingValueImpl<V, T extends AbstractMergingValueImpl<V, T>>
        implements MergingValue<V>, SerializationServiceAware, IdentifiedDataSerializable {

    private Object value;

    private transient SerializationService serializationService;

    public AbstractMergingValueImpl() {
    }

    public AbstractMergingValueImpl(SerializationService serializationService) {
        this.serializationService = serializationService;
    }

    @Override
    public Object getRawValue() {
        return value;
    }

    @Override
    public V getValue() {
        return serializationService.toObject(value);
    }

    public T setValue(Object value) {
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
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AbstractMergingValueImpl)) {
            return false;
        }

        AbstractMergingValueImpl<?, ?> that = (AbstractMergingValueImpl<?, ?>) o;
        return value != null ? value.equals(that.value) : that.value == null;
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
