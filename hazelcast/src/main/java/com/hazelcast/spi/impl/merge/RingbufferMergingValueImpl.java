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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.merge.RingbufferMergeData;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.RingbufferMergeTypes;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.SerializationServiceAware;

import java.io.IOException;

import static com.hazelcast.internal.nio.IOUtil.readObject;
import static com.hazelcast.internal.nio.IOUtil.writeObject;

/**
 * Implementation of {@link RingbufferMergeTypes}.
 * The class will hold a ringbuffer of items with the type of the
 *
 * @since 3.10
 */
@SuppressWarnings("WeakerAccess")
public class RingbufferMergingValueImpl
        implements RingbufferMergeTypes, SerializationServiceAware, IdentifiedDataSerializable {

    private RingbufferMergeData value;

    private transient SerializationService serializationService;

    public RingbufferMergingValueImpl() {
    }

    public RingbufferMergingValueImpl(SerializationService serializationService) {
        this.serializationService = serializationService;
    }

    @Override
    public RingbufferMergeData getRawValue() {
        return value;
    }

    @Override
    public RingbufferMergeData getValue() {
        final RingbufferMergeData deserializedValues = new RingbufferMergeData(value.getItems().length);
        deserializedValues.setHeadSequence(value.getHeadSequence());
        deserializedValues.setTailSequence(value.getTailSequence());

        for (long seq = value.getHeadSequence(); seq <= value.getTailSequence(); seq++) {
            deserializedValues.set(seq, serializationService.toObject(value.read(seq)));
        }

        return deserializedValues;
    }

    public RingbufferMergingValueImpl setValues(RingbufferMergeData values) {
        this.value = values;
        return this;
    }

    @Override
    public void setSerializationService(SerializationService serializationService) {
        this.serializationService = serializationService;
    }


    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(value.getTailSequence());
        out.writeLong(value.getHeadSequence());
        out.writeInt(value.getItems().length);
        for (long seq = value.getHeadSequence(); seq <= value.getTailSequence(); seq++) {
            writeObject(out, value.read(seq));
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        final long tailSequence = in.readLong();
        final long headSequence = in.readLong();
        final int capacity = in.readInt();
        value = new RingbufferMergeData(capacity);
        value.setTailSequence(tailSequence);
        value.setHeadSequence(headSequence);

        for (long seq = headSequence; seq <= tailSequence; seq++) {
            value.set(seq, readObject(in));
        }
    }

    @Override
    public int getFactoryId() {
        return SplitBrainDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SplitBrainDataSerializerHook.RINGBUFFER_MERGING_ENTRY;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RingbufferMergingValueImpl that = (RingbufferMergingValueImpl) o;

        return value != null ? value.equals(that.value) : that.value == null;
    }

    @Override
    public int hashCode() {
        return value != null ? value.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "RingbufferMergingValueImpl{value=" + value + '}';
    }
}
