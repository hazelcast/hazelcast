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

package com.hazelcast.ringbuffer.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IFunction;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.ringbuffer.ReadResultSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.AbstractList;

import static com.hazelcast.ringbuffer.impl.RingbufferDataSerializerHook.F_ID;
import static com.hazelcast.ringbuffer.impl.RingbufferDataSerializerHook.READ_RESULT_SET;
import static com.hazelcast.util.Preconditions.checkNotNegative;
import static com.hazelcast.util.Preconditions.checkTrue;

/**
 * A list for the {@link com.hazelcast.ringbuffer.impl.operations.ReadManyOperation}.
 *
 * The problem with a regular list is that if you store Data objects, then on the receiving side you get
 * a list with data objects. If you hand this list out to the caller, you have a problem because he sees
 * data objects instead of deserialized objects.
 *
 * @param <E>
 */
public class ReadResultSetImpl<E> extends AbstractList<E>
        implements IdentifiedDataSerializable, HazelcastInstanceAware, ReadResultSet<E> {

    private transient int minSize;
    private transient int maxSize;
    private transient IFunction<Object, Boolean> filter;
    private transient HazelcastInstance hz;

    private Data[] items;
    private int size;
    private int readCount;

    public ReadResultSetImpl() {
    }

    public ReadResultSetImpl(int minSize, int maxSize, HazelcastInstance hz, IFunction<Object, Boolean> filter) {
        this.minSize = minSize;
        this.maxSize = maxSize;
        this.items = new Data[maxSize];
        this.hz = hz;
        this.filter = filter;
    }

    public boolean isMaxSizeReached() {
        return size == maxSize;
    }

    public boolean isMinSizeReached() {
        return size >= minSize;
    }

    @SuppressFBWarnings({"EI_EXPOSE_REP" })
    public Data[] getItems() {
        return items;
    }

    @Override
    public int readCount() {
        return readCount;
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hz) {
        this.hz = hz;
    }

    @Override
    public E get(int index) {
        checkNotNegative(index, "index should not be negative");
        checkTrue(index < size, "index should not be equal or larger than size");

        SerializationService serializationService = getSerializationService();

        Data item = items[index];
        return serializationService.toObject(item);
    }

    private SerializationService getSerializationService() {
        HazelcastInstanceImpl impl = (HazelcastInstanceImpl) hz;
        return impl.getSerializationService();
    }

    public void addItem(Object item) {
        assert size < maxSize;

        readCount++;

        if (!acceptable(item)) {
            return;
        }

        items[size] = getSerializationService().toData(item);
        size++;
    }

    private boolean acceptable(Object item) {
        if (filter == null) {
            return true;
        }

        Object object = getSerializationService().toObject(item);
        return filter.apply(object);
    }

    @Override
    public boolean add(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public int getId() {
        return READ_RESULT_SET;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(readCount);
        out.writeInt(size);
        for (int k = 0; k < size; k++) {
            out.writeData(items[k]);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        readCount = in.readInt();
        size = in.readInt();
        items = new Data[size];
        for (int k = 0; k < size; k++) {
            items[k] = in.readData();
        }
    }
}
