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

package com.hazelcast.ringbuffer.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IFunction;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.projection.Projection;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.spi.impl.SerializationServiceSupport;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.AbstractList;
import java.util.List;
import java.util.function.Predicate;

import static com.hazelcast.ringbuffer.impl.RingbufferDataSerializerHook.F_ID;
import static com.hazelcast.ringbuffer.impl.RingbufferDataSerializerHook.READ_RESULT_SET;

/**
 * A list for the {@link com.hazelcast.ringbuffer.impl.operations.ReadManyOperation}.
 * <p>
 * The problem with a regular list is that if you store Data objects, then
 * on the receiving side you get a list with data objects. If you hand this
 * list out to the caller, you have a problem because he sees data objects
 * instead of deserialized objects.
 * The predicate, filter and projection may be {@code null} in which case
 * all elements are returned and no projection is applied.
 *
 * @param <O> deserialized ringbuffer type
 * @param <E> result set type, is equal to {@code O} if the projection
 *            is {@code null} or returns the same type as the parameter
 */
public class ReadResultSetImpl<O, E> extends AbstractList<E>
        implements IdentifiedDataSerializable, HazelcastInstanceAware, ReadResultSet<E> {

    protected transient SerializationService serializationService;
    private transient int minSize;
    private transient int maxSize;
    private transient IFunction<O, Boolean> filter;
    private transient Predicate<? super O> predicate;
    private transient Projection<? super O, E> projection;

    private Data[] items;
    private long[] seqs;
    private int size;
    private int readCount;
    private long nextSeq;

    public ReadResultSetImpl() {
    }

    public ReadResultSetImpl(int minSize, int maxSize,
                             SerializationService serializationService,
                             IFunction<O, Boolean> filter) {
        this.minSize = minSize;
        this.maxSize = maxSize;
        this.serializationService = serializationService;
        this.filter = filter;
    }

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public ReadResultSetImpl(int readCount, List<Data> items, long[] seqs, long nextSeq) {
        this.readCount = readCount;
        this.items = items.toArray(new Data[0]);
        this.size = items.size();
        this.seqs = seqs;
        this.nextSeq = nextSeq;
    }

    public ReadResultSetImpl(int minSize, int maxSize,
                             SerializationService serializationService,
                             Predicate<? super O> predicate,
                             Projection<? super O, E> projection) {
        this(minSize, maxSize, serializationService, null);
        this.predicate = predicate;
        this.projection = projection;
    }

    public boolean isMaxSizeReached() {
        return size == maxSize;
    }

    public boolean isMinSizeReached() {
        return size >= minSize;
    }

    @SuppressFBWarnings("EI_EXPOSE_REP")
    public Data[] getDataItems() {
        return items;
    }

    @Override
    public int readCount() {
        return readCount;
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hz) {
        setSerializationService(((SerializationServiceSupport) hz).getSerializationService());
    }

    public void setSerializationService(SerializationService serializationService) {
        this.serializationService = serializationService;
    }

    @Override
    public E get(int index) {
        rangeCheck(index);
        final Data item = items[index];
        return serializationService.toObject(item);
    }

    @Override
    public long getSequence(int index) {
        rangeCheck(index);
        return seqs.length > index ? seqs[index] : -1;
    }

    private void rangeCheck(int index) {
        if (index < 0 || index >= size) {
            throw new IllegalArgumentException("index=" + index + ", size=" + size);
        }
    }

    /**
     * Applies the {@link Projection} and adds an item to this {@link ReadResultSetImpl} if
     * it passes the {@link Predicate} and {@link IFunction} with which it was constructed.
     * The {@code item} may be in serialized or deserialized format as this method will
     * adapt the parameter if necessary before providing it to the predicate and projection.
     * <p>
     * If the {@code item} is in {@link Data} format and there is no filter, predicate or projection,
     * the item is added to the set without any additional serialization or deserialization.
     *
     * @param seq  the sequence ID of the item
     * @param item the item to add to the result set
     */
    public void addItem(long seq, Object item) {
        assert size < maxSize;
        readCount++;

        Data resultItem;
        if (filter != null || predicate != null || projection != null) {
            final O objectItem = serializationService.toObject(item);
            final boolean passesFilter = filter == null || filter.apply(objectItem);
            final boolean passesPredicate = predicate == null || predicate.test(objectItem);
            if (!passesFilter || !passesPredicate) {
                return;
            }
            if (projection != null) {
                resultItem = serializationService.toData(projection.transform(objectItem));
            } else {
                resultItem = serializationService.toData(item);
            }
        } else {
            resultItem = serializationService.toData(item);
        }

        // lazily create item and seqs arrays
        if (items == null) {
            items = new Data[maxSize];
            seqs = new long[maxSize];
        }

        items[size] = resultItem;
        seqs[size] = seq;
        size++;
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
    public int getClassId() {
        return READ_RESULT_SET;
    }

    @Override
    public long getNextSequenceToReadFrom() {
        return nextSeq;
    }

    public void setNextSequenceToReadFrom(long nextSeq) {
        this.nextSeq = nextSeq;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(readCount);
        out.writeInt(size);
        for (int k = 0; k < size; k++) {
            IOUtil.writeData(out, items[k]);
        }
        out.writeLongArray(seqs);
        out.writeLong(nextSeq);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        readCount = in.readInt();
        size = in.readInt();
        items = new Data[size];
        for (int k = 0; k < size; k++) {
            items[k] = IOUtil.readData(in);
        }
        seqs = in.readLongArray();
        nextSeq = in.readLong();
    }
}
