/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.ringbuffer;

import com.hazelcast.jet.runtime.InputChunk;
import com.hazelcast.jet.runtime.OutputCollector;
import com.hazelcast.jet.strategy.DataTransferringStrategy;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Arrays;
import java.util.Iterator;

public class IOBuffer<T> implements InputChunk<T>, OutputCollector<T> {
    private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE;

    protected T[] buffer;

    private final Iterator<T> iterator = new DataIterator();
    private final DataTransferringStrategy dataTransferringStrategy;
    private int size;
    private int currentIdx;

    public IOBuffer(T[] buffer) {
        this(buffer, ByReferenceDataTransferringStrategy.INSTANCE);
    }

    @SuppressFBWarnings("EI_EXPOSE_REP")
    public IOBuffer(T[] buffer,
                    DataTransferringStrategy dataTransferringStrategy) {
        this.buffer = buffer;
        this.dataTransferringStrategy = dataTransferringStrategy;
        initBuffer();
    }

    private void initBuffer() {
        if (!dataTransferringStrategy.byReference()) {
            for (int i = 0; i < buffer.length; i++) {
                if (buffer[i] == null) {
                    buffer[i] = (T) dataTransferringStrategy.newInstance();
                }
            }
        }
    }

    @Override
    public T get(int idx) {
        return idx < buffer.length ? buffer[idx] : null;
    }

    @Override
    public Iterator<T> iterator() {
        currentIdx = 0;
        return iterator;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public void collect(T object) {
        if (buffer == null) {
            buffer = (T[]) new Object[1];
            initBuffer();
        }

        if (size >= buffer.length) {
            expand(size + 1);
            initBuffer();
        }

        if (dataTransferringStrategy.byReference()) {
            buffer[size++] = object;
        } else {
            dataTransferringStrategy.copy(object, buffer[this.size++]);
        }
    }

    @Override
    public void collect(T[] chunk, int size) {
        if (buffer.length < size) {
            expand(size);
        }

        if (dataTransferringStrategy.byReference()) {
            System.arraycopy(chunk, 0, buffer, 0, size);
        } else {
            for (int i = 0; i < size; i++) {
                dataTransferringStrategy.copy(chunk[i], buffer[size++]);
            }
        }

        this.size = size;
    }

    @Override
    public void collect(InputChunk<T> chunk) {
        collect(((IOBuffer<T>) chunk).buffer, chunk.size());
    }

    public void reset() {
        size = 0;
    }

    private void expand(int minCapacity) {
        // overflow-conscious code
        int oldCapacity = buffer.length;
        int newCapacity = oldCapacity + (oldCapacity >> 1);
        if (newCapacity - minCapacity < 0) {
            newCapacity = minCapacity;
        }
        if (newCapacity - MAX_ARRAY_SIZE > 0) {
            newCapacity = minCapacity;
        }
        // minCapacity is usually close to size, so this is a win:
        buffer = Arrays.copyOf(buffer, newCapacity);
    }

    public class DataIterator implements Iterator<T> {
        @Override
        public boolean hasNext() {
            return currentIdx < size();
        }

        @Override
        public T next() {
            return get(currentIdx++);
        }
    }
}
