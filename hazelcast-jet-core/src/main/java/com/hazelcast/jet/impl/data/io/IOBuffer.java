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

package com.hazelcast.jet.impl.data.io;

import com.hazelcast.jet.runtime.OutputCollector;
import com.hazelcast.jet.runtime.InputChunk;
import com.hazelcast.jet.impl.ringbuffer.ByReferenceDataTransferringStrategy;
import com.hazelcast.jet.strategy.DataTransferringStrategy;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Arrays;
import java.util.Iterator;

public class IOBuffer<T> implements InputChunk<T>, OutputCollector<T> {
    private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE;
    private final Iterator<T> iterator = new DataIterator();
    private final DataTransferringStrategy dataTransferringStrategy;
    private int size;
    private T[] buffer;
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
            for (int i = 0; i < this.buffer.length; i++) {
                if (this.buffer[i] == null) {
                    this.buffer[i] = (T) dataTransferringStrategy.newInstance();
                }
            }
        }
    }

    @Override
    public T get(int idx) {
        return idx < this.buffer.length ? this.buffer[idx] : null;
    }

    @Override
    public Iterator<T> iterator() {
        this.currentIdx = 0;
        return this.iterator;
    }

    @Override
    public int size() {
        return this.size;
    }

    @Override
    public void collect(T object) {
        if (this.buffer == null) {
            this.buffer = (T[]) new Object[1];
            initBuffer();
        }

        if (this.size >= this.buffer.length) {
            expand(this.size + 1);
            initBuffer();
        }

        if (this.dataTransferringStrategy.byReference()) {
            this.buffer[this.size++] = object;
        } else {
            this.dataTransferringStrategy.copy(object, this.buffer[this.size++]);
        }
    }

    @Override
    public void collect(T[] chunk, int size) {
        if (this.buffer.length < size) {
            expand(size);
        }

        if (this.dataTransferringStrategy.byReference()) {
            System.arraycopy(chunk, 0, this.buffer, 0, size);
        } else {
            for (int i = 0; i < size; i++) {
                this.dataTransferringStrategy.copy(chunk[i], this.buffer[this.size++]);
            }
        }

        this.size = size;
    }

    @Override
    public void collect(InputChunk<T> chunk) {
        collect(((IOBuffer<T>) chunk).toArray(), chunk.size());
    }

    @SuppressFBWarnings("EI_EXPOSE_REP")
    public T[] toArray() {
        return this.buffer;
    }

    public void reset() {
        if (this.buffer != null) {
            if (this.dataTransferringStrategy.byReference()) {
                Arrays.fill(this.buffer, null);
            } else {
                for (T object : this.buffer) {
                    this.dataTransferringStrategy.clean(object);
                }
            }
        }

        this.size = 0;
    }

    private void expand(int minCapacity) {
        // overflow-conscious code
        int oldCapacity = this.buffer.length;
        int newCapacity = oldCapacity + (oldCapacity >> 1);
        if (newCapacity - minCapacity < 0) {
            newCapacity = minCapacity;
        }
        if (newCapacity - MAX_ARRAY_SIZE > 0) {
            newCapacity = minCapacity;
        }
        // minCapacity is usually close to size, so this is a win:
        this.buffer = Arrays.copyOf(this.buffer, newCapacity);
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
