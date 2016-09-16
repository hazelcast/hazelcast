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

import com.hazelcast.jet.impl.data.io.IOBuffer;
import com.hazelcast.jet.strategy.DataTransferringStrategy;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressWarnings("checkstyle:magicnumber")
abstract class RingbufferFieldsByValue<T> extends RingbufferPad {
    protected static final int BUFFER_PAD;

    static {
        final int scale = UnsafeUtil.UNSAFE_AVAILABLE ? UnsafeUtil.UNSAFE.arrayIndexScale(Object[].class) : 1;
        BUFFER_PAD = 128 / scale;
    }

    protected final long indexMask;
    protected final T[] entries;
    protected final int bufferSize;
    protected final DataTransferringStrategy<T> dataTransferringStrategy;

    RingbufferFieldsByValue(int bufferSize, DataTransferringStrategy<T> dataTransferringStrategy) {
        this.bufferSize = bufferSize;

        if (bufferSize < 1) {
            throw new IllegalArgumentException("bufferSize must not be less than 1");
        }

        if (Integer.bitCount(bufferSize) != 1) {
            throw new IllegalArgumentException("bufferSize must be a power of 2");
        }

        this.dataTransferringStrategy = dataTransferringStrategy;

        indexMask = bufferSize - 1;
        entries = (T[]) new Object[bufferSize + 2 * BUFFER_PAD];

        for (int i = 0; i < entries.length; i++) {
            entries[i] = dataTransferringStrategy.newInstance();
        }
    }
}

@SuppressFBWarnings("UUF_UNUSED_PUBLIC_OR_PROTECTED_FIELD")
@SuppressWarnings({
        "checkstyle:declarationorder", "checkstyle:multiplevariabledeclarations"
})
final class RingbufferWithValueStrategy<T> extends RingbufferFieldsByValue<T> implements RingbufferIO<T> {
    public static final long INITIAL_CURSOR_VALUE = 0L;
    private final PaddedLong readSequencer = new PaddedLong(RingbufferWithValueStrategy.INITIAL_CURSOR_VALUE);
    private final PaddedLong writeSequencer = new PaddedLong(RingbufferWithValueStrategy.INITIAL_CURSOR_VALUE);
    private final PaddedLong availableSequencer = new PaddedLong(RingbufferWithValueStrategy.INITIAL_CURSOR_VALUE);
    protected long p1, p2, p3, p4, p5, p6, p7;

    public RingbufferWithValueStrategy(int bufferSize, DataTransferringStrategy<T> dataTransferringStrategy) {
        super(bufferSize, dataTransferringStrategy);
    }

    @Override
    public int acquire(int acquired) {
        if (acquired > bufferSize) {
            acquired = bufferSize;
        }

        int remaining = bufferSize - (int) (availableSequencer.getValue() - readSequencer.getValue());

        if (remaining <= 0) {
            return 0;
        }

        int realAcquired = Math.min(remaining, acquired);

        writeSequencer.setValue(availableSequencer.getValue() + realAcquired);

        return realAcquired;
    }

    @Override
    public void commit(IOBuffer<T> chunk, int consumed) {
        long writerSequencerValue = writeSequencer.getValue();
        long availableSequencerValue = availableSequencer.getValue();

        int entriesStart = (int) (BUFFER_PAD + ((availableSequencerValue & indexMask)));
        int count = (int) (writerSequencerValue - availableSequencerValue);
        int window = entries.length - BUFFER_PAD - entriesStart;

        T[] buffer = chunk.toArray();

        if (count <= window) {
            for (int i = 0; i < count; i++) {
                dataTransferringStrategy.copy(buffer[consumed + i], entries[entriesStart + i]);
            }
        } else {
            for (int i = 0; i < window; i++) {
                dataTransferringStrategy.copy(buffer[consumed + i], entries[entriesStart + i]);
            }

            for (int i = 0; i < count - window; i++) {
                dataTransferringStrategy.copy(buffer[consumed + window + i], entries[BUFFER_PAD + i]);
            }
        }

        availableSequencer.setValue(writerSequencerValue);
    }

    @Override
    public int fetch(T[] chunk) {
        long availableSequence = availableSequencer.getValue();
        long readerSequencerValue = readSequencer.getValue();

        int count = Math.min(chunk.length, (int) (availableSequence - readerSequencerValue));
        int entriesStart = (int) (BUFFER_PAD + ((readerSequencerValue & indexMask)));
        int window = entries.length - BUFFER_PAD - entriesStart;

        if (count <= window) {
            for (int i = 0; i < count; i++) {
                dataTransferringStrategy.copy(entries[entriesStart + i], chunk[i]);
                dataTransferringStrategy.clean(entries[entriesStart + i]);
            }
        } else {
            for (int i = 0; i < window; i++) {
                dataTransferringStrategy.copy(entries[entriesStart + i], chunk[i]);
                dataTransferringStrategy.clean(entries[entriesStart + i]);
            }

            for (int i = 0; i < count - window; i++) {
                dataTransferringStrategy.copy(entries[BUFFER_PAD + i], chunk[window + i]);
                dataTransferringStrategy.clean(entries[BUFFER_PAD + i]);
            }
        }

        readSequencer.setValue(readSequencer.getValue() + count);
        return count;
    }

    @Override
    public void reset() {
        readSequencer.setValue(0);
        writeSequencer.setValue(0);
        availableSequencer.setValue(0);
    }
}
