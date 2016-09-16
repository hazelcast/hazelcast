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
import com.hazelcast.logging.ILogger;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Arrays;



@SuppressWarnings("checkstyle:magicnumber")
abstract class RingufferFields<T> extends RingbufferPad {
    protected static final int BUFFER_PAD;

    static {
        final int scale = UnsafeUtil.UNSAFE_AVAILABLE ? UnsafeUtil.UNSAFE.arrayIndexScale(Object[].class) : 1;
        BUFFER_PAD = 128 / scale;
    }

    protected final long indexMask;
    protected final T[] entries;
    protected final int bufferSize;

    RingufferFields(int bufferSize) {
        this.bufferSize = bufferSize;

        if (bufferSize < 1) {
            throw new IllegalArgumentException("bufferSize must not be less than 1");
        }

        if (Integer.bitCount(bufferSize) != 1) {
            throw new IllegalArgumentException("bufferSize must be a power of 2");
        }

        indexMask = bufferSize - 1;
        entries = (T[]) new Object[bufferSize + 2 * BUFFER_PAD];
    }
}

@SuppressFBWarnings("UUF_UNUSED_PUBLIC_OR_PROTECTED_FIELD")
@SuppressWarnings({
        "checkstyle:declarationorder", "checkstyle:multiplevariabledeclarations"
})
final class RingbufferWithReferenceStrategy<T> extends RingufferFields<T> implements RingbufferIO<T> {
    public static final long INITIAL_CURSOR_VALUE = 0L;
    private final PaddedLong readSequencer = new PaddedLong(RingbufferWithReferenceStrategy.INITIAL_CURSOR_VALUE);
    private final PaddedLong writeSequencer = new PaddedLong(RingbufferWithReferenceStrategy.INITIAL_CURSOR_VALUE);
    private final PaddedLong availableSequencer = new PaddedLong(RingbufferWithReferenceStrategy.INITIAL_CURSOR_VALUE);
    private final ILogger logger;
    protected long p1, p2, p3, p4, p5, p6, p7;

    public RingbufferWithReferenceStrategy(int bufferSize, ILogger logger) {
        super(bufferSize);
        this.logger = logger;
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
            System.arraycopy(buffer, consumed, entries, entriesStart, count);
        } else {
            System.arraycopy(buffer, consumed, entries, entriesStart, window);
            System.arraycopy(buffer, consumed + window, entries, BUFFER_PAD, count - window);
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
            System.arraycopy(entries, entriesStart, chunk, 0, count);
            Arrays.fill(entries, entriesStart, entriesStart + count, null);
        } else {
            System.arraycopy(entries, entriesStart, chunk, 0, window);
            Arrays.fill(entries, entriesStart, entriesStart + window, null);
            System.arraycopy(entries, BUFFER_PAD, chunk, window, count - window);
            Arrays.fill(entries, BUFFER_PAD, BUFFER_PAD + count - window, null);
        }

        readSequencer.setValue(readSequencer.getValue() + count);
        return count;
    }

    @Override
    public void reset() {
        readSequencer.setValue(0);
        writeSequencer.setValue(0);
        availableSequencer.setValue(0);
        Arrays.fill(entries, null);
    }
}
