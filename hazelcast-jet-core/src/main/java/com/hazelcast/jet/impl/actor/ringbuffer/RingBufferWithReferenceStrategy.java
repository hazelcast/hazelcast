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

package com.hazelcast.jet.impl.actor.ringbuffer;

import com.hazelcast.jet.impl.actor.RingBuffer;
import com.hazelcast.jet.impl.data.BufferAware;
import com.hazelcast.jet.data.io.ProducerInputStream;
import com.hazelcast.logging.ILogger;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Arrays;

@SuppressFBWarnings("UUF_UNUSED_PUBLIC_OR_PROTECTED_FIELD")
@SuppressWarnings({
        "checkstyle:declarationorder", "checkstyle:multiplevariabledeclarations"
})
abstract class RingBufferPadByReference {
    protected long p1, p2, p3, p4, p5, p6, p7;
}

@SuppressWarnings("checkstyle:magicnumber")
abstract class RingBufferFieldsByReference<T> extends RingBufferPadByReference {
    protected static final int BUFFER_PAD;

    static {
        final int scale =
                UnsafeUtil.UNSAFE_AVAILABLE
                        ?
                        UnsafeUtil.UNSAFE.arrayIndexScale(Object[].class)
                        :
                        1;
        BUFFER_PAD = 128 / scale;
    }

    protected final long indexMask;
    protected final T[] entries;
    protected final int bufferSize;

    RingBufferFieldsByReference(
            int bufferSize
    ) {
        this.bufferSize = bufferSize;

        if (bufferSize < 1) {
            throw new IllegalArgumentException("bufferSize must not be less than 1");
        }

        if (Integer.bitCount(bufferSize) != 1) {
            throw new IllegalArgumentException("bufferSize must be a power of 2");
        }

        this.indexMask = bufferSize - 1;
        this.entries = (T[]) new Object[bufferSize + 2 * BUFFER_PAD];
    }
}

@SuppressFBWarnings("UUF_UNUSED_PUBLIC_OR_PROTECTED_FIELD")
@SuppressWarnings({
        "checkstyle:declarationorder", "checkstyle:multiplevariabledeclarations"
})
public final class RingBufferWithReferenceStrategy<T> extends RingBufferFieldsByReference<T> implements RingBuffer<T> {
    public static final long INITIAL_CURSOR_VALUE = 0L;
    private final PaddedLong readSequencer = new PaddedLong(RingBufferWithReferenceStrategy.INITIAL_CURSOR_VALUE);
    private final PaddedLong writeSequencer = new PaddedLong(RingBufferWithReferenceStrategy.INITIAL_CURSOR_VALUE);
    private final PaddedLong availableSequencer = new PaddedLong(RingBufferWithReferenceStrategy.INITIAL_CURSOR_VALUE);
    private final ILogger logger;
    protected long p1, p2, p3, p4, p5, p6, p7;

    public RingBufferWithReferenceStrategy(
            int bufferSize,
            ILogger logger
    ) {
        super(bufferSize);
        this.logger = logger;
    }

    @Override
    public int acquire(int acquired) {
        if (acquired > this.bufferSize) {
            acquired = bufferSize;
        }

        int remaining = this.bufferSize - (int) (this.availableSequencer.getValue() - this.readSequencer.getValue());

        if (remaining <= 0) {
            return 0;
        }

        int realAcquired = Math.min(remaining, acquired);

        this.writeSequencer.setValue(this.availableSequencer.getValue() + realAcquired);

        return realAcquired;
    }

    @Override
    public void commit(ProducerInputStream<T> chunk, int consumed) {
        long writerSequencerValue = this.writeSequencer.getValue();
        long availableSequencerValue = this.availableSequencer.getValue();

        int entriesStart = (int) (BUFFER_PAD + ((availableSequencerValue & this.indexMask)));
        int count = (int) (writerSequencerValue - availableSequencerValue);
        int window = this.entries.length - BUFFER_PAD - entriesStart;

        T[] buffer = ((BufferAware<T>) chunk).getBuffer();

        if (count <= window) {
            System.arraycopy(
                    buffer,
                    consumed,
                    entries,
                    entriesStart,
                    count
            );
        } else {
            System.arraycopy(
                    buffer,
                    consumed,
                    this.entries,
                    entriesStart,
                    window
            );

            System.arraycopy(
                    buffer,
                    consumed + window,
                    this.entries,
                    BUFFER_PAD,
                    count - window
            );
        }

        this.availableSequencer.setValue(writerSequencerValue);
    }

    @Override
    public int fetch(T[] chunk) {
        long availableSequence = this.availableSequencer.getValue();
        long readerSequencerValue = this.readSequencer.getValue();

        int count = Math.min(chunk.length, (int) (availableSequence - readerSequencerValue));
        int entriesStart = (int) (BUFFER_PAD + ((readerSequencerValue & this.indexMask)));
        int window = this.entries.length - BUFFER_PAD - entriesStart;

        if (count <= window) {
            System.arraycopy(this.entries, entriesStart, chunk, 0, count);
            Arrays.fill(this.entries, entriesStart, entriesStart + count, null);
        } else {
            System.arraycopy(this.entries, entriesStart, chunk, 0, window);
            Arrays.fill(this.entries, entriesStart, entriesStart + window, null);
            System.arraycopy(this.entries, BUFFER_PAD, chunk, window, count - window);
            Arrays.fill(this.entries, BUFFER_PAD, BUFFER_PAD + count - window, null);
        }

        this.readSequencer.setValue(this.readSequencer.getValue() + count);
        return count;
    }

    @Override
    public void reset() {
        this.readSequencer.setValue(0);
        this.writeSequencer.setValue(0);
        this.availableSequencer.setValue(0);
        Arrays.fill(this.entries, null);
    }
}
