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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Abstract interface fo ringBuffers;
 *
 * @param <T> - type of the content in the ring buffer;
 */
interface RingbufferIO<T> {
    int acquire(int acquired);

    void commit(IOBuffer<T> buffer, int consumed);

    int fetch(T[] chunk);

    void reset();
}

@SuppressFBWarnings("UUF_UNUSED_PUBLIC_OR_PROTECTED_FIELD")
@SuppressWarnings({
        "checkstyle:declarationorder", "checkstyle:multiplevariabledeclarations"
})
abstract class RingbufferPad {
    protected long p1, p2, p3, p4, p5, p6, p7;
}

@SuppressFBWarnings("UUF_UNUSED_PUBLIC_OR_PROTECTED_FIELD")
@SuppressWarnings({
        "checkstyle:declarationorder", "checkstyle:multiplevariabledeclarations"
})
abstract class LeftPaddedLong {
    protected long p1, p2, p3, p4, p5, p6, p7;
}

abstract class RightPaddedLong extends LeftPaddedLong {
    private volatile long value;

    RightPaddedLong(long value) {
        this.value = value;
    }

    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }
}

@SuppressFBWarnings("UUF_UNUSED_PUBLIC_OR_PROTECTED_FIELD")
@SuppressWarnings({
        "checkstyle:declarationorder", "checkstyle:multiplevariabledeclarations"
})
class PaddedLong extends RightPaddedLong {
    protected long p1, p2, p3, p4, p5, p6, p7;

    public PaddedLong(long value) {
        super(value);
    }
}
