/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.merge;

import com.hazelcast.spi.annotation.Beta;

import java.util.Iterator;

/**
 * Read-only iterator over items in a provided {@link RingbufferMergeData}.
 *
 * @param <E> ringbuffer item types
 * @since 3.10
 */
@Beta
public class RingbufferMergeDataReadOnlyIterator<E> implements Iterator<E> {

    private final RingbufferMergeData ringbuffer;
    private long sequence;

    RingbufferMergeDataReadOnlyIterator(RingbufferMergeData ringbuffer) {
        this.ringbuffer = ringbuffer;
        this.sequence = ringbuffer.getHeadSequence();
    }

    @Override
    public boolean hasNext() {
        return sequence <= ringbuffer.getTailSequence();
    }

    @Override
    public E next() {
        return ringbuffer.read(sequence++);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
