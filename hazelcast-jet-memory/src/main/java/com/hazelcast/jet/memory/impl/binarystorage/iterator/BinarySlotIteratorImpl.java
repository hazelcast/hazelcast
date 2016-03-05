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

package com.hazelcast.jet.memory.impl.binarystorage.iterator;

import com.hazelcast.internal.util.hashslot.HashSlotCursor8byteKey;
import com.hazelcast.jet.memory.api.binarystorage.oalayout.OALayout;
import com.hazelcast.jet.memory.api.binarystorage.iterator.BinarySlotIterator;
import com.hazelcast.jet.memory.impl.binarystorage.JetHashSlotArray8ByteKeyImpl;

public class BinarySlotIteratorImpl implements BinarySlotIterator {
    private final OALayout layout;

    private HashSlotCursor8byteKey cursor;
    private boolean lastHasNextCalled;
    private boolean lastHasNextResult;

    public BinarySlotIteratorImpl(OALayout layout) {
        this.layout = layout;
    }

    @Override
    public boolean hasNext() {
        if (lastHasNextCalled) {
            return lastHasNextResult;
        }

        try {
            lastHasNextResult = cursor.advance();
            return lastHasNextResult;
        } finally {
            lastHasNextCalled = true;
        }
    }

    @Override
    public long next() {
        if (!hasNext()) {
            throw new IllegalStateException("iterator finished");
        }

        try {
            return JetHashSlotArray8ByteKeyImpl.valueAddr2slotBase(cursor.valueAddress());
        } finally {
            lastHasNextCalled = false;
        }
    }

    @Override
    public void reset() {
        lastHasNextCalled = false;
        lastHasNextResult = false;
        cursor = this.layout.getHashSlotAllocator().cursor();
    }
}
