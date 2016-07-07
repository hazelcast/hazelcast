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

package com.hazelcast.jet.memory.binarystorage.cursor;

import com.hazelcast.jet.memory.multimap.TupleMultimapHsa;

import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;

/**
 * Cursor over the tuple chain of a single multimap entry.
 */
public class TupleAddressCursorImpl implements TupleAddressCursor {
    private final TupleMultimapHsa layout;

    private long slotAddress = NULL_ADDRESS;
    private long tupleAddress = NULL_ADDRESS;

    public TupleAddressCursorImpl(TupleMultimapHsa layout) {
        this.layout = layout;
    }

    @Override
    public boolean advance() {
        assert slotAddress != NULL_ADDRESS : "Cursor invalid";
        tupleAddress = tupleAddress == NULL_ADDRESS
                ? layout.addrOfFirstTupleAt(slotAddress)
                : layout.addrOfNextTuple(tupleAddress);
        final boolean advanced = tupleAddress != NULL_ADDRESS;
        if (!advanced) {
            slotAddress = NULL_ADDRESS;
        }
        return advanced;
    }

    @Override
    public long tupleAddress() {
        assert slotAddress != NULL_ADDRESS : "Cursor invalid";
        return tupleAddress;
    }

    @Override
    public void reset(long slotAddress, int sourceId) {
        assert slotAddress != NULL_ADDRESS : "TupleCursor.reset() called with NULL slot address";
        this.slotAddress = slotAddress;
    }
}
