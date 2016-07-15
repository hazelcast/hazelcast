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

import com.hazelcast.internal.util.hashslot.HashSlotArray8byteKey;
import com.hazelcast.internal.util.hashslot.HashSlotCursor8byteKey;
import com.hazelcast.jet.memory.multimap.TupleMultimapHsa;

import static com.hazelcast.jet.memory.multimap.TupleMultimapHsa.toSlotAddr;

/**
 * Javadoc pending.
 */
public final class SlotAddressCursorImpl implements SlotAddressCursor {
    private final HashSlotArray8byteKey hsa;
    private HashSlotCursor8byteKey cursor;

    public SlotAddressCursorImpl(TupleMultimapHsa layout) {
        this.hsa = layout.getHashSlotArray();
        reset();
    }

    @Override
    public boolean advance() {
        return cursor.advance();
    }

    @Override
    public long slotAddress() {
        return toSlotAddr(cursor.valueAddress());
    }

    @Override
    public void reset() {
        cursor = hsa.cursor();
    }
}
