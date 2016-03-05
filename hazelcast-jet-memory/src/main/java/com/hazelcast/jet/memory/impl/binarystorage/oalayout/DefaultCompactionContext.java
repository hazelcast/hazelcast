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

package com.hazelcast.jet.memory.impl.binarystorage.oalayout;

import com.hazelcast.jet.memory.api.binarystorage.oalayout.CompactionContext;
import com.hazelcast.jet.memory.impl.util.MemoryUtil;

public class DefaultCompactionContext implements CompactionContext {
    private long slotsCount;
    private long slotNumber;
    private long chunkAddress;

    @Override
    public long chunkAddress() {
        return chunkAddress;
    }

    @Override
    public long slotNumber() {
        return slotNumber;
    }

    @Override
    public long slotsCount() {
        return slotsCount;
    }

    @Override
    public void setSlotNumber(long slotNumber) {
        this.slotNumber = slotNumber;
    }

    @Override
    public void setChunkAddress(long chunkAddress) {
        this.chunkAddress = chunkAddress;
    }

    @Override
    public void incrementCount() {
        slotsCount++;
    }

    @Override
    public void setSlotCount(long slotsCount) {
        this.slotsCount = slotsCount;
    }

    @Override
    public void incrementSlotNumber(long delta) {
        slotNumber += delta;
    }

    @Override
    public void reset() {
        slotsCount = 0L;
        slotNumber = 0L;
        chunkAddress = MemoryUtil.NULL_VALUE;
    }
}
