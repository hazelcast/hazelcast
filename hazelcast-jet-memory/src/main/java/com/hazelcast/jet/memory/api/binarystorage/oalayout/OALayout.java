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

package com.hazelcast.jet.memory.api.binarystorage.oalayout;

import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.internal.memory.MemoryManager;
import com.hazelcast.jet.memory.spi.binarystorage.BinaryHasher;
import com.hazelcast.internal.util.hashslot.HashSlotArray8byteKey;

/**
 * Represents abstract api to work with an arbitrary open addressing layout
 */
public interface OALayout {
    int KEY_SIZE_IN_BYTES = 8;
    float DEFAULT_LOAD_FACTOR = 0.50f;
    int DEFAULT_INITIAL_CAPACITY = 2 * 1024;

    long ensureSlot(
            long recordAddress,
            short sourceId,
            MemoryAccessor memoryAccessor,
            boolean createIfNotExists,
            boolean createIfExists
    );

    long ensureSlot(
            long recordAddress,
            short sourceId,
            MemoryAccessor memoryAccessor,
            BinaryHasher binaryHasher,
            boolean createIfNotExists,
            boolean createIfExists
    );

    long gotoNew();

    long slotsCount();

    void compact();

    long capacityMask();

    int getSlotSizeInBytes();

    boolean wasLastSlotCreated();

    //Record methods
    long getKeyAddress(long recordAddress);

    long getValueAddress(long recordAddress);

    long getKeyWrittenBytes(long recordAddress);

    byte getSlotMarker(long slotAddress);

    void markSlot(long slotAddress, byte marker);

    long getValueWrittenBytes(long recordAddress);

    long getNextRecordAddress(long recordAddress);

    // Slot methods
    long getHeaderRecordAddress(long slotAddress);

    long getSlotsWrittenRecords(long slotAddress);

    long getSlotHashCode(long slotAddress);

    void setSlotHashCode(long slotAddress, long hashCode);

    void gotoAddress(long baseAddress);

    void setAuxMemoryManager(MemoryManager auxMemoryManager);

    HashSlotArray8byteKey getHashSlotAllocator();

    void setListener(HsaSlotCreationListener hsaSlotCreationListener);
}
