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

package com.hazelcast.jet.memory.operation.aggregator.cursor;

import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.jet.memory.binarystorage.SortOrder;
import com.hazelcast.jet.memory.binarystorage.SortedStorage;
import com.hazelcast.jet.memory.binarystorage.StorageHeader;
import com.hazelcast.jet.memory.memoryblock.MemoryBlock;
import com.hazelcast.jet.memory.memoryblock.MemoryBlockChain;
import com.hazelcast.jet.memory.spilling.SpillFileCursor;
import com.hazelcast.jet.memory.util.JetIoUtil;

import java.util.Arrays;

/**
 * Cursor over records spilled to disk.
 */
public class InputsCursor {
    private final MemoryBlock serviceMemoryBlock;
    private final SortOrder sortOrder;
    private final StorageHeader storageHeader = new StorageHeader();
    private final SortedStorage storage;

    private int inputsCount;
    private long[] keySize;
    private long[] hashCodes;
    private long[] valueSize;
    private int[] sources;
    private int[] partitionId;
    private long[] tupleCounts;
    private long[] recordAddresses;
    private long[] keyAddress;
    private long[] valueAddress;
    private long[] slotAddresses;
    private long[] storageAddresses;
    private MemoryBlockChain memoryInput;
    private SpillFileCursor diskInput;

    public InputsCursor(SortOrder sortOrder, SortedStorage storage, MemoryBlock serviceMemoryBlock) {
        this.storage = storage;
        this.sortOrder = sortOrder;
        this.serviceMemoryBlock = serviceMemoryBlock;
    }

    public void setInputs(SpillFileCursor diskInput, MemoryBlockChain memoryInput) {
        assert diskInput != null;
        assert memoryInput != null;
        this.diskInput = diskInput;
        this.memoryInput = memoryInput;
        this.inputsCount = memoryInput.size() + 1;
        initCells();
        for (int i = 0; i < memoryInput.size(); i++) {
            storageHeader.setMemoryBlock(memoryInput.get(i));
            storageAddresses[i + 1] = storageHeader.baseAddress();
        }
    }

    public boolean nextSlot(int inputId) {
        recordAddresses[inputId] = MemoryAllocator.NULL_ADDRESS;
        if (inputId == 0) {
            return diskInput.slotAdvance();
        }
        storageGotoInput(inputId);
        long slotAddress = slotAddresses[inputId];
        if (slotAddress == MemoryAllocator.NULL_ADDRESS) {
            storage.ensureSorted(sortOrder);
            slotAddress = storage.addrOfFirstSlot(sortOrder);
        } else {
            slotAddress = storage.addrOfNextSlot(slotAddress, sortOrder);
        }
        if (slotAddress != MemoryAllocator.NULL_ADDRESS) {
            slotAddresses[inputId] = slotAddress;
            hashCodes[inputId] = storage.getSlotHashCode(slotAddress);
            tupleCounts[inputId] = storage.tupleCountAt(slotAddress);
            return true;
        } else {
            hashCodes[inputId] = 0;
            tupleCounts[inputId] = 0;
            slotAddresses[inputId] = MemoryAllocator.NULL_ADDRESS;
            return false;
        }
    }

    public boolean nextSource(int inputId) {
        return inputId != 0 || readSourceFromDisk();
    }

    public boolean nextRecord(int inputId) {
        long recordAddress = readRecord(inputId);
        if (recordAddress == MemoryAllocator.NULL_ADDRESS) {
            return false;
        }
        recordAddresses[inputId] = recordAddress;
        MemoryBlock memoryBlock = getMemoryBlock(inputId);
        keyAddress[inputId] = JetIoUtil.addressOfKeyBlockAt(recordAddress);
        final MemoryAccessor accessor = memoryBlock.getAccessor();
        keySize[inputId] = JetIoUtil.sizeOfKeyBlockAt(recordAddress, accessor);
        valueAddress[inputId] = JetIoUtil.addrOfValueBlockAt(recordAddress, accessor);
        valueSize[inputId] = JetIoUtil.sizeOfValueBlockAt(recordAddress, accessor);
        return true;
    }

    public long keySize(int inputId) {
        return keySize[inputId];
    }

    public long keyAddress(int inputId) {
        return keyAddress[inputId];
    }

    public long valueSize(int inputId) {
        return valueSize[inputId];
    }

    public long valueAddress(int inputId) {
        return valueAddress[inputId];
    }

    public long recordsCount(int inputId) {
        return tupleCounts[inputId];
    }

    public long recordAddress(int inputId) {
        return recordAddresses[inputId];
    }

    public int partitionId(int inputId) {
        return partitionId[inputId];
    }

    public int getSourceId(int inputId) {
        return sources[inputId];
    }

    public long getHashCode(int inputId) {
        return hashCodes[inputId];
    }

    public MemoryBlock getMemoryBlock(int inputId) {
        return inputId == 0 ? serviceMemoryBlock : memoryInput.get(inputId - 1);
    }

    public int getInputsCount() {
        return inputsCount;
    }

    private long readRecord(int inputId) {
        if (inputId == 0) {
            return diskInput.recordAdvance(serviceMemoryBlock, MemoryBlock.TOP_OFFSET, false)
                    ? MemoryBlock.TOP_OFFSET : MemoryAllocator.NULL_ADDRESS;
        }
        storageGotoInput(inputId);
        long recordAddress = recordAddresses[inputId];
        return recordAddress != MemoryAllocator.NULL_ADDRESS
                ? storage.addrOfNextTuple(recordAddress)
                : storage.addrOfFirstTuple(slotAddresses[inputId]);
    }

    private void storageGotoInput(int inputId) {
        storage.setMemoryBlock(getMemoryBlock(inputId));
        storage.gotoAddress(storageAddresses[inputId]);
        storage.setAlreadySorted(sortOrder);
    }

    private void initCells() {
        sources = reuseIfPossible(sources, 0);
        keySize = reuseIfPossible(keySize, 0L);
        hashCodes = reuseIfPossible(hashCodes, 0L);
        valueSize = reuseIfPossible(valueSize, 0L);
        partitionId = reuseIfPossible(partitionId, 0);
        tupleCounts = reuseIfPossible(tupleCounts, 0);
        keyAddress = reuseIfPossible(keyAddress, MemoryAllocator.NULL_ADDRESS);
        valueAddress = reuseIfPossible(valueAddress, MemoryAllocator.NULL_ADDRESS);
        recordAddresses = reuseIfPossible(recordAddresses, MemoryAllocator.NULL_ADDRESS);
        slotAddresses = reuseIfPossible(slotAddresses, MemoryAllocator.NULL_ADDRESS);
        recordAddresses = reuseIfPossible(recordAddresses, MemoryAllocator.NULL_ADDRESS);
        storageAddresses = reuseIfPossible(storageAddresses, MemoryAllocator.NULL_ADDRESS);
    }

    private boolean readSourceFromDisk() {
        if (diskInput.segmentAdvance()) {
            tupleCounts[0] = diskInput.getRecordCountInCurrentSegment();
            return true;
        }
        return false;
    }

    private long[] reuseIfPossible(long[] input, long defaultValue) {
        if (input == null || input.length < inputsCount) {
            return new long[inputsCount];
        }
        Arrays.fill(input, defaultValue);
        return input;
    }

    private int[] reuseIfPossible(int[] input, int defaultValue) {
        if (input == null || input.length < inputsCount) {
            return new int[inputsCount];
        }
        Arrays.fill(input, defaultValue);
        return input;
    }
}
