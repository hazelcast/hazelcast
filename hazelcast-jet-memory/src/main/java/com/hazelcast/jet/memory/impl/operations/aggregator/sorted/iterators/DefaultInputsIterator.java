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

package com.hazelcast.jet.memory.impl.operations.aggregator.sorted.iterators;

import com.hazelcast.jet.memory.impl.util.IOUtil;
import com.hazelcast.jet.memory.impl.util.MemoryUtil;
import com.hazelcast.jet.memory.api.binarystorage.StorageHeader;
import com.hazelcast.jet.memory.api.memory.management.MemoryBlock;
import com.hazelcast.jet.memory.api.memory.management.MemoryBlockChain;
import com.hazelcast.jet.memory.impl.binarystorage.DefaultStorageHeader;
import com.hazelcast.jet.memory.spi.binarystorage.sorted.OrderingDirection;
import com.hazelcast.jet.memory.api.operations.aggregator.sorted.InputsIterator;
import com.hazelcast.jet.memory.api.memory.spilling.format.SpillingKeyValueReader;
import com.hazelcast.jet.memory.api.binarystorage.sorted.BinaryKeyValueSortedStorage;

import java.util.Arrays;

public final class DefaultInputsIterator
        implements InputsIterator {
    private int inputsCount;

    private long[] keySize;

    private long[] hashCode;

    private long[] valueSize;

    private int[] sources;

    private int[] partitionId;

    private long[] recordsCount;

    private long[] recordAddresses;

    private long[] keyAddress;

    private long[] valueAddress;

    private long[] slotAddresses;

    private long[] storageAddresses;

    private MemoryBlockChain memoryInput;

    private SpillingKeyValueReader diskInput;

    private final MemoryBlock serviceMemoryBlock;

    private BinaryKeyValueSortedStorage binaryStorage;

    private final OrderingDirection orderingDirection;

    private final StorageHeader storageHeader = new DefaultStorageHeader();

    public DefaultInputsIterator(OrderingDirection orderingDirection,
                                 BinaryKeyValueSortedStorage binaryStorage,
                                 MemoryBlock serviceMemoryBlock) {
        this.binaryStorage = binaryStorage;
        this.orderingDirection = orderingDirection;
        this.serviceMemoryBlock = serviceMemoryBlock;
    }

    private boolean readSlotFromDisk() {
        if ((diskInput.hasNextSlot())) {
            diskInput.readNextSlot();
            return true;
        }

        return false;
    }

    private boolean readSourceFromDisk() {
        if ((diskInput.hasNextSource())) {
            diskInput.readNextSource();
            recordsCount[0] = diskInput.getRecordsCount();
            return true;
        }

        return false;
    }


    @Override
    public boolean nextSlot(int inputId) {
        recordAddresses[inputId] = MemoryUtil.NULL_VALUE;

        if (inputId == 0) {
            return readSlotFromDisk();
        }

        initStorage(inputId);

        long slotAddress = slotAddresses[inputId];

        if (slotAddress == MemoryUtil.NULL_VALUE) {
            slotAddress = binaryStorage.first(orderingDirection);
        } else {
            slotAddress = binaryStorage.getNext(slotAddress, orderingDirection);
        }

        if (slotAddress != MemoryUtil.NULL_VALUE) {
            slotAddresses[inputId] = slotAddress;
            hashCode[inputId] = binaryStorage.getSlotHashCode(slotAddress);
            recordsCount[inputId] = binaryStorage.getRecordsCount(slotAddress);
            return true;
        } else {
            hashCode[inputId] = 0;
            recordsCount[inputId] = 0;
            slotAddresses[inputId] = MemoryUtil.NULL_VALUE;
            return false;
        }
    }

    @Override
    public boolean nextSource(int inputId) {
        return inputId != 0 || readSourceFromDisk();
    }

    @Override
    public boolean nextRecord(int inputId) {
        long recordAddress = readRecord(inputId);

        if (recordAddress == MemoryUtil.NULL_VALUE) {
            return false;
        }

        recordAddresses[inputId] = recordAddress;
        MemoryBlock memoryBlock = getMemoryBlock(inputId);

        keyAddress[inputId] = IOUtil.getKeyAddress(recordAddress, memoryBlock);
        keySize[inputId] = IOUtil.getKeyWrittenBytes(recordAddress, memoryBlock);
        valueAddress[inputId] = IOUtil.getValueAddress(recordAddress, memoryBlock);
        valueSize[inputId] = IOUtil.getValueWrittenBytes(recordAddress, memoryBlock);
        return true;
    }

    private long readRecord(int inputId) {
        if (inputId == 0) {
            if (!readRecordFromDisk()) {
                return MemoryUtil.NULL_VALUE;
            }

            return MemoryBlock.TOP_OFFSET;
        }

        initStorage(inputId);

        long recordAddress = recordAddresses[inputId];

        if (recordAddress == MemoryUtil.NULL_VALUE) {
            recordAddress = binaryStorage.getHeaderRecordAddress(slotAddresses[inputId]);
        } else {
            recordAddress = binaryStorage.getNextRecordAddress(recordAddress);
        }

        return recordAddress;
    }

    private boolean readRecordFromDisk() {
        if (diskInput.hasNextRecord()) {
            serviceMemoryBlock.reset();
            diskInput.readNextRecord(MemoryBlock.TOP_OFFSET, serviceMemoryBlock);
            return true;
        }

        return false;
    }

    private void initStorage(int inputId) {
        binaryStorage.setMemoryBlock(getMemoryBlock(inputId));
        binaryStorage.gotoAddress(storageAddresses[inputId]);
        binaryStorage.setSorted(orderingDirection);
    }

    @Override
    public long keySize(int inputId) {
        return keySize[inputId];
    }

    @Override
    public long keyAddress(int inputId) {
        return keyAddress[inputId];
    }

    @Override
    public long valueSize(int inputId) {
        return valueSize[inputId];
    }

    @Override
    public long valueAddress(int inputId) {
        return valueAddress[inputId];
    }

    @Override
    public long recordsCount(int inputId) {
        return recordsCount[inputId];
    }

    @Override
    public long recordAddress(int inputId) {
        return recordAddresses[inputId];
    }

    @Override
    public int partitionId(int inputId) {
        return partitionId[inputId];
    }

    @Override
    public int getSourceId(int inputId) {
        return sources[inputId];
    }

    @Override
    public long getHashCode(int inputId) {
        return hashCode[inputId];
    }

    @Override
    public MemoryBlock getMemoryBlock(int inputId) {
        if (inputId == 0) {
            return serviceMemoryBlock;
        }

        return memoryInput.getElement(inputId - 1);
    }

    @Override
    public int getInputsCount() {
        return inputsCount;
    }

    @Override
    public void setInputs(SpillingKeyValueReader diskInput,
                          MemoryBlockChain memoryInput) {
        assert diskInput != null;
        assert memoryInput != null;

        this.diskInput = diskInput;
        this.memoryInput = memoryInput;
        this.inputsCount = memoryInput.size() + 1;

        initCells();

        for (int i = 0; i < memoryInput.size(); i++) {
            storageHeader.setMemoryBlock(memoryInput.getElement(i));
            storageAddresses[i + 1] = storageHeader.getBaseStorageAddress();
        }
    }

    private void initCells() {
        sources = initArray(sources, 0);
        keySize = initArray(keySize, 0L);
        hashCode = initArray(hashCode, 0L);
        valueSize = initArray(valueSize, 0L);
        partitionId = initArray(partitionId, 0);
        recordsCount = initArray(recordsCount, 0);
        keyAddress = initArray(keyAddress, MemoryUtil.NULL_VALUE);
        valueAddress = initArray(valueAddress, MemoryUtil.NULL_VALUE);
        recordAddresses = initArray(recordAddresses, MemoryUtil.NULL_VALUE);
        slotAddresses = initArray(slotAddresses, MemoryUtil.NULL_VALUE);
        recordAddresses = initArray(recordAddresses, MemoryUtil.NULL_VALUE);
        storageAddresses = initArray(storageAddresses, MemoryUtil.NULL_VALUE);
    }

    private long[] initArray(long[] input,
                             long defaultValue) {
        if ((input == null)
                || (input.length < inputsCount)) {
            return new long[inputsCount];
        }

        Arrays.fill(input, defaultValue);
        return input;
    }

    private int[] initArray(int[] input,
                            int defaultValue) {
        if ((input == null)
                || (input.length < inputsCount)) {
            return new int[inputsCount];
        }

        Arrays.fill(input, defaultValue);
        return input;
    }
}
