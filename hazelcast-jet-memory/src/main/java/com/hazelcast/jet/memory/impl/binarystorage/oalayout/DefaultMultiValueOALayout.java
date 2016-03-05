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

import com.hazelcast.jet.memory.impl.util.Util;
import com.hazelcast.jet.memory.impl.util.IOUtil;
import com.hazelcast.jet.memory.impl.util.MemoryUtil;
import com.hazelcast.jet.memory.spi.binarystorage.BinaryHasher;
import com.hazelcast.jet.memory.api.memory.management.MemoryBlock;
import com.hazelcast.jet.memory.api.binarystorage.oalayout.HsaResizeListener;
import com.hazelcast.jet.memory.api.binarystorage.oalayout.RecordHeaderLayOut;

/**
 * Implementation for multi-value open-addressing layout
 * Example:
 * <pre>
 *      KEY_1
 *              VALUE_1_1
 *              VALUE_1_2
 *              VALUE_2_1
 *              VALUE_2_2
 *      KEY_2
 *              VALUE_2_1
 *              VALUE_2_2
 *              VALUE_2_1
 *              VALUE_2_2
 */
public class DefaultMultiValueOALayout
        extends BaseOALayout {
    public DefaultMultiValueOALayout(
            MemoryBlock memoryBlock,
            BinaryHasher binaryHasher,
            HsaResizeListener hsaResizeListener,
            int initialCapacity,
            float loadFactor
    ) {
        super(memoryBlock, binaryHasher, hsaResizeListener, initialCapacity, loadFactor);
    }

    @Override
    protected void onSlotCreated(long slotAddress,
                                 short sourceId,
                                 long recordAddress,
                                 long valueAddress) {
        long recordSize = IOUtil.getRecordSize(recordAddress, memoryAccessor);

        IOUtil.setLong(
                recordAddress,
                recordSize +
                        RecordHeaderLayOut.LAST_RECORD_OFFSET,
                recordAddress,
                memoryAccessor
        );

        IOUtil.setLong(
                recordAddress,
                recordSize +
                        RecordHeaderLayOut.NEXT_RECORD_OFFSET,
                MemoryUtil.NULL_VALUE,
                memoryAccessor
        );

        IOUtil.setShort(
                recordAddress,
                recordSize +
                        RecordHeaderLayOut.SOURCE_OFFSET,
                sourceId,
                memoryAccessor
        );

        IOUtil.setLong(
                recordAddress,
                recordSize +
                        RecordHeaderLayOut.HASH_CODE_OFFSET,
                hsa.getLastHashCode(),
                memoryAccessor
        );

        IOUtil.setLong(
                recordAddress,
                recordSize +
                        RecordHeaderLayOut.RECORDS_COUNT_OFFSET,
                1,
                memoryAccessor
        );

        markSlot(slotAddress, Util.ZERO);
    }

    @Override
    protected void onSlotUpdated(long slotAddress,
                                 short sourceId,
                                 long recordAddress) {
        long headRecordAddress =
                getHeaderRecordAddress(slotAddress);

        long headRecordSize =
                IOUtil.getRecordSize(headRecordAddress, memoryAccessor);

        long previousRecordAddress = IOUtil.getLong(
                headRecordAddress,
                headRecordSize + RecordHeaderLayOut.LAST_RECORD_OFFSET,
                memoryAccessor
        );

        long recordCount =
                IOUtil.getLong(headRecordAddress,
                        headRecordSize +
                                RecordHeaderLayOut.RECORDS_COUNT_OFFSET,
                        memoryAccessor
                );

        long recordSize = IOUtil.getRecordSize(recordAddress, memoryAccessor);

        IOUtil.setLong(
                headRecordAddress,
                headRecordSize + RecordHeaderLayOut.LAST_RECORD_OFFSET,
                recordAddress,
                memoryAccessor
        );

        IOUtil.setLong(
                previousRecordAddress,
                IOUtil.getRecordSize(previousRecordAddress, memoryAccessor) +
                        RecordHeaderLayOut.NEXT_RECORD_OFFSET,
                recordAddress,
                memoryAccessor
        );

        IOUtil.setLong(
                headRecordAddress,
                headRecordSize +
                        RecordHeaderLayOut.RECORDS_COUNT_OFFSET,
                recordCount + 1,
                memoryAccessor
        );

        IOUtil.setLong(
                recordAddress,
                recordSize + RecordHeaderLayOut.NEXT_RECORD_OFFSET,
                MemoryUtil.NULL_VALUE,
                memoryAccessor
        );

        IOUtil.setShort(
                recordAddress,
                recordSize + RecordHeaderLayOut.SOURCE_OFFSET,
                sourceId,
                memoryAccessor
        );
    }

    @Override
    public long getNextRecordAddress(long recordAddress) {
        return IOUtil.getLong(
                recordAddress,
                IOUtil.getRecordSize(recordAddress, memoryAccessor) +
                        RecordHeaderLayOut.NEXT_RECORD_OFFSET,
                memoryAccessor
        );
    }

    @Override
    public long getValueAddress(long recordAddress) {
        return IOUtil.getValueAddress(recordAddress, memoryAccessor);
    }

    @Override
    public long getValueWrittenBytes(long recordAddress) {
        return IOUtil.getValueWrittenBytes(recordAddress, memoryAccessor);
    }
}
