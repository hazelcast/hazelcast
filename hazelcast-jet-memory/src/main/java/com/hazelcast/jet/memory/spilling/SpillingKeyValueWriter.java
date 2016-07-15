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

package com.hazelcast.jet.memory.spilling;

import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.jet.memory.memoryblock.MemoryBlock;
import com.hazelcast.jet.memory.util.JetIoUtil;

import java.io.Flushable;
import java.io.IOException;

/**
 * Provides methods to save memory storage to disk
 * <p>
 * Storage format is:
 * <p>
 * <pre>
 * -------------------------------------------------------------------------------------------------
 * |int        |long    |int        |int     |long        |long     |byte[]  |long     |byte[]     |
 * |partitionID|hashCode|sourceCount|sourceID|recordsCount|keySize  |keyBytes|valueSize|valueBytes |
 * -------------------------------------------------------------------------------------------------
 * </pre>
 */
public class SpillingKeyValueWriter implements Flushable {
    protected SpillFileWriter dataOutput;

    public void open(SpillFileWriter source) {
        this.dataOutput = source;
    }

    public void writeSlotHeader(int partitionID, long hashCode, int segmentCount) {
        dataOutput.writeInt(partitionID);
        dataOutput.writeLong(hashCode);
        dataOutput.writeInt(segmentCount);
    }

    public void writeSegmentHeader(int segmentId, long recordCount) {
        dataOutput.writeInt(segmentId);
        dataOutput.writeLong(recordCount);
    }

    public void writeRecord(MemoryBlock mBlock, long tupleAddress) {
        final MemoryAccessor mem = mBlock.getAccessor();
        long keySize = JetIoUtil.sizeOfKeyBlockAt(tupleAddress, mem);
        long valueSize = JetIoUtil.sizeOfValueBlockAt(tupleAddress, mem);
        long keyAddress = JetIoUtil.addressOfKeyBlockAt(tupleAddress);
        long valueAddress = JetIoUtil.addrOfValueBlockAt(tupleAddress, mem);
        writeRecord(mBlock, keySize, valueSize, keyAddress, valueAddress);
    }

    public void writeRecord(MemoryBlock sourceBlock, long keySize, long valueSize, long keyAddress, long valueAddress) {
        dataOutput.writeLong(keySize);
        dataOutput.writeBlob(sourceBlock, keyAddress, keySize);
        dataOutput.writeLong(valueSize);
        dataOutput.writeBlob(sourceBlock, valueAddress, valueSize);
    }

    @Override
    public void flush() throws IOException {
        dataOutput.flush();
    }

    public void close() {
        if (dataOutput != null) {
            dataOutput.close();
        }
    }
}
