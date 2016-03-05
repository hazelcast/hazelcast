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

package com.hazelcast.jet.memory.impl.memory.impl.spilling.format;

import com.hazelcast.jet.memory.impl.util.IOUtil;
import com.hazelcast.jet.memory.api.memory.management.MemoryBlock;
import com.hazelcast.jet.memory.api.memory.spilling.io.SpillingDataOutput;
import com.hazelcast.jet.memory.api.memory.spilling.format.SpillingKeyValueWriter;


import java.io.IOException;

/**
 * Provides methods to write key-value storage to disk storage
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
public class DefaultSpillingKeyValueWriter implements SpillingKeyValueWriter {
    protected SpillingDataOutput dataOutput;

    @Override
    public void open(SpillingDataOutput source) {
        this.dataOutput = source;
    }

    @Override
    public void writeSlotHeader(int partitionID,
                                long hashCode,
                                int sourceCount) {
        dataOutput.writeInt(partitionID);
        dataOutput.writeLong(hashCode);
        dataOutput.writeInt(sourceCount);
    }

    @Override
    public void writeSourceHeader(int sourceId,
                                  long recordsCount) {
        dataOutput.writeInt(sourceId);
        dataOutput.writeLong(recordsCount);
    }

    @Override
    public void writeRecord(MemoryBlock sourceBlock,
                            final long recordAddress) {
        long keySize = IOUtil.getKeyWrittenBytes(recordAddress, sourceBlock);
        long valueSize = IOUtil.getValueWrittenBytes(recordAddress, sourceBlock);

        long keyAddress = IOUtil.getKeyAddress(recordAddress, sourceBlock);
        long valueAddress = IOUtil.getValueAddress(recordAddress, sourceBlock);

        writeRecord(sourceBlock, keySize, valueSize, keyAddress, valueAddress);
    }

    @Override
    public void writeRecord(MemoryBlock sourceBlock,
                            long keySize,
                            long valueSize,
                            long keyAddress,
                            long valueAddress) {
        dataOutput.writeLong(keySize);
        dataOutput.writeBlob(sourceBlock, keyAddress, keySize);
        dataOutput.writeLong(valueSize);
        dataOutput.writeBlob(sourceBlock, valueAddress, valueSize);
    }

    @Override
    public void flush() throws IOException {
        dataOutput.flush();
    }

    @Override
    public void close() {
        if (dataOutput != null) {
            dataOutput.close();
        }
    }
}
