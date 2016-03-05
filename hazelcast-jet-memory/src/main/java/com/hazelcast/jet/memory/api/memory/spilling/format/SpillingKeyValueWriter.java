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

package com.hazelcast.jet.memory.api.memory.spilling.format;

import com.hazelcast.jet.memory.api.memory.management.MemoryBlock;
import com.hazelcast.jet.memory.api.memory.spilling.io.SpillingDataOutput;

import java.io.Flushable;

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
public interface SpillingKeyValueWriter extends Flushable {
    void open(SpillingDataOutput dataOutput);

    void writeSlotHeader(int partitionID,
                         long hashCode,
                         int sourceCount);

    void writeSourceHeader(
            int sourceId,
            long recordsCount
    );

    void writeRecord(MemoryBlock sourceBlock,
                     long recordAddress
    );

    void writeRecord(
            MemoryBlock sourceBlock,
            long keySize,
            long valueSize,
            long keyAddress,
            long valueAddress
    );

    void close();
}
