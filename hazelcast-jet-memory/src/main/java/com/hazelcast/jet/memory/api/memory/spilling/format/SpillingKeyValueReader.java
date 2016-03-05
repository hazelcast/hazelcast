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
import com.hazelcast.jet.memory.api.memory.spilling.io.SpillingDataInput;

/**
 * * Provides methods to read key-value storage from some storage (disk or memory);
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
public interface SpillingKeyValueReader {
    /**
     * Open reader using corresponding inputStream;
     *
     * @param source - source to be used;
     */
    void open(SpillingDataInput source);

    /**
     * Read next record:
     *
     * @param offset             - offset where record will be written;
     * @param serviceMemoryBlock - memoryBlock which will be used as container;
     */
    void readNextRecord(long offset, MemoryBlock serviceMemoryBlock);

    /**
     * @return - partitionId of the last fetched key;
     */
    int getPartitionId();

    /**
     * @return - hashCode of the corresponding key;
     */
    long getHashCode();

    /**
     * @return - address of the last read record inside the corresponding MemoryBlock;
     */
    long getRecordAddress();

    /**
     * @return - amount of values for the last fetched key;
     */
    long getRecordsCount();

    /**
     * @return - number of sourceId;
     */
    int getSourceId();

    /**
     * @return true - if there is next key to read, false otherwise;
     */
    boolean hasNextRecord();

    /**
     * Read next slot
     */
    void readNextSlot();

    /**
     * @return true - if there is next slot to read, false otherwise;
     */
    boolean hasNextSlot();

    /**
     * Read next source;
     */
    void readNextSource();

    /**
     * @return n true - if there is next source, false otherwise;
     */
    boolean hasNextSource();

    /**
     * Close and finalize reader;
     */
    void close();
}
