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

package com.hazelcast.jet.memory.api.memory.spilling.io;

import com.hazelcast.jet.memory.api.memory.management.MemoryBlock;

import java.io.IOException;

/**
 * Represents writer abstraction for spilled data;
 *
 * @param <S> - type of the source were data are stored;
 */
public interface SpillingDataOutput<S> {
    /**
     * Open current output for write;
     *
     * @param source - source with spilled data;
     */
    void open(S source);

    /**
     * Write next long value to the output;
     *
     * @param payLoad - long value to write;
     */
    void writeLong(long payLoad);

    /**
     * Write next int value to the output;
     *
     * @param payLoad - int value to write;
     */
    void writeInt(int payLoad);

    /**
     * Write next blob value to the output;
     *
     * @param memoryBlock - source where blob is stored;
     * @param address     - address of the blob;
     * @param size        - size of the blob;
     */
    void writeBlob(
            MemoryBlock memoryBlock,
            long address,
            long size
    );

    /**
     * Flush data;
     *
     * @throws IOException
     */
    void flush() throws IOException;

    /**
     * Close current output object;
     */
    void close();
}
