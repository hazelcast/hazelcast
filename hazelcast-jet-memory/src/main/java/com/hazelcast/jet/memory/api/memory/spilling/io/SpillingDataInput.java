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

/**
 * Represents reader abstraction for spilled data;
 *
 * @param <S> - type of the source were data are stored;
 */
public interface SpillingDataInput<S> {
    /**
     * Open reader using corresponding inputStream;
     *
     * @param source - source to be used;
     */
    void open(S source);

    /**
     * Check and (prefetch if necessary)
     * if it is possible to read next bytes from this input;
     *
     * @param size - amount of bytes to read;
     * @return - true - it is possible to read next bytes;
     * false - otherwise;
     */
    boolean checkAvailable(int size);

    /**
     * @return - int value had been read;
     */
    int readNextInt();

    /**
     * @return - long value had been read;
     */
    long readNextLong();

    /**
     * Read blob of specified size to the corresponding MemoryBlock of spillingContext;
     */
    void readBlob(long offset, long size, MemoryBlock memoryBlock);

    /**
     * Read next bytes;
     *
     * @param size - amount of bytes to read;
     */
    void stepOnBytes(long size);

    /**
     * @return value of endianness;
     */
    boolean isBigEndian();

    /**
     * Closes current dataInput;
     */
    void close();
}
