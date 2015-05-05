/*
 * Copyright 2014 Real Logic Ltd.
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
package com.hazelcast.client.impl.protocol.util;

/**
 * Abstraction over a range of buffer types that allows type to be accessed with memory ordering semantics.
 */
public interface AtomicBuffer
        extends MutableDirectBuffer {
    /**
     * Get the value at a given index with volatile semantics.
     *
     * @param index The index in bytes from where to get the value.
     * @return The value retrieved from the given index.
     */
    long getLongVolatile(int index);

    /**
     * Put a value to a given index with volatile semantics.
     *
     * @param index The index in bytes for where to put the value.
     * @param value The value to put at the given index.
     */
    void putLongVolatile(int index, long value);

    /**
     * Put a value to a given index with ordered store semantics.
     *
     * @param index The index in bytes for where to put the value.
     * @param value The value to put at the given index.
     */
    void putLongOrdered(int index, long value);

    /**
     * Add a value to a given index with ordered store semantics. Use a negative increment to decrement.
     *
     * @param index     The index in bytes for where to increment or decrement the value.
     * @param increment The increment or decrement by which the value at the index will be adjusted.
     */
    void addLongOrdered(int index, long increment);

    /**
     * Performs an atomic compare and set of a long, given an expected value.
     *
     * @param index         The index in bytes for where to put the value.
     * @param expectedValue at to be compared   The expected value that is compared to the value at the index.
     * @param updateValue   The value to be exchanged with the value at the index.
     * @return true if the compare and put were successful, false otherwise.
     */
    boolean compareAndSetLong(int index, long expectedValue, long updateValue);

    /**
     * Get the value at a given index with volatile semantics.
     *
     * @param index The index in bytes from where to get the value.
     * @return The value retrieved from the given index.
     */
    int getIntVolatile(int index);

    /**
     * Put a value to a given index with volatile semantics.
     *
     * @param index The index in bytes for where to put the value.
     * @param value The value to put at the given index.
     */
    void putIntVolatile(int index, int value);

    /**
     * Put a value to a given index with ordered semantics.
     *
     * @param index The index in bytes for where to put the value.
     * @param value The value to put at the given index.
     */
    void putIntOrdered(int index, int value);

    /**
     * Add a value to a given index with ordered store semantics. Use a negative increment to decrement.
     *
     * @param index     The index in bytes for where to put the value.
     * @param increment by which the value at the index will be adjusted.
     */
    void addIntOrdered(int index, int increment);

    /**
     * Atomic compare and set of a int given an expected value.
     *
     * @param index         The index in bytes for where to put the value.
     * @param expectedValue at to be compared   The expected value that is compared to the value at the index.
     * @param updateValue   The value to be exchanged with the value at the index.
     * @return true if the compare and put were successful, false otherwise.
     */
    boolean compareAndSetInt(int index, int expectedValue, int updateValue);

    /**
     * Get the value at a given index with volatile semantics.
     *
     * @param index The index in bytes from where to get the value.
     * @return The value retrieved from the given index.
     */
    short getShortVolatile(int index);

    /**
     * Put a value to a given index with volatile semantics.
     *
     * @param index The index in bytes for where to put the value.
     * @param value The value to put at the given index.
     */
    void putShortVolatile(int index, short value);
}
