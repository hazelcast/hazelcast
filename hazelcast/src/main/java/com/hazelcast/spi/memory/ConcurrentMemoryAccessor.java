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

package com.hazelcast.spi.memory;

/**
 * Extension of {@link MemoryAccessor} for memory operations with concurrent semantics:
 * volatile, ordered, and CAS.
 */
public interface ConcurrentMemoryAccessor extends MemoryAccessor {


    /**
     * Reads the boolean value as volatile from given object by its offset.
     *
     * @param address  address to access
     * @return the read value
     */
    boolean getBooleanVolatile(long address);

    /**
     * Writes the boolean value as volatile to given object by its offset.
     *
     * @param address  address to access
     * @param x        the boolean value to be written
     */
    void putBooleanVolatile(long address, boolean x);

    /**
     * Reads the byte value as volatile from given object by its offset.
     *
     * @param address  address to access
     * @return the read value
     */
    byte getByteVolatile(long address);

    /**
     * Writes the byte value as volatile to given object by its offset.
     *
     * @param address  address to access
     * @param x        the byte value to be written
     */
    void putByteVolatile(long address, byte x);

    /**
     * Reads the char value as volatile from given object by its offset.
     *
     * @param address  address to access
     * @return the read value
     */
    char getCharVolatile(long address);

    /**
     * Writes the char value as volatile to given object by its offset.
     *
     * @param address  address to access
     * @param x        the char value to be written
     */
    void putCharVolatile(long address, char x);

    /**
     * Reads the short value as volatile from given object by its offset.
     *
     * @param address  address to access
     * @return the read value
     */
    short getShortVolatile(long address);

    /**
     * Writes the short value as volatile to given object by its offset.
     *
     * @param address  address to access
     * @param x        the short value to be written
     */
    void putShortVolatile(long address, short x);

    /**
     * Reads the int value as volatile from given object by its offset.
     *
     * @param address  address to access
     * @return the read value
     */
    int getIntVolatile(long address);

    /**
     * Writes the int value as volatile to given object by its offset.
     *
     * @param address  address to access
     * @param x        the int value to be written
     */
    void putIntVolatile(long address, int x);

    /**
     * Reads the float value as volatile from given object by its offset.
     *
     * @param address  address to access
     * @return the read value
     */
    float getFloatVolatile(long address);

    /**
     * Writes the float value as volatile to given object by its offset.
     *
     * @param address  address to access
     * @param x        the float value to be written
     */
    void putFloatVolatile(long address, float x);

    /**
     * Reads the long value as volatile from given object by its offset.
     *
     * @param address  address to access
     * @return the read value
     */
    long getLongVolatile(long address);

    /**
     * Writes the long value as volatile to given object by its offset.
     *
     * @param address  address to access
     * @param x        the long value to be written
     */
    void putLongVolatile(long address, long x);

    /**
     * Reads the double value as volatile from given object by its offset.
     *
     * @param address  address to access
     * @return the read value
     */
    double getDoubleVolatile(long address);

    /**
     * Writes the double value as volatile to given object by its offset.
     *
     * @param address  address to access
     * @param x        the double value to be written
     */
    void putDoubleVolatile(long address, double x);


    /**
     * Compares and swaps int value to specified value atomically
     * based by given object with given offset
     * if and only if its current value equals to specified expected value.
     *
     * @param address  address to access
     * @param expected the expected current int value to be set new int value
     * @param x        the int value to be written
     * @return <tt>true</tt> if CAS is successful, <tt>false</tt> otherwise
     */
    boolean compareAndSwapInt(long address, int expected, int x);

    /**
     * Compares and swaps long value to specified value atomically
     * based by given object with given offset
     * if and only if its current value equals to specified expected value.
     *
     * @param address  address to access
     * @param expected the expected current long value to be set new long value
     * @param x        the long value to be written
     * @return <tt>true</tt> if CAS is successful, <tt>false</tt> otherwise
     */
    boolean compareAndSwapLong(long address, long expected, long x);

    /**
     * Compares and swaps referenced object to specified object atomically
     * based by given owner object at given offset
     * if and only if its current object is the specified object.
     *
     * @param address  address to access
     * @param expected the expected current referenced object to be set new referenced object
     * @param x        the referenced object to be written
     * @return <tt>true</tt> if CAS is successful, <tt>false</tt> otherwise
     */
    boolean compareAndSwapObject(long address, Object expected, Object x);

    /**
     * Puts given int value as ordered to CPU write buffer
     * based by given object at given offset.
     *
     * @param address  address to access
     * @param x        the int value to be written
     */
    void putOrderedInt(long address, int x);

    /**
     * Puts given long value as ordered to CPU write buffer
     * based by given object at given offset.
     *
     * @param address  address to access
     * @param x        the long value to be written
     */
    void putOrderedLong(long address, long x);

    /**
     * Puts given referenced object as ordered to CPU write buffer
     * based by given owner object at given offset.
     *
     * @param address  address to access
     * @param x        the referenced object to be written
     */
    void putOrderedObject(long address, Object x);
}
