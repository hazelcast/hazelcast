/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.memory;

/**
 * Extends {@link HeapMemoryAccessor} with concurrent operations.
 */
@SuppressWarnings("checkstyle:methodcount")
public interface ConcurrentHeapMemoryAccessor extends HeapMemoryAccessor {

    /**
     * Reads the boolean value as volatile from given object by its offset.
     *
     * @param base   the object where boolean value will be read from
     * @param offset offset from object's base to the accessed location
     * @return the boolean value that was read
     */
    boolean getBooleanVolatile(Object base, long offset);

    /**
     * Writes the boolean value as volatile to given object by its offset.
     *
     * @param base   the object where boolean value will be written to
     * @param offset offset from object's base to the accessed location
     * @param x      the boolean value to be written
     */
    void putBooleanVolatile(Object base, long offset, boolean x);

    /**
     * Reads the byte value as volatile from given object by its offset.
     *
     * @param base   the object where the byte value will be read from
     * @param offset offset from object's base to the accessed location
     * @return the byte value that was read
     */
    byte getByteVolatile(Object base, long offset);

    /**
     * Writes the byte value as volatile to given object by its offset.
     *
     * @param base   the object where the byte value will be written to
     * @param offset offset from object's base to the accessed location
     * @param x      the byte value to be written
     */
    void putByteVolatile(Object base, long offset, byte x);

    /**
     * Reads the char value as volatile from given object by its offset.
     *
     * @param base   the object where char value will be read from
     * @param offset offset from object's base to the accessed location
     * @return the char value that was read
     */
    char getCharVolatile(Object base, long offset);

    /**
     * Writes the char value as volatile to given object by its offset.
     *
     * @param base   the object where char value will be written to
     * @param offset offset from object's base to the accessed location
     * @param x      the char value to be written
     */
    void putCharVolatile(Object base, long offset, char x);

    /**
     * Reads the short value as volatile from given object by its offset.
     *
     * @param base   the object where short value will be read from
     * @param offset offset from object's base to the accessed location
     * @return the short value that ws read
     */
    short getShortVolatile(Object base, long offset);

    /**
     * Writes the short value as volatile to given object by its offset.
     *
     * @param base   the object where short value will be written to
     * @param offset offset from object's base to the accessed location
     * @param x      the short value to be written
     */
    void putShortVolatile(Object base, long offset, short x);

    /**
     * Reads the int value as volatile from given object by its offset.
     *
     * @param base   the object where int value will be read from
     * @param offset offset from object's base to the accessed location
     * @return the int value that was read
     */
    int getIntVolatile(Object base, long offset);

    /**
     * Writes the int value as volatile to given object by its offset.
     *
     * @param base   the object where int value will be written to
     * @param offset offset from object's base to the accessed location
     * @param x      the int value to be written
     */
    void putIntVolatile(Object base, long offset, int x);

    /**
     * Reads the float value as volatile from given object by its offset.
     *
     * @param base   the object where float value will be read from
     * @param offset offset from object's base to the accessed location
     * @return the float value that was read
     */
    float getFloatVolatile(Object base, long offset);

    /**
     * Writes the float value as volatile to given object by its offset.
     *
     * @param base   the object where float value will be written to
     * @param offset offset from object's base to the accessed location
     * @param x      the float value to be written
     */
    void putFloatVolatile(Object base, long offset, float x);

    /**
     * Reads the long value as volatile from given object by its offset.
     *
     * @param base   the object where long value will be read from
     * @param offset offset from object's base to the accessed location
     * @return the long value that was read
     */
    long getLongVolatile(Object base, long offset);

    /**
     * Writes the long value as volatile to given object by its offset.
     *
     * @param base   the object where long value will be written to
     * @param offset offset from object's base to the accessed location
     * @param x      the long value to be written
     */
    void putLongVolatile(Object base, long offset, long x);

    /**
     * Reads the double value as volatile from given object by its offset.
     *
     * @param base   the object where double value will be read from
     * @param offset offset from object's base to the accessed location
     * @return the double value that was read
     */
    double getDoubleVolatile(Object base, long offset);

    /**
     * Writes the double value as volatile to given object by its offset.
     *
     * @param base   the object where double value will be written to
     * @param offset offset from object's base to the accessed location
     * @param x      the double value to be written
     */
    void putDoubleVolatile(Object base, long offset, double x);

    /**
     * Gets the referenced object from given owner object as volatile by its offset.
     *
     * @param base   the owner object where the referenced object will be read from
     * @param offset offset from object's base to the accessed location
     * @return the retrieved referenced object
     */
    Object getObjectVolatile(Object base, long offset);

    /**
     * Puts the referenced object to given owner object as volatile by its offset.
     *
     * @param base   the owner object where the referenced object will be written to
     * @param offset offset from object's base to the accessed location
     * @param x      the referenced object to be written
     */
    void putObjectVolatile(Object base, long offset, Object x);

    /**
     * Compares and swaps an int value to an expected value atomically
     * based by given object with given offset,
     * if and only if its current value is equal to the expected value.
     *
     * @param base     the object where int value will be written to
     * @param offset   offset from object's base to the accessed location
     * @param expected the expected current int value to be set new int value
     * @param x        the int value to be written
     * @return <tt>true</tt> if CAS is successful, <tt>false</tt> otherwise
     */
    boolean compareAndSwapInt(Object base, long offset, int expected, int x);

    /**
     * Compares and swaps a long value to an expected value atomically
     * based by given object with given offset
     * if and only if its current value equals to the expected value.
     *
     * @param base     the object where long value will be written to
     * @param offset   offset from object's base to the accessed location
     * @param expected the expected current long value to be set new long value
     * @param x        the long value to be written
     * @return <tt>true</tt> if CAS is successful, <tt>false</tt> otherwise
     */
    boolean compareAndSwapLong(Object base, long offset, long expected, long x);

    /**
     * Compares and swaps referenced object to expected object atomically
     * based by given owner object at given offset
     * if and only if its current object is the expected object.
     *
     * @param base     the owner object where the referenced object will be written to
     * @param offset   offset from object's base to the accessed location
     * @param expected the expected current referenced object to be set to new referenced object
     * @param x        the new referenced object that will be written
     * @return <tt>true</tt> if CAS is successful, <tt>false</tt> otherwise
     */
    boolean compareAndSwapObject(Object base, long offset, Object expected, Object x);

    /**
     * Puts the given int value as ordered to the CPU write buffer
     * based by the given object at the given offset.
     *
     * @param base   the object where the int value will be written to
     * @param offset offset from object's base to the accessed location
     * @param x      the int value to be written
     */
    void putOrderedInt(Object base, long offset, int x);

    /**
     * Puts the given long value as ordered to the CPU write buffer
     * based by the given object at the given offset.
     *
     * @param base   the object where the long value will be written to
     * @param offset offset from object's base to the accessed location
     * @param x      the long value to be written
     */
    void putOrderedLong(Object base, long offset, long x);

    /**
     * Puts the given referenced object as ordered to the CPU write buffer
     * based by the given owner object at the given offset.
     *
     * @param base   the owner object where the referenced object will be written to
     * @param offset offset from object's base to the accessed location
     * @param x      the referenced object to be written
     */
    void putOrderedObject(Object base, long offset, Object x);
}
