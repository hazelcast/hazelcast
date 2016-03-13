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
 * Extends {@link HeapMemoryAccessor} with concurrent operations.
 */
@SuppressWarnings("checkstyle:methodcount")
public interface ConcurrentHeapMemoryAccessor extends HeapMemoryAccessor {

    /**
     * Reads the boolean value as volatile from given object by its offset.
     *
     * @param base   the object where boolean value will be read from
     * @param offset offset from object's base to the accessed location
     * @return the read value
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
     * @param base   the object where byte value will be read from
     * @param offset offset from object's base to the accessed location
     * @return the read value
     */
    byte getByteVolatile(Object base, long offset);

    /**
     * Writes the byte value as volatile to given object by its offset.
     *
     * @param base   the object where byte value will be written to
     * @param offset offset from object's base to the accessed location
     * @param x      the byte value to be written
     */
    void putByteVolatile(Object base, long offset, byte x);

    /**
     * Reads the char value as volatile from given object by its offset.
     *
     * @param base   the object where char value will be read from
     * @param offset offset from object's base to the accessed location
     * @return the read value
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
     * @return the read value
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
     * @return the read value
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
     * @return the read value
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
     * @return the read value
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
     * @return the read value
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
     * Compares and swaps int value to specified value atomically
     * based by given object with given offset
     * if and only if its current value equals to specified expected value.
     *
     * @param base     the object where int value will be written to
     * @param offset   offset from object's base to the accessed location
     * @param expected the expected current int value to be set new int value
     * @param x        the int value to be written
     * @return <tt>true</tt> if CAS is successful, <tt>false</tt> otherwise
     */
    boolean compareAndSwapInt(Object base, long offset, int expected, int x);

    /**
     * Compares and swaps long value to specified value atomically
     * based by given object with given offset
     * if and only if its current value equals to specified expected value.
     *
     * @param base     the object where long value will be written to
     * @param offset   offset from object's base to the accessed location
     * @param expected the expected current long value to be set new long value
     * @param x        the long value to be written
     * @return <tt>true</tt> if CAS is successful, <tt>false</tt> otherwise
     */
    boolean compareAndSwapLong(Object base, long offset, long expected, long x);

    /**
     * Compares and swaps referenced object to specified object atomically
     * based by given owner object at given offset
     * if and only if its current object is the specified object.
     *
     * @param base     the owner object where the referenced object will be written to
     * @param offset   offset from object's base to the accessed location
     * @param expected the expected current referenced object to be set new referenced object
     * @param x        the referenced object to be written
     * @return <tt>true</tt> if CAS is successful, <tt>false</tt> otherwise
     */
    boolean compareAndSwapObject(Object base, long offset, Object expected, Object x);

    /**
     * Puts given int value as ordered to CPU write buffer
     * based by given object at given offset.
     *
     * @param base   the object where int value will be written to
     * @param offset offset from object's base to the accessed location
     * @param x      the int value to be written
     */
    void putOrderedInt(Object base, long offset, int x);

    /**
     * Puts given long value as ordered to CPU write buffer
     * based by given object at given offset.
     *
     * @param base   the object where long value will be written to
     * @param offset offset from object's base to the accessed location
     * @param x      the long value to be written
     */
    void putOrderedLong(Object base, long offset, long x);

    /**
     * Puts given referenced object as ordered to CPU write buffer
     * based by given owner object at given offset.
     *
     * @param base   the owner object where the referenced object will be written to
     * @param offset offset from object's base to the accessed location
     * @param x      the referenced object to be written
     */
    void putOrderedObject(Object base, long offset, Object x);
}
