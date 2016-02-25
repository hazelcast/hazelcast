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

package com.hazelcast.internal.memory;

import java.lang.reflect.Field;

/**
 * Abstract entity which provides different way to access data from external resources
 *
 * @param <R> - type of resource
 */
@SuppressWarnings("checkstyle:methodcount")
public interface MemoryAccessStrategy<R> extends ByteMemoryAccessStrategy<R> {
    MemoryAccessStrategy<Object> STANDARD_MEM =
            MemoryAccessorStrategyProvider.getMemoryAccessStrategy(MemoryAccessStrategyType.STANDARD);

    MemoryAccessStrategy<Object> STANDARD_ALIGNED_MEM =
            MemoryAccessorStrategyProvider.getMemoryAccessStrategy(MemoryAccessStrategyType.STANDARD_ALIGNMENT);

    MemoryAccessStrategy<byte[]> HEAP_BYTE_ARRAY_MEM =
            MemoryAccessorStrategyProvider.getMemoryAccessStrategy(MemoryAccessStrategyType.HEAP_BYTE_ARRAY);

    MemoryAccessStrategy<Object> MEM =
            MemoryAccessorStrategyProvider.getMemoryAccessStrategy(MemoryAccessStrategyType.PLATFORM_AWARE);

    /////////////////////////////////////////////////////////////////////////

    /**
     * Base offset of boolean[]
     */
    int ARRAY_BOOLEAN_BASE_OFFSET = STANDARD_MEM != null ? STANDARD_MEM.arrayBaseOffset(boolean[].class) : -1;

    /**
     * Base offset of byte[]
     */
    int ARRAY_BYTE_BASE_OFFSET = STANDARD_MEM != null ? STANDARD_MEM.arrayBaseOffset(byte[].class) : -1;

    /**
     * Base offset of short[]
     */
    int ARRAY_SHORT_BASE_OFFSET = STANDARD_MEM != null ? STANDARD_MEM.arrayBaseOffset(short[].class) : -1;

    /**
     * Base offset of char[]
     */
    int ARRAY_CHAR_BASE_OFFSET = STANDARD_MEM != null ? STANDARD_MEM.arrayBaseOffset(char[].class) : -1;

    /**
     * Base offset of int[]
     */
    int ARRAY_INT_BASE_OFFSET = STANDARD_MEM != null ? STANDARD_MEM.arrayBaseOffset(int[].class) : -1;

    /**
     * Base offset of float[]
     */
    int ARRAY_FLOAT_BASE_OFFSET = STANDARD_MEM != null ? STANDARD_MEM.arrayBaseOffset(float[].class) : -1;

    /**
     * Base offset of long[]
     */
    int ARRAY_LONG_BASE_OFFSET = STANDARD_MEM != null ? STANDARD_MEM.arrayBaseOffset(long[].class) : -1;

    /**
     * Base offset of double[]
     */
    int ARRAY_DOUBLE_BASE_OFFSET = STANDARD_MEM != null ? STANDARD_MEM.arrayBaseOffset(double[].class) : -1;

    /**
     * Base offset of any type of Object[]
     */
    int ARRAY_OBJECT_BASE_OFFSET = STANDARD_MEM != null ? STANDARD_MEM.arrayBaseOffset(Object[].class) : -1;

    /////////////////////////////////////////////////////////////////////////

    /**
     * Index scale of boolean[]
     */
    int ARRAY_BOOLEAN_INDEX_SCALE = STANDARD_MEM != null ? STANDARD_MEM.arrayIndexScale(boolean[].class) : -1;

    /**
     * Index scale of byte[]
     */
    int ARRAY_BYTE_INDEX_SCALE = STANDARD_MEM != null ? STANDARD_MEM.arrayIndexScale(byte[].class) : -1;

    /**
     * Index scale of short[]
     */
    int ARRAY_SHORT_INDEX_SCALE = STANDARD_MEM != null ? STANDARD_MEM.arrayIndexScale(short[].class) : -1;

    /**
     * Index scale of char[]
     */
    int ARRAY_CHAR_INDEX_SCALE = STANDARD_MEM != null ? STANDARD_MEM.arrayIndexScale(char[].class) : -1;

    /**
     * Index scale of int[]
     */
    int ARRAY_INT_INDEX_SCALE = STANDARD_MEM != null ? STANDARD_MEM.arrayIndexScale(int[].class) : -1;

    /**
     * Index scale of float[]
     */
    int ARRAY_FLOAT_INDEX_SCALE = STANDARD_MEM != null ? STANDARD_MEM.arrayIndexScale(float[].class) : -1;

    /**
     * Index scale of long[]
     */
    int ARRAY_LONG_INDEX_SCALE = STANDARD_MEM != null ? STANDARD_MEM.arrayIndexScale(long[].class) : -1;

    /**
     * Index scale of double[]
     */
    int ARRAY_DOUBLE_INDEX_SCALE = STANDARD_MEM != null ? STANDARD_MEM.arrayIndexScale(double[].class) : -1;

    /**
     * Index scale of any type of Object[]
     */
    int ARRAY_OBJECT_INDEX_SCALE = STANDARD_MEM != null ? STANDARD_MEM.arrayIndexScale(Object[].class) : -1;

    /////////////////////////////////////////////////////////////////////////

    /**
     * Gets the offset of given field.
     *
     * @param field the field whose offset is requested
     * @return the offset of given field
     */
    long objectFieldOffset(Field field);

    /**
     * Gets the base offset of the array typed with given class.
     *
     * @param arrayClass the type of the array whose base offset is requested
     * @return the base offset of the array typed with given class
     */
    int arrayBaseOffset(Class<?> arrayClass);

    /**
     * Gets the index scale of the array typed with given class.
     *
     * @param arrayClass the type of the array whose index scale is requested
     * @return the index scale of the array typed with given class
     */
    int arrayIndexScale(Class<?> arrayClass);

    /**
     * Copies memory from given source object by given source offset
     * to given destination object by given destination offset as given size.
     * <p>
     * <p>
     * NOTE:
     * Destination object can only be <tt>byte[]</tt> or <tt>null</tt>.
     * But source object can be any object or <tt>null</tt>.
     * </p>
     *
     * @param srcObj     the source object to be copied from
     * @param srcOffset  the source offset relative to object itself to be copied from
     * @param destObj    the destination object to be copied to
     * @param destOffset the destination offset relative to object itself to be copied to
     * @param bytes      the number of bytes to be copied
     */
    void copyMemory(R srcObj, long srcOffset, R destObj, long destOffset, long bytes);


    /**
     * Reads the boolean value from given object by its offset.
     *
     * @param resource the object where boolean value will be read from
     * @param offset   the offset of boolean field relative to object itself
     * @return the read value
     */
    boolean getBoolean(R resource, long offset);


    /**
     * Reads the boolean value as volatile from given object by its offset.
     *
     * @param resource the object where boolean value will be read from
     * @param offset   the offset of boolean field relative to object itself
     * @return the read value
     */
    boolean getBooleanVolatile(R resource, long offset);

    /**
     * Writes the boolean value to given object by its offset.
     *
     * @param resource the object where boolean value will be written to
     * @param offset   the offset of boolean field relative to object itself
     * @param x        the boolean value to be written
     */
    void putBoolean(R resource, long offset, boolean x);

    /**
     * Writes the boolean value as volatile to given object by its offset.
     *
     * @param resource the object where boolean value will be written to
     * @param offset   the offset of boolean field relative to object itself
     * @param x        the boolean value to be written
     */
    void putBooleanVolatile(R resource, long offset, boolean x);

    /**
     * Reads the byte value from given object by its offset.
     *
     * @param resource the object where byte value will be read from
     * @param offset   the offset of byte field relative to object itself
     * @return the read value
     */
    byte getByte(R resource, long offset);

    /**
     * Reads the byte value as volatile from given object by its offset.
     *
     * @param resource the object where byte value will be read from
     * @param offset   the offset of byte field relative to object itself
     * @return the read value
     */
    byte getByteVolatile(R resource, long offset);

    /**
     * Writes the byte value to given object by its offset.
     *
     * @param resource the object where byte value will be written to
     * @param offset   the offset of byte field relative to object itself
     * @param x        the byte value to be written
     */
    void putByte(R resource, long offset, byte x);

    /**
     * Writes the byte value as volatile to given object by its offset.
     *
     * @param resource the object where byte value will be written to
     * @param offset   the offset of byte field relative to object itself
     * @param x        the byte value to be written
     */
    void putByteVolatile(R resource, long offset, byte x);

    /**
     * Reads the char value from given object by its offset.
     *
     * @param resource the object where char value will be read from
     * @param offset   the offset of char field relative to object itself
     * @return the read value
     */
    char getChar(R resource, long offset);

    /**
     * Reads the char value as volatile from given object by its offset.
     *
     * @param resource the object where char value will be read from
     * @param offset   the offset of char field relative to object itself
     * @return the read value
     */
    char getCharVolatile(R resource, long offset);

    /**
     * Writes the char value to given object by its offset.
     *
     * @param resource the object where char value will be written to
     * @param offset   the offset of char field relative to object itself
     * @param x        the char value to be written
     */
    void putChar(R resource, long offset, char x);

    /**
     * Writes the char value as volatile to given object by its offset.
     *
     * @param resource the object where char value will be written to
     * @param offset   the offset of char field relative to object itself
     * @param x        the char value to be written
     */
    void putCharVolatile(R resource, long offset, char x);

    /**
     * Reads the short value from given object by its offset.
     *
     * @param resource the object where short value will be read from
     * @param offset   the offset of short field relative to object itself
     * @return the read value
     */
    short getShort(R resource, long offset);

    /**
     * Reads the short value as volatile from given object by its offset.
     *
     * @param resource the object where short value will be read from
     * @param offset   the offset of short field relative to object itself
     * @return the read value
     */
    short getShortVolatile(R resource, long offset);

    /**
     * Writes the short value to given object by its offset.
     *
     * @param resource the object where short value will be written to
     * @param offset   the offset of short field relative to object itself
     * @param x        the short value to be written
     */
    void putShort(R resource, long offset, short x);

    /**
     * Writes the short value as volatile to given object by its offset.
     *
     * @param resource the object where short value will be written to
     * @param offset   the offset of short field relative to object itself
     * @param x        the short value to be written
     */
    void putShortVolatile(R resource, long offset, short x);

    /**
     * Reads the int value from given object by its offset.
     *
     * @param resource the object where int value will be read from
     * @param offset   the offset of int field relative to object itself
     * @return the read value
     */
    int getInt(R resource, long offset);

    /**
     * Reads the int value as volatile from given object by its offset.
     *
     * @param resource the object where int value will be read from
     * @param offset   the offset of int field relative to object itself
     * @return the read value
     */
    int getIntVolatile(R resource, long offset);

    /**
     * Writes the int value to given object by its offset.
     *
     * @param resource the object where int value will be written to
     * @param offset   the offset of int field relative to object itself
     * @param x        the int value to be written
     */
    void putInt(R resource, long offset, int x);

    /**
     * Writes the int value as volatile to given object by its offset.
     *
     * @param resource the object where int value will be written to
     * @param offset   the offset of int field relative to object itself
     * @param x        the int value to be written
     */
    void putIntVolatile(R resource, long offset, int x);

    /**
     * Reads the float value from given object by its offset.
     *
     * @param resource the object where float value will be read from
     * @param offset   the offset of float field relative to object itself
     * @return the read value
     */
    float getFloat(R resource, long offset);

    /**
     * Reads the float value as volatile from given object by its offset.
     *
     * @param resource the object where float value will be read from
     * @param offset   the offset of float field relative to object itself
     * @return the read value
     */
    float getFloatVolatile(R resource, long offset);


    /**
     * Writes the float value to given object by its offset.
     *
     * @param resource the object where float value will be written to
     * @param offset   the offset of float field relative to object itself
     * @param x        the float value to be written
     */
    void putFloat(R resource, long offset, float x);

    /**
     * Writes the float value as volatile to given object by its offset.
     *
     * @param resource the object where float value will be written to
     * @param offset   the offset of float field relative to object itself
     * @param x        the float value to be written
     */
    void putFloatVolatile(R resource, long offset, float x);

    /**
     * Reads the long value from given object by its offset.
     *
     * @param resource the object where long value will be read from
     * @param offset   the offset of long field relative to object itself
     * @return the read value
     */
    long getLong(R resource, long offset);

    /**
     * Reads the long value as volatile from given object by its offset.
     *
     * @param resource the object where long value will be read from
     * @param offset   the offset of long field relative to object itself
     * @return the read value
     */
    long getLongVolatile(R resource, long offset);


    /**
     * Writes the long value to given object by its offset.
     *
     * @param resource the object where long value will be written to
     * @param offset   the offset of long field relative to object itself
     * @param x        the long value to be written
     */
    void putLong(R resource, long offset, long x);

    /**
     * Writes the long value as volatile to given object by its offset.
     *
     * @param resource the object where long value will be written to
     * @param offset   the offset of long field relative to object itself
     * @param x        the long value to be written
     */
    void putLongVolatile(R resource, long offset, long x);

    /**
     * Reads the double value from given object by its offset.
     *
     * @param resource the object where double value will be read from
     * @param offset   the offset of double field relative to object itself
     * @return the read value
     */
    double getDouble(R resource, long offset);

    /**
     * Reads the double value as volatile from given object by its offset.
     *
     * @param resource the object where double value will be read from
     * @param offset   the offset of double field relative to object itself
     * @return the read value
     */
    double getDoubleVolatile(R resource, long offset);


    /**
     * Writes the double value to given object by its offset.
     *
     * @param resource the object where double value will be written to
     * @param offset   the offset of double field relative to object itself
     * @param x        the double value to be written
     */
    void putDouble(R resource, long offset, double x);

    /**
     * Writes the double value as volatile to given object by its offset.
     *
     * @param resource the object where double value will be written to
     * @param offset   the offset of double field relative to object itself
     * @param x        the double value to be written
     */
    void putDoubleVolatile(R resource, long offset, double x);

    /////////////////////////////////////////////////////////////////////////

    /**
     * Gets the referenced object from given owner object by its offset.
     *
     * @param resource the owner object where the referenced object will be read from
     * @param offset   the offset of the referenced object field relative to owner object itself
     * @return the retrieved referenced object
     */
    Object getObject(R resource, long offset);

    /**
     * Gets the referenced object from given owner object as volatile by its offset.
     *
     * @param resource the owner object where the referenced object will be read from
     * @param offset   the offset of the referenced object field relative to owner object itself
     * @return the retrieved referenced object
     */
    Object getObjectVolatile(R resource, long offset);

    /**
     * Puts the referenced object to given owner object by its offset.
     *
     * @param resource the owner object where the referenced object will be written to
     * @param offset   the offset of the referenced object field relative to owner object itself
     * @param x        the referenced object to be written
     */
    void putObject(R resource, long offset, Object x);

    /**
     * Puts the referenced object to given owner object as volatile by its offset.
     *
     * @param resource the owner object where the referenced object will be written to
     * @param offset   the offset of the referenced object field relative to owner object itself
     * @param x        the referenced object to be written
     */
    void putObjectVolatile(R resource, long offset, Object x);

    /////////////////////////////////////////////////////////////////////////

    /**
     * Compares and swaps int value to specified value atomically
     * based by given object with given offset
     * if and only if its current value equals to specified expected value.
     *
     * @param resource the object where int value will be written to
     * @param offset   the offset of int field relative to object itself
     * @param expected the expected current int value to be set new int value
     * @param x        the int value to be written
     * @return
     */
    boolean compareAndSwapInt(R resource, long offset, int expected, int x);

    /**
     * Compares and swaps long value to specified value atomically
     * based by given object with given offset
     * if and only if its current value equals to specified expected value.
     *
     * @param resource the object where long value will be written to
     * @param offset   the offset of long field relative to object itself
     * @param expected the expected current long value to be set new long value
     * @param x        the long value to be written
     * @return <tt>true</tt> if CAS is successful, <tt>false</tt> otherwise
     */
    boolean compareAndSwapLong(R resource, long offset, long expected, long x);

    /**
     * Compares and swaps referenced object to specified object atomically
     * based by given owner object at given offset
     * if and only if its current object is the specified object.
     *
     * @param resource the owner object where the referenced object will be written to
     * @param offset   the offset of the referenced object field relative to owner object itself
     * @param expected the expected current referenced object to be set new referenced object
     * @param x        the referenced object to be written
     * @return <tt>true</tt> if CAS is successful, <tt>false</tt> otherwise
     */
    boolean compareAndSwapObject(R resource, long offset, Object expected, Object x);

    /////////////////////////////////////////////////////////////////////////

    /**
     * Puts given int value as ordered to CPU write buffer
     * based by given object at given offset.
     *
     * @param resource the object where int value will be written to
     * @param offset   the offset of int field relative to object itself
     * @param x        the int value to be written
     */
    void putOrderedInt(R resource, long offset, int x);

    /**
     * Puts given long value as ordered to CPU write buffer
     * based by given object at given offset.
     *
     * @param resource the object where long value will be written to
     * @param offset   the offset of long field relative to object itself
     * @param x        the long value to be written
     */
    void putOrderedLong(R resource, long offset, long x);

    /**
     * Puts given referenced object as ordered to CPU write buffer
     * based by given owner object at given offset.
     *
     * @param resource the owner object where the referenced object will be written to
     * @param offset   the offset of the referenced object field relative to owner object itself
     * @param x        the referenced object to be written
     */
    void putOrderedObject(R resource, long offset, Object x);
}
