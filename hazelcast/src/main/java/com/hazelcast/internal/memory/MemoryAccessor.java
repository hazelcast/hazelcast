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

import static com.hazelcast.internal.memory.MemoryAccessorType.STANDARD;

/**
 * <p>
 * Contract point for direct memory access operations.
 * </p>
 *
 * <p>
 * A few notes on this contract point:
 * <ul>
 *      <li>
 *        Addresses are not required to be a native memory addresses.
 *        It depends on the actual {@link MemoryAccessor} implementation.
 *      </li>
 *      <li>
 *        {@link MemoryAccessor} implementations can be retrieved
 *        via {@link MemoryAccessorProvider} by specifying {@link MemoryAccessorType}.
 *      </li>
 *      <li>
 *        The default {@link MemoryAccessor} implementation
 *        can be accessed via {@link #MEM} field of this interface
 *        and its availability state can be checked via {@link #MEM_AVAILABLE} field.
 *      </li>
 * </ul>
 * </p>
 *
 * @see MemoryAccessorType
 * @see MemoryAccessorProvider
 */
public interface MemoryAccessor {

    /**
     * The default {@link MemoryAccessor} instance.
     */
    MemoryAccessor MEM = MemoryAccessorProvider.getDefaultMemoryAccessor();

    /**
     * The instance of {@link MemoryAccessor} which correctly handles only aligned memory access.
     * Requesting unaligned memory access from this instance will result in low-level JVM crash on
     * platforms which do not support it.
     */
    MemoryAccessor AMEM = MemoryAccessorProvider.getMemoryAccessor(STANDARD);

    /**
     * If this constant is {@code true}, then {@link MEM} refers to a usable {@code MemoryAccessor}
     * instance.
     */
    boolean MEM_AVAILABLE = MEM != null;

    /**
     * If this constant is {@code true}, then {@link AMEM} refers to a usable {@code MemoryAccessor}
     * instance.
     */
    boolean AMEM_AVAILABLE = AMEM != null;

    /////////////////////////////////////////////////////////////////////////

    /**
     * Maximum size of a block of memory to copy in a single low-level memory-copying
     * operation. The goal is to prevent large periods without a GC safepoint.
     */
    int MEM_COPY_THRESHOLD = 1024 * 1024;

    /**
     * Base offset of boolean[]
     */
    int ARRAY_BOOLEAN_BASE_OFFSET = MEM != null ? MEM.arrayBaseOffset(boolean[].class) : -1;

    /**
     * Base offset of byte[]
     */
    int ARRAY_BYTE_BASE_OFFSET = MEM != null ? MEM.arrayBaseOffset(byte[].class) : -1;

    /**
     * Base offset of short[]
     */
    int ARRAY_SHORT_BASE_OFFSET = MEM != null ? MEM.arrayBaseOffset(short[].class) : -1;

    /**
     * Base offset of char[]
     */
    int ARRAY_CHAR_BASE_OFFSET = MEM != null ? MEM.arrayBaseOffset(char[].class) : -1;

    /**
     * Base offset of int[]
     */
    int ARRAY_INT_BASE_OFFSET = MEM != null ? MEM.arrayBaseOffset(int[].class) : -1;

    /**
     * Base offset of float[]
     */
    int ARRAY_FLOAT_BASE_OFFSET = MEM != null ? MEM.arrayBaseOffset(float[].class) : -1;

    /**
     * Base offset of long[]
     */
    int ARRAY_LONG_BASE_OFFSET = MEM != null ? MEM.arrayBaseOffset(long[].class) : -1;

    /**
     * Base offset of double[]
     */
    int ARRAY_DOUBLE_BASE_OFFSET = MEM != null ? MEM.arrayBaseOffset(double[].class) : -1;

    /**
     * Base offset of any type of Object[]
     */
    int ARRAY_OBJECT_BASE_OFFSET = MEM != null ? MEM.arrayBaseOffset(Object[].class) : -1;

    /////////////////////////////////////////////////////////////////////////

    /**
     * Index scale of boolean[]
     */
    int ARRAY_BOOLEAN_INDEX_SCALE = MEM != null ? MEM.arrayIndexScale(boolean[].class) : -1;

    /**
     * Index scale of byte[]
     */
    int ARRAY_BYTE_INDEX_SCALE = MEM != null ? MEM.arrayIndexScale(byte[].class) : -1;

    /**
     * Index scale of short[]
     */
    int ARRAY_SHORT_INDEX_SCALE = MEM != null ? MEM.arrayIndexScale(short[].class) : -1;

    /**
     * Index scale of char[]
     */
    int ARRAY_CHAR_INDEX_SCALE = MEM != null ? MEM.arrayIndexScale(char[].class) : -1;

    /**
     * Index scale of int[]
     */
    int ARRAY_INT_INDEX_SCALE = MEM != null ? MEM.arrayIndexScale(int[].class) : -1;

    /**
     * Index scale of float[]
     */
    int ARRAY_FLOAT_INDEX_SCALE = MEM != null ? MEM.arrayIndexScale(float[].class) : -1;

    /**
     * Index scale of long[]
     */
    int ARRAY_LONG_INDEX_SCALE = MEM != null ? MEM.arrayIndexScale(long[].class) : -1;

    /**
     * Index scale of double[]
     */
    int ARRAY_DOUBLE_INDEX_SCALE = MEM != null ? MEM.arrayIndexScale(double[].class) : -1;

    /**
     * Index scale of any type of Object[]
     */
    int ARRAY_OBJECT_INDEX_SCALE = MEM != null ? MEM.arrayIndexScale(Object[].class) : -1;

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

    /////////////////////////////////////////////////////////////////////////

    /**
     * Copies memory from given source address to given destination address
     * as given size.
     *
     * @param srcAddress    the source address to be copied from
     * @param destAddress   the destination address to be copied to
     * @param bytes         the number of bytes to be copied
     */
    void copyMemory(long srcAddress, long destAddress, long bytes);

    /**
     * Copies memory from given source object by given source offset
     * to given destination object by given destination offset as given size.
     *
     * <p>
     *  NOTE:
     *      Destination object can only be <tt>byte[]</tt> or <tt>null</tt>.
     *      But source object can be any object or <tt>null</tt>.
     * </p>
     * @param srcObj        the source object to be copied from
     * @param srcOffset     the source offset relative to object itself to be copied from
     * @param destObj       the destination object to be copied to
     * @param destOffset    the destination offset relative to object itself to be copied to
     * @param bytes         the number of bytes to be copied
     */
    void copyMemory(Object srcObj, long srcOffset, Object destObj, long destOffset, long bytes);

    /**
     * Sets memory with given value from specified address as given size.
     *
     * @param address   the start address of the memory region
     *                  which will be set with given value
     * @param bytes     the number of bytes to be set
     * @param value     the value to be set
     */
    void setMemory(long address, long bytes, byte value);

    /////////////////////////////////////////////////////////////////////////

    /**
     * Reads the boolean value from given address.
     *
     * @param address the address where boolean value will be read from
     * @return the read value
     */
    boolean getBoolean(long address);

    /**
     * Reads the boolean value from given object by its offset.
     *
     * @param o         the object where boolean value will be read from
     * @param offset    the offset of boolean field relative to object itself
     * @return the read value
     */
    boolean getBoolean(Object o, long offset);

    /**
     * Reads the boolean value as volatile from given object by its offset.
     *
     * @param o         the object where boolean value will be read from
     * @param offset    the offset of boolean field relative to object itself
     * @return the read value
     */
    boolean getBooleanVolatile(Object o, long offset);

    /////////////////////////////////////////////////////////////////////////

    /**
     * Writes the given boolean value to given address.
     *
     * @param address   the address where boolean value will be written to
     * @param x         the boolean value to be written
     */
    void putBoolean(long address, boolean x);

    /**
     * Writes the boolean value to given object by its offset.
     *
     * @param o         the object where boolean value will be written to
     * @param offset    the offset of boolean field relative to object itself
     * @param x         the boolean value to be written
     */
    void putBoolean(Object o, long offset, boolean x);

    /**
     * Writes the boolean value as volatile to given object by its offset.
     *
     * @param o         the object where boolean value will be written to
     * @param offset    the offset of boolean field relative to object itself
     * @param x         the boolean value to be written
     */
    void putBooleanVolatile(Object o, long offset, boolean x);

    /////////////////////////////////////////////////////////////////////////

    /**
     * Reads the byte value from given address.
     *
     * @param address the address where byte value will be read from
     * @return the read value
     */
    byte getByte(long address);

    /**
     * Reads the byte value from given object by its offset.
     *
     * @param o         the object where byte value will be read from
     * @param offset    the offset of byte field relative to object itself
     * @return the read value
     */
    byte getByte(Object o, long offset);

    /**
     * Reads the byte value as volatile from given object by its offset.
     *
     * @param o         the object where byte value will be read from
     * @param offset    the offset of byte field relative to object itself
     * @return the read value
     */
    byte getByteVolatile(Object o, long offset);

    /////////////////////////////////////////////////////////////////////////

    /**
     * Writes the given byte value to given address.
     *
     * @param address   the address where byte value will be written to
     * @param x         the byte value to be written
     */
    void putByte(long address, byte x);

    /**
     * Writes the byte value to given object by its offset.
     *
     * @param o         the object where byte value will be written to
     * @param offset    the offset of byte field relative to object itself
     * @param x         the byte value to be written
     */
    void putByte(Object o, long offset, byte x);

    /**
     * Writes the byte value as volatile to given object by its offset.
     *
     * @param o         the object where byte value will be written to
     * @param offset    the offset of byte field relative to object itself
     * @param x         the byte value to be written
     */
    void putByteVolatile(Object o, long offset, byte x);

    /////////////////////////////////////////////////////////////////////////

    /**
     * Reads the char value from given address.
     *
     * @param address the address where char value will be read from
     * @return the read value
     */
    char getChar(long address);

    /**
     * Reads the char value from given object by its offset.
     *
     * @param o         the object where char value will be read from
     * @param offset    the offset of char field relative to object itself
     * @return the read value
     */
    char getChar(Object o, long offset);

    /**
     * Reads the char value as volatile from given object by its offset.
     *
     * @param o         the object where char value will be read from
     * @param offset    the offset of char field relative to object itself
     * @return the read value
     */
    char getCharVolatile(Object o, long offset);

    /////////////////////////////////////////////////////////////////////////

    /**
     * Writes the given char value to given address.
     *
     * @param address   the address where char value will be written to
     * @param x         the char value to be written
     */
    void putChar(long address, char x);

    /**
     * Writes the char value to given object by its offset.
     *
     * @param o         the object where char value will be written to
     * @param offset    the offset of char field relative to object itself
     * @param x         the char value to be written
     */
    void putChar(Object o, long offset, char x);

    /**
     * Writes the char value as volatile to given object by its offset.
     *
     * @param o         the object where char value will be written to
     * @param offset    the offset of char field relative to object itself
     * @param x         the char value to be written
     */
    void putCharVolatile(Object o, long offset, char x);

    /////////////////////////////////////////////////////////////////////////

    /**
     * Reads the short value from given address.
     *
     * @param address the address where short value will be read from
     * @return the read value
     */
    short getShort(long address);

    /**
     * Reads the short value from given object by its offset.
     *
     * @param o         the object where short value will be read from
     * @param offset    the offset of short field relative to object itself
     * @return the read value
     */
    short getShort(Object o, long offset);

    /**
     * Reads the short value as volatile from given object by its offset.
     *
     * @param o         the object where short value will be read from
     * @param offset    the offset of short field relative to object itself
     * @return the read value
     */
    short getShortVolatile(Object o, long offset);

    /////////////////////////////////////////////////////////////////////////

    /**
     * Writes the given short value to given address.
     *
     * @param address   the address where short value will be written to
     * @param x         the short value to be written
     */
    void putShort(long address, short x);

    /**
     * Writes the short value to given object by its offset.
     *
     * @param o         the object where short value will be written to
     * @param offset    the offset of short field relative to object itself
     * @param x         the short value to be written
     */
    void putShort(Object o, long offset, short x);

    /**
     * Writes the short value as volatile to given object by its offset.
     *
     * @param o         the object where short value will be written to
     * @param offset    the offset of short field relative to object itself
     * @param x         the short value to be written
     */
    void putShortVolatile(Object o, long offset, short x);

    /////////////////////////////////////////////////////////////////////////

    /**
     * Reads the int value from given address.
     *
     * @param address the address where int value will be read from
     * @return the read value
     */
    int getInt(long address);

    /**
     * Reads the int value from given object by its offset.
     *
     * @param o         the object where int value will be read from
     * @param offset    the offset of int field relative to object itself
     * @return the read value
     */
    int getInt(Object o, long offset);

    /**
     * Reads the int value as volatile from given object by its offset.
     *
     * @param o         the object where int value will be read from
     * @param offset    the offset of int field relative to object itself
     * @return the read value
     */
    int getIntVolatile(Object o, long offset);

    /////////////////////////////////////////////////////////////////////////

    /**
     * Writes the given int value to given address.
     *
     * @param address   the address where int value will be written to
     * @param x         the int value to be written
     */
    void putInt(long address, int x);

    /**
     * Writes the int value to given object by its offset.
     *
     * @param o         the object where int value will be written to
     * @param offset    the offset of int field relative to object itself
     * @param x         the int value to be written
     */
    void putInt(Object o, long offset, int x);

    /**
     * Writes the int value as volatile to given object by its offset.
     *
     * @param o         the object where int value will be written to
     * @param offset    the offset of int field relative to object itself
     * @param x         the int value to be written
     */
    void putIntVolatile(Object o, long offset, int x);

    /////////////////////////////////////////////////////////////////////////

    /**
     * Reads the float value from given address.
     *
     * @param address the address where float value will be read from
     * @return the read value
     */
    float getFloat(long address);

    /**
     * Reads the float value from given object by its offset.
     *
     * @param o         the object where float value will be read from
     * @param offset    the offset of float field relative to object itself
     * @return the read value
     */
    float getFloat(Object o, long offset);

    /**
     * Reads the float value as volatile from given object by its offset.
     *
     * @param o         the object where float value will be read from
     * @param offset    the offset of float field relative to object itself
     * @return the read value
     */
    float getFloatVolatile(Object o, long offset);

    /////////////////////////////////////////////////////////////////////////

    /**
     * Writes the given float value to given address.
     *
     * @param address   the address where float value will be written to
     * @param x         the float value to be written
     */
    void putFloat(long address, float x);

    /**
     * Writes the float value to given object by its offset.
     *
     * @param o         the object where float value will be written to
     * @param offset    the offset of float field relative to object itself
     * @param x         the float value to be written
     */
    void putFloat(Object o, long offset, float x);

    /**
     * Writes the float value as volatile to given object by its offset.
     *
     * @param o         the object where float value will be written to
     * @param offset    the offset of float field relative to object itself
     * @param x         the float value to be written
     */
    void putFloatVolatile(Object o, long offset, float x);

    /////////////////////////////////////////////////////////////////////////

    /**
     * Reads the long value from given address.
     *
     * @param address the address where long value will be read from
     * @return the read value
     */
    long getLong(long address);

    /**
     * Reads the long value from given object by its offset.
     *
     * @param o         the object where long value will be read from
     * @param offset    the offset of long field relative to object itself
     * @return the read value
     */
    long getLong(Object o, long offset);

    /**
     * Reads the long value as volatile from given object by its offset.
     *
     * @param o         the object where long value will be read from
     * @param offset    the offset of long field relative to object itself
     * @return the read value
     */
    long getLongVolatile(Object o, long offset);

    /////////////////////////////////////////////////////////////////////////

    /**
     * Writes the given long value to given address.
     *
     * @param address   the address where long value will be written to
     * @param x         the long value to be written
     */
    void putLong(long address, long x);

    /**
     * Writes the long value to given object by its offset.
     *
     * @param o         the object where long value will be written to
     * @param offset    the offset of long field relative to object itself
     * @param x         the long value to be written
     */
    void putLong(Object o, long offset, long x);

    /**
     * Writes the long value as volatile to given object by its offset.
     *
     * @param o         the object where long value will be written to
     * @param offset    the offset of long field relative to object itself
     * @param x         the long value to be written
     */
    void putLongVolatile(Object o, long offset, long x);

    /////////////////////////////////////////////////////////////////////////

    /**
     * Reads the double value from given address.
     *
     * @param address the address where double value will be read from
     * @return the read value
     */
    double getDouble(long address);

    /**
     * Reads the double value from given object by its offset.
     *
     * @param o         the object where double value will be read from
     * @param offset    the offset of double field relative to object itself
     * @return the read value
     */
    double getDouble(Object o, long offset);

    /**
     * Reads the double value as volatile from given object by its offset.
     *
     * @param o         the object where double value will be read from
     * @param offset    the offset of double field relative to object itself
     * @return the read value
     */
    double getDoubleVolatile(Object o, long offset);

    /////////////////////////////////////////////////////////////////////////

    /**
     * Writes the given double value to given address.
     *
     * @param address   the address where double value will be written to
     * @param x         the double value to be written
     */
    void putDouble(long address, double x);

    /**
     * Writes the double value to given object by its offset.
     *
     * @param o         the object where double value will be written to
     * @param offset    the offset of double field relative to object itself
     * @param x         the double value to be written
     */
    void putDouble(Object o, long offset, double x);

    /**
     * Writes the double value as volatile to given object by its offset.
     *
     * @param o         the object where double value will be written to
     * @param offset    the offset of double field relative to object itself
     * @param x         the double value to be written
     */
    void putDoubleVolatile(Object o, long offset, double x);

    /////////////////////////////////////////////////////////////////////////

    /**
     * Gets the referenced object from given owner object by its offset.
     *
     * @param o         the owner object where the referenced object will be read from
     * @param offset    the offset of the referenced object field relative to owner object itself
     * @return the retrieved referenced object
     */
    Object getObject(Object o, long offset);

    /**
     * Gets the referenced object from given owner object as volatile by its offset.
     *
     * @param o         the owner object where the referenced object will be read from
     * @param offset    the offset of the referenced object field relative to owner object itself
     * @return the retrieved referenced object
     */
    Object getObjectVolatile(Object o, long offset);

    /////////////////////////////////////////////////////////////////////////

    /**
     * Puts the referenced object to given owner object by its offset.
     *
     * @param o         the owner object where the referenced object will be written to
     * @param offset    the offset of the referenced object field relative to owner object itself
     * @param x         the referenced object to be written
     */
    void putObject(Object o, long offset, Object x);

    /**
     * Puts the referenced object to given owner object as volatile by its offset.
     *
     * @param o         the owner object where the referenced object will be written to
     * @param offset    the offset of the referenced object field relative to owner object itself
     * @param x         the referenced object to be written
     */
    void putObjectVolatile(Object o, long offset, Object x);

    /////////////////////////////////////////////////////////////////////////

    /**
     * Compares and swaps int value to specified value atomically
     * based by given object with given offset
     * if and only if its current value equals to specified expected value.
     *
     * @param o         the object where int value will be written to
     * @param offset    the offset of int field relative to object itself
     * @param expected  the expected current int value to be set new int value
     * @param x         the int value to be written
     * @return
     */
    boolean compareAndSwapInt(Object o, long offset, int expected, int x);

    /**
     * Compares and swaps long value to specified value atomically
     * based by given object with given offset
     * if and only if its current value equals to specified expected value.
     *
     * @param o         the object where long value will be written to
     * @param offset    the offset of long field relative to object itself
     * @param expected  the expected current long value to be set new long value
     * @param x         the long value to be written
     * @return <tt>true</tt> if CAS is successful, <tt>false</tt> otherwise
     */
    boolean compareAndSwapLong(Object o, long offset, long expected, long x);

    /**
     * Compares and swaps referenced object to specified object atomically
     * based by given owner object at given offset
     * if and only if its current object is the specified object.
     *
     * @param o         the owner object where the referenced object will be written to
     * @param offset    the offset of the referenced object field relative to owner object itself
     * @param expected  the expected current referenced object to be set new referenced object
     * @param x         the referenced object to be written
     * @return <tt>true</tt> if CAS is successful, <tt>false</tt> otherwise
     */
    boolean compareAndSwapObject(Object o, long offset, Object expected, Object x);

    /////////////////////////////////////////////////////////////////////////

    /**
     * Puts given int value as ordered to CPU write buffer
     * based by given object at given offset.
     *
     * @param o         the object where int value will be written to
     * @param offset    the offset of int field relative to object itself
     * @param x         the int value to be written
     */
    void putOrderedInt(Object o, long offset, int x);

    /**
     * Puts given long value as ordered to CPU write buffer
     * based by given object at given offset.
     *
     * @param o         the object where long value will be written to
     * @param offset    the offset of long field relative to object itself
     * @param x         the long value to be written
     */
    void putOrderedLong(Object o, long offset, long x);

    /**
     * Puts given referenced object as ordered to CPU write buffer
     * based by given owner object at given offset.
     *
     * @param o         the owner object where the referenced object will be written to
     * @param offset    the offset of the referenced object field relative to owner object itself
     * @param x         the referenced object to be written
     */
    void putOrderedObject(Object o, long offset, Object x);

}
