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

import java.lang.reflect.Field;

import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.MEM;
import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.MEM_AVAILABLE;

/**
 * Accessor of Java heap memory. Heap must be addressed as an offset from a given object's base address.
 */
public interface HeapMemoryAccessor extends ByteAccessStrategy<Object> {

    /**
     * Base offset of boolean[]
     */
    int ARRAY_BOOLEAN_BASE_OFFSET = MEM_AVAILABLE ? MEM.arrayBaseOffset(boolean[].class) : -1;

    /**
     * Base offset of byte[]
     */
    int ARRAY_BYTE_BASE_OFFSET = MEM_AVAILABLE ? MEM.arrayBaseOffset(byte[].class) : -1;

    /**
     * Base offset of short[]
     */
    int ARRAY_SHORT_BASE_OFFSET = MEM_AVAILABLE ? MEM.arrayBaseOffset(short[].class) : -1;

    /**
     * Base offset of char[]
     */
    int ARRAY_CHAR_BASE_OFFSET = MEM_AVAILABLE ? MEM.arrayBaseOffset(char[].class) : -1;

    /**
     * Base offset of int[]
     */

    int ARRAY_INT_BASE_OFFSET = MEM_AVAILABLE ? MEM.arrayBaseOffset(int[].class) : -1;
    /**
     * Base offset of float[]
     */

    int ARRAY_FLOAT_BASE_OFFSET = MEM_AVAILABLE ? MEM.arrayBaseOffset(float[].class) : -1;
    /**
     * Base offset of long[]
     */
    int ARRAY_LONG_BASE_OFFSET = MEM_AVAILABLE ? MEM.arrayBaseOffset(long[].class) : -1;

    /**
     * Base offset of double[]
     */
    int ARRAY_DOUBLE_BASE_OFFSET = MEM_AVAILABLE ? MEM.arrayBaseOffset(double[].class) : -1;

    /**
     * Base offset of any type of Object[]
     */
    int ARRAY_OBJECT_BASE_OFFSET = MEM_AVAILABLE ? MEM.arrayBaseOffset(Object[].class) : -1;

    /**
     * Index scale of boolean[]
     */
    int ARRAY_BOOLEAN_INDEX_SCALE = MEM_AVAILABLE ? MEM.arrayIndexScale(boolean[].class) : -1;

    /**
     * Index scale of byte[]
     */
    int ARRAY_BYTE_INDEX_SCALE = MEM_AVAILABLE ? MEM.arrayIndexScale(byte[].class) : -1;

    /**
     * Index scale of short[]
     */
    int ARRAY_SHORT_INDEX_SCALE = MEM_AVAILABLE ? MEM.arrayIndexScale(short[].class) : -1;

    /**
     * Index scale of char[]
     */
    int ARRAY_CHAR_INDEX_SCALE = MEM_AVAILABLE ? MEM.arrayIndexScale(char[].class) : -1;

    /**
     * Index scale of int[]
     */
    int ARRAY_INT_INDEX_SCALE = MEM_AVAILABLE ? MEM.arrayIndexScale(int[].class) : -1;

    /**
     * Index scale of float[]
     */
    int ARRAY_FLOAT_INDEX_SCALE = MEM_AVAILABLE ? MEM.arrayIndexScale(float[].class) : -1;

    /**
     * Index scale of long[]
     */
    int ARRAY_LONG_INDEX_SCALE = MEM_AVAILABLE ? MEM.arrayIndexScale(long[].class) : -1;

    /**
     * Index scale of double[]
     */
    int ARRAY_DOUBLE_INDEX_SCALE = MEM_AVAILABLE ? MEM.arrayIndexScale(double[].class) : -1;

    /**
     * Index scale of any type of Object[]
     */
    int ARRAY_OBJECT_INDEX_SCALE = MEM_AVAILABLE ? MEM.arrayIndexScale(Object[].class) : -1;


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
     * Copies memory from source to destination. Source and destination addresses are specified
     * in the form {@code (baseObject, offset)} such that the offset is added to the object's base
     * address on Java heap.
     *
     * @param srcObj      source object
     * @param srcOffset   source offset
     * @param destObj     destination object
     * @param destOffset  destination offset
     * @param lengthBytes number of bytes to be copied
     */
    void copyMemory(Object srcObj, long srcOffset, Object destObj, long destOffset, long lengthBytes);

    /**
     * Returns the object reference at the supplied offset from the supplied object's base address.
     *
     * @param base   the object whose base address to use for the operation
     * @param offset offset from object's base to the accessed location
     */
    Object getObject(Object base, long offset);

    /**
     * Puts the supplied object reference at the supplied offset from the supplied object's base address.
     *
     * @param base   the owner object where the referenced object will be written to
     * @param offset offset from object's base to the accessed location
     * @param x      the referenced object to be written
     */
    void putObject(Object base, long offset, Object x);

    /**
     * Returns the boolean value at the supplied offset from the supplied object's base address.
     *
     * @param base   the object whose base address to use for the operation
     * @param offset offset from object's base to the accessed location
     */
    boolean getBoolean(Object base, long offset);

    /**
     * Puts the supplied boolean value at the supplied offset from the supplied object's base address.
     *
     * @param base   the owner object where the referenced object will be written to
     * @param offset offset from object's base to the accessed location
     * @param x      the referenced object to be written
     */
    void putBoolean(Object base, long offset, boolean x);

    /**
     * Returns the byte value at the supplied offset from the supplied object's base address.
     *
     * @param base   the object whose base address to use for the operation
     * @param offset offset from object's base to the accessed location
     */
    byte getByte(Object base, long offset);

    /**
     * Puts the supplied byte value at the supplied offset from the supplied object's base address.
     *
     * @param base   the owner object where the referenced object will be written to
     * @param offset offset from object's base to the accessed location
     * @param x      the referenced object to be written
     */
    void putByte(Object base, long offset, byte x);

    /**
     * Returns the char value at the supplied offset from the supplied object's base address.
     *
     * @param base   the object whose base address to use for the operation
     * @param offset offset from object's base to the accessed location
     */
    char getChar(Object base, long offset);

    /**
     * Puts the supplied char value at the supplied offset from the supplied object's base address.
     *
     * @param base   the owner object where the referenced object will be written to
     * @param offset offset from object's base to the accessed location
     * @param x      the referenced object to be written
     */
    void putChar(Object base, long offset, char x);

    /**
     * Returns the short value at the supplied offset from the supplied object's base address.
     *
     * @param base   the object whose base address to use for the operation
     * @param offset offset from object's base to the accessed location
     */
    short getShort(Object base, long offset);

    /**
     * Puts the supplied short value at the supplied offset from the supplied object's base address.
     *
     * @param base   the owner object where the referenced object will be written to
     * @param offset offset from object's base to the accessed location
     * @param x      the referenced object to be written
     */
    void putShort(Object base, long offset, short x);

    /**
     * Returns the int value at the supplied offset from the supplied object's base address.
     *
     * @param base   the object whose base address to use for the operation
     * @param offset offset from object's base to the accessed location
     */
    int getInt(Object base, long offset);

    /**
     * Puts the supplied int value at the supplied offset from the supplied object's base address.
     *
     * @param base   the owner object where the referenced object will be written to
     * @param offset offset from object's base to the accessed location
     * @param x      the referenced object to be written
     */
    void putInt(Object base, long offset, int x);

    /**
     * Returns the float value at the supplied offset from the supplied object's base address.
     *
     * @param base   the object whose base address to use for the operation
     * @param offset offset from object's base to the accessed location
     */
    float getFloat(Object base, long offset);

    /**
     * Puts the supplied float value at the supplied offset from the supplied object's base address.
     *
     * @param base   the owner object where the referenced object will be written to
     * @param offset offset from object's base to the accessed location
     * @param x      the referenced object to be written
     */
    void putFloat(Object base, long offset, float x);

    /**
     * Returns the long value at the supplied offset from the supplied object's base address.
     *
     * @param base   the object whose base address to use for the operation
     * @param offset offset from object's base to the accessed location
     */
    long getLong(Object base, long offset);

    /**
     * Puts the supplied long value at the supplied offset from the supplied object's base address.
     *
     * @param base   the owner object where the referenced object will be written to
     * @param offset offset from object's base to the accessed location
     * @param x      the referenced object to be written
     */
    void putLong(Object base, long offset, long x);

    /**
     * Returns the double value at the supplied offset from the supplied object's base address.
     *
     * @param base   the object whose base address to use for the operation
     * @param offset offset from object's base to the accessed location
     */
    double getDouble(Object base, long offset);

    /**
     * Puts the supplied double value at the supplied offset from the supplied object's base address.
     *
     * @param base   the owner object where the referenced object will be written to
     * @param offset offset from object's base to the accessed location
     * @param x      the referenced object to be written
     */
    void putDouble(Object base, long offset, double x);
}
