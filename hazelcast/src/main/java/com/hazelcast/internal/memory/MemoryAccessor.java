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

import static com.hazelcast.internal.memory.MemoryAccessorType.STANDARD;

/**
 * <p>
 * Abstraction over an address space of readable and writable bytes. A distinguished
 * special case is the native address space of the underlying CPU, to which the two
 * constants, {@link #MEM} and {@link #AMEM}, are devoted. These and other kinds of
 * memory accessor can be retrieved from a {@link MemoryAccessorProvider} by specifying
 * the desired {@link MemoryAccessorType}.
 * </p>
 *
 * @see MemoryAccessorType
 * @see MemoryAccessorProvider
 */
public interface MemoryAccessor {
    /**
     * A {@link MemoryAccessor} that accesses the underlying CPU's native address space.
     */
    MemoryAccessor MEM = MemoryAccessorProvider.getDefaultMemoryAccessor();

    /**
     * Like {@link #MEM}, but an instance specialized for aligned memory access. Requesting
     * unaligned memory access from this instance will result in low-level JVM crash on platforms
     * which do not support it.
     */
    MemoryAccessor AMEM = MemoryAccessorProvider.getMemoryAccessor(STANDARD);

    /**
     * Maximum size of a block of memory to copy in a single low-level memory-copying
     * operation. The goal is to prevent large periods without a GC safepoint.
     */
    int MEM_COPY_THRESHOLD = 1024 * 1024;

    /**
     * If this constant is {@code true}, then {@link #MEM} refers to a usable {@code MemoryAccessor}
     * instance.
     */
    boolean MEM_AVAILABLE = MEM != null;

    /**
     * If this constant is {@code true}, then {@link #AMEM} refers to a usable {@code MemoryAccessor}
     * instance.
     */
    boolean AMEM_AVAILABLE = AMEM != null;

    /////////////////////////////////////////////////////////////////////////

    /**
     * Copies memory from given source address to given destination address
     * as given size.
     *
     * @param srcAddress  the source address to be copied from
     * @param destAddress the destination address to be copied to
     * @param bytes       the number of bytes to be copied
     */
    void copyMemory(long srcAddress, long destAddress, long bytes);

    /**
     * Sets memory with given value from specified address as given size.
     *
     * @param address the start address of the memory region
     *                which will be set with given value
     * @param bytes   the number of bytes to be set
     * @param value   the value to be set
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


    /////////////////////////////////////////////////////////////////////////

    /**
     * Writes the given boolean value to given address.
     *
     * @param address the address where boolean value will be written to
     * @param x       the boolean value to be written
     */
    void putBoolean(long address, boolean x);

    /////////////////////////////////////////////////////////////////////////

    /**
     * Reads the byte value from given address.
     *
     * @param address the address where byte value will be read from
     * @return the read value
     */
    byte getByte(long address);

    /////////////////////////////////////////////////////////////////////////

    /**
     * Writes the given byte value to given address.
     *
     * @param address the address where byte value will be written to
     * @param x       the byte value to be written
     */
    void putByte(long address, byte x);

    /////////////////////////////////////////////////////////////////////////

    /**
     * Reads the char value from given address.
     *
     * @param address the address where char value will be read from
     * @return the read value
     */
    char getChar(long address);

    /////////////////////////////////////////////////////////////////////////

    /**
     * Writes the given char value to given address.
     *
     * @param address the address where char value will be written to
     * @param x       the char value to be written
     */
    void putChar(long address, char x);

    /////////////////////////////////////////////////////////////////////////

    /**
     * Reads the short value from given address.
     *
     * @param address the address where short value will be read from
     * @return the read value
     */
    short getShort(long address);

    /////////////////////////////////////////////////////////////////////////

    /**
     * Writes the given short value to given address.
     *
     * @param address the address where short value will be written to
     * @param x       the short value to be written
     */
    void putShort(long address, short x);

    /////////////////////////////////////////////////////////////////////////

    /**
     * Reads the int value from given address.
     *
     * @param address the address where int value will be read from
     * @return the read value
     */
    int getInt(long address);

    /////////////////////////////////////////////////////////////////////////

    /**
     * Writes the given int value to given address.
     *
     * @param address the address where int value will be written to
     * @param x       the int value to be written
     */
    void putInt(long address, int x);

    /////////////////////////////////////////////////////////////////////////

    /**
     * Reads the float value from given address.
     *
     * @param address the address where float value will be read from
     * @return the read value
     */
    float getFloat(long address);

    /////////////////////////////////////////////////////////////////////////

    /**
     * Writes the given float value to given address.
     *
     * @param address the address where float value will be written to
     * @param x       the float value to be written
     */
    void putFloat(long address, float x);

    /////////////////////////////////////////////////////////////////////////

    /**
     * Reads the long value from given address.
     *
     * @param address the address where long value will be read from
     * @return the read value
     */
    long getLong(long address);

    /////////////////////////////////////////////////////////////////////////

    /**
     * Writes the given long value to given address.
     *
     * @param address the address where long value will be written to
     * @param x       the long value to be written
     */
    void putLong(long address, long x);

    /////////////////////////////////////////////////////////////////////////

    /**
     * Reads the double value from given address.
     *
     * @param address the address where double value will be read from
     * @return the read value
     */
    double getDouble(long address);

    /////////////////////////////////////////////////////////////////////////

    /**
     * Writes the given double value to given address.
     *
     * @param address the address where double value will be written to
     * @param x       the double value to be written
     */
    void putDouble(long address, double x);
}
