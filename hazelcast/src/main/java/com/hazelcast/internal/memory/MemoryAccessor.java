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
 * Abstraction over an address space of readable and writable bytes.
 */
public interface MemoryAccessor {

    /**
     * Tells whether this memory accessor is big- or little-endian. This applies to all the
     * multibyte-width methods of integral type ({@code get/putChar}, {@code get/putShort},
     * {@code get/putInt}, {@code get/putLong} declared in this interface and others declared
     * in subinterfaces).
     *
     * @return {@code true} if the accessor is big-endian; {@code false} otherwise
     */
    boolean isBigEndian();

    /**
     * Reads the boolean value from given address.
     *
     * @param address the address where the boolean value will be read from
     * @return the value that is read
     */
    boolean getBoolean(long address);

    /**
     * Writes the given boolean value to given address.
     *
     * @param address the address where the boolean value will be written to
     * @param x       the boolean value to be written
     */
    void putBoolean(long address, boolean x);

    /**
     * Reads the byte value from given address.
     *
     * @param address the address where the byte value will be read from
     * @return the byte value that was read
     */
    byte getByte(long address);

    /**
     * Writes the given byte value to given address.
     *
     * @param address the address where the byte value will be written to
     * @param x       the byte value to be written
     */
    void putByte(long address, byte x);

    /**
     * Reads the char value from given address.
     *
     * @param address the address where the char value will be read from
     * @return the char value that was read
     */
    char getChar(long address);

    /**
     * Writes the given char value to given address.
     *
     * @param address the address where the char value will be written to
     * @param x       the char value to be written
     */
    void putChar(long address, char x);

    /**
     * Reads the short value from given address.
     *
     * @param address the address where the short value will be read from
     * @return the short value that was read
     */
    short getShort(long address);

    /**
     * Writes the given short value to given address.
     *
     * @param address the address where the short value will be written to
     * @param x       the short value to be written
     */
    void putShort(long address, short x);

    /**
     * Reads the int value from given address.
     *
     * @param address the address where the int value will be read from
     * @return the int value that was read
     */
    int getInt(long address);

    /**
     * Writes the given int value to given address.
     *
     * @param address the address where the int value will be written to
     * @param x       the int value to be written
     */
    void putInt(long address, int x);

    /**
     * Reads the float value from given address.
     *
     * @param address the address where the float value will be read from
     * @return the float value that was read
     */
    float getFloat(long address);

    /**
     * Writes the given float value to given address.
     *
     * @param address the address where the float value will be written to
     * @param x       the float value to be written
     */
    void putFloat(long address, float x);

    /**
     * Reads the long value from given address.
     *
     * @param address the address where the long value will be read from
     * @return the long value that was read
     */
    long getLong(long address);

    /**
     * Writes the given long value to given address.
     *
     * @param address the address where the long value will be written to
     * @param x       the long value to be written
     */
    void putLong(long address, long x);

    /**
     * Reads the double value from given address.
     *
     * @param address the address where the double value will be read from
     * @return the double value that was read
     */
    double getDouble(long address);

    /**
     * Writes the given double value to given address.
     *
     * @param address the address where the double value will be written to
     * @param x       the double value to be written
     */
    void putDouble(long address, double x);

    /**
     * Copies memory from given source address to given destination address
     * as given size.
     *
     * @param srcAddress  the source address to be copied from
     * @param destAddress the destination address to be copied to
     * @param lengthBytes       the number of bytes to be copied
     */
    void copyMemory(long srcAddress, long destAddress, long lengthBytes);

    /**
     * Copies bytes from a Java byte array into this accessor's address space.
     *
     * @param source the source byte array
     * @param offset index of the first byte to copy
     * @param destAddress address where the first byte will be written
     * @param length number of bytes to copy
     */
    void copyFromByteArray(byte[] source, int offset, long destAddress, int length);

    /**
     * Copies bytes from this accessor's address space to a Java byte array.
     *
     * @param srcAddress address of the first byte to copy
     * @param destination the destination byte array
     * @param offset array index where the first byte will be written
     * @param length number of bytes to copy
     */
    void copyToByteArray(long srcAddress, byte[] destination, int offset, int length);

    /**
     * Sets memory with given value from specified address as given size.
     *
     * @param address the start address of the memory region
     *                which will be set with the given value
     * @param lengthBytes   the number of bytes to be set
     * @param value   the value to be set
     */
    void setMemory(long address, long lengthBytes, byte value);
}
