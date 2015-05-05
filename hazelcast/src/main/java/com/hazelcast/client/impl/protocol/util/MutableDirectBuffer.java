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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Abstraction over a range of buffer types that allows fields to be written (put) in native typed fashion.
 */
public interface MutableDirectBuffer
        extends DirectBuffer {
    /**
     * Set a region of memory to a given byte value.
     *
     * @param index  The index at which to start the memory region.
     * @param length The length of the run of bytes to set in the memory region.
     * @param value  The value of the byte that is set in the memory region.
     */
    void setMemory(int index, int length, byte value);

    /**
     * Put a value at a given index.
     *
     * @param index     The index in bytes where the value is put.
     * @param value     The value to put at the given index.
     * @param byteOrder The byteOrder of the value when written.
     */
    void putLong(int index, long value, ByteOrder byteOrder);

    /**
     * Put a value at a given index.
     *
     * @param index The index in bytes where the value is put.
     * @param value The value put at the given index.
     */
    void putLong(int index, long value);

    /**
     * Put a value at a given index.
     *
     * @param index The index in bytes where the value is put.
     * @param value The value put at the given index.
     * @param byteOrder The byteOrder of the value when written.
     */
    void putInt(int index, int value, ByteOrder byteOrder);

    /**
     * Put a value to a given index.
     *
     * @param index The index in bytes where the value is put.
     * @param value The value put at the given index.
     */
    void putInt(int index, int value);

    /**
     * Put a value to a given index.
     *
     * @param index     The index in bytes where the value is put.
     * @param value     The value put at the given index.
     * @param byteOrder The byteOrder of the value when written.
     */
    void putDouble(int index, double value, ByteOrder byteOrder);

    /**
     * Put a value to a given index.
     *
     * @param index The index in bytes where the value is put.
     * @param value The value put at the given index.
     */
    void putDouble(int index, double value);

    /**
     * Put a value to a given index.
     *
     * @param index     The index in bytes where the value is put.
     * @param value     The value put at the given index.
     * @param byteOrder of the value when written.
     */
    void putFloat(int index, float value, ByteOrder byteOrder);

    /**
     * Put a value to a given index.
     *
     * @param index The index in bytes where the value is put.
     * @param value The value put at the given index.
     */
    void putFloat(int index, float value);

    /**
     * Put a value to a given index.
     *
     * @param index     The index in bytes where the value is put.
     * @param value     The value put at the given index.
     * @param byteOrder of the value when written.
     */
    void putShort(int index, short value, ByteOrder byteOrder);

    /**
     * Put a value to a given index.
     *
     * @param index The index in bytes where the value is put.
     * @param value The value put at the given index.
     */
    void putShort(int index, short value);

    /**
     * Put a value to a given index.
     *
     * @param index The index in bytes where the value is put.
     * @param value The value put at the given index.
     */
    void putByte(int index, byte value);

    /**
     * Put an array of src into the underlying buffer.
     *
     * @param index The index in the underlying buffer from which to start the array.
     * @param src   The array to be copied into the underlying buffer.
     */
    void putBytes(int index, byte[] src);

    /**
     * Put an array into the underlying buffer.
     *
     * @param index  The index in the underlying buffer from which to start the array.
     * @param src    The array to be copied into the underlying buffer.
     * @param offset The offset in the supplied buffer at which to begin the copy.
     * @param length The length of the supplied buffer to copy.
     */
    void putBytes(int index, byte[] src, int offset, int length);

    /**
     * Put bytes into the underlying buffer for the view.  Bytes will be copied from current
     * {@link java.nio.ByteBuffer#position()} to {@link java.nio.ByteBuffer#limit()}.
     *
     * @param index     The index in the underlying buffer from which to start the array.
     * @param srcBuffer The source buffer to copy the bytes from.
     * @param length    The length of the supplied buffer to copy.
     */
    void putBytes(int index, ByteBuffer srcBuffer, int length);

    /**
     * Put bytes into the underlying buffer for the view. Bytes will be copied from the buffer index to
     * the buffer index + length.
     *
     * @param index     The index in the underlying buffer from which to start the array.
     * @param srcBuffer The source buffer to copy the bytes from (does not change position).
     * @param srcIndex  The index in the source buffer from which the copy will begin.
     * @param length    The length in bytes to be copied.
     */
    void putBytes(int index, ByteBuffer srcBuffer, int srcIndex, int length);

    /**
     * Put bytes from a source {@link DirectBuffer} into this {@link MutableDirectBuffer} at given indices.
     *
     * @param index     The index in this buffer from which to begin putting the bytes.
     * @param srcBuffer The source buffer from which the bytes will be copied.
     * @param srcIndex  The index in the source buffer from which the byte copy will begin.
     * @param length    The length in bytes to be copied.
     */
    void putBytes(int index, DirectBuffer srcBuffer, int srcIndex, int length);

    /**
     * Encode a String as UTF-8 bytes to the buffer with a length prefix.
     *
     * @param offset    The offset at which the String should be encoded.
     * @param value     The value of the String to be encoded.
     * @param byteOrder The byteOrder for the length prefix.
     * @return The number of bytes put to the buffer.
     */
    int putStringUtf8(int offset, String value, ByteOrder byteOrder);

    /**
     * Encode a String as UTF-8 bytes the buffer with a length prefix with a maximum encoded size check.
     *
     * @param offset         The offset at which the String should be encoded.
     * @param value          The value of the String to be encoded.
     * @param byteOrder      The byteOrder for the length prefix.
     * @param maxEncodedSize The maximum encoded size to be checked before writing to the buffer.
     * @return The number of bytes put to the buffer.
     * @throws java.lang.IllegalArgumentException if the encoded bytes are greater than maxEncodedSize.
     */
    int putStringUtf8(int offset, String value, ByteOrder byteOrder, int maxEncodedSize);

    /**
     * Encode a String as UTF-8 bytes in the buffer without a length prefix.
     *
     * @param offset The offset at which the String begins.
     * @param value  The value of the String to be encoded.
     * @return The number of bytes encoded.
     */
    int putStringWithoutLengthUtf8(int offset, String value);
}
