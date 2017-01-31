/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
 * Interface for buffer to be used in client protocol.
 * Implemented by {@link SafeBuffer} and {@link UnsafeBuffer}
 */
public interface ClientProtocolBuffer {
    /**
     * Attach a view to a byte[] for providing direct access.
     *
     * @param buffer to which the view is attached.
     */
    void wrap(byte[] buffer);

    /**
     * Get the underlying byte[] if one exists.
     *
     * @return the underlying byte[] if one exists.
     */
    byte[] byteArray();

    /**
     * Get the capacity of the underlying buffer.
     *
     * @return the capacity of the underlying buffer in bytes.
     */
    int capacity();

    /**
     * Get the value at a given index.
     *
     * @param index in bytes from which to get.
     * @return the value for at a given index
     */
    long getLong(int index);

    /**
     * Get the value at a given index.
     *
     * @param index in bytes from which to get.
     * @return the value at a given index.
     */
    int getInt(int index);

    /**
     * Get the value at a given index.
     *
     * @param index in bytes from which to get.
     * @return the value at a given index.
     */
    short getShort(int index);

    /**
     * Get the value at a given index.
     *
     * @param index in bytes from which to get.
     * @return the value at a given index.
     */
    byte getByte(int index);

    /**
     * Get from the underlying buffer into a supplied byte array.
     * This method will try to fill the supplied byte array.
     *
     * @param index in the underlying buffer to start from.
     * @param dst   into which the dst will be copied.
     */
    void getBytes(int index, byte[] dst);

    /**
     * Get bytes from the underlying buffer into a supplied byte array.
     *
     * @param index  in the underlying buffer to start from.
     * @param dst    into which the bytes will be copied.
     * @param offset in the supplied buffer to start the copy
     * @param length of the supplied buffer to use.
     */
    void getBytes(int index, byte[] dst, int offset, int length);

    /**
     * Get part of String from bytes encoded in UTF-8 format without a length prefix.
     *
     * @param offset at which the String begins.
     * @param length of the String in bytes to decode.
     * @return the String as represented by the UTF-8 encoded bytes.
     */
    String getStringUtf8(int offset, int length);

    /**
     * Put a value at a given index.
     *
     * @param index The index in bytes where the value is put.
     * @param value The value to put at the given index.
     */
    void putLong(int index, long value);

    /**
     * Put a value at a given index.
     *
     * @param index The index in bytes where the value is put.
     * @param value The value put at the given index.
     */
    void putInt(int index, int value);

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
     * Encode a String as UTF-8 bytes to the buffer with a length prefix.
     *
     * @param index The index at which the String should be encoded.
     * @param value The value of the String to be encoded.
     * @return The number of bytes put to the buffer.
     */
    int putStringUtf8(int index, String value);

    /**
     * Encode a String as UTF-8 bytes the buffer with a length prefix with a maximum encoded size check.
     *
     * @param index          The index at which the String should be encoded.
     * @param value          The value of the String to be encoded.
     * @param maxEncodedSize The maximum encoded size to be checked before writing to the buffer.
     * @return The number of bytes put to the buffer.
     * @throws java.lang.IllegalArgumentException if the encoded bytes are greater than maxEncodedSize.
     */
    int putStringUtf8(int index, String value, int maxEncodedSize);
}
