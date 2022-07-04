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

package com.hazelcast.internal.serialization.impl.compact;

import com.hazelcast.internal.nio.BufferObjectDataInput;

import java.io.IOException;

import static com.hazelcast.internal.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.SHORT_SIZE_IN_BYTES;

/**
 * Reads the offsets of the variable-size fields with the given indexes.
 */
public interface OffsetReader {

    /**
     * Offset of the null fields.
     */
    int NULL_OFFSET = -1;

    /**
     * Range of the offsets that can be represented by a single byte
     * and can be read with {@link OffsetReader#BYTE_OFFSET_READER}.
     */
    int BYTE_OFFSET_READER_RANGE = Byte.MAX_VALUE - Byte.MIN_VALUE;

    /**
     * Range of the offsets that can be represented by two bytes
     * and can be read with {@link OffsetReader#SHORT_OFFSET_READER}.
     */
    int SHORT_OFFSET_READER_RANGE = Short.MAX_VALUE - Short.MIN_VALUE;

    OffsetReader BYTE_OFFSET_READER = (in, variableOffsetsPos, index) -> {
        byte offset = in.readByte(variableOffsetsPos + index);
        if (offset == NULL_OFFSET) {
            return offset;
        }
        return Byte.toUnsignedInt(offset);
    };

    OffsetReader SHORT_OFFSET_READER = (in, variableOffsetsPos, index) -> {
        short offset = in.readShort(variableOffsetsPos + (index * SHORT_SIZE_IN_BYTES));
        if (offset == NULL_OFFSET) {
            return offset;
        }
        return Short.toUnsignedInt(offset);
    };

    OffsetReader INT_OFFSET_READER = (in, variableOffsetsPos, index) ->
            in.readInt(variableOffsetsPos + (index * INT_SIZE_IN_BYTES));

    /**
     * Returns the offset of the variable-size field at the given index.
     * @param in Input to read the offset from.
     * @param variableOffsetsPos Start of the variable-size field offsets
     *                           section of the input.
     * @param index Index of the field.
     * @return The offset.
     */
    int getOffset(BufferObjectDataInput in, int variableOffsetsPos, int index) throws IOException;
}
