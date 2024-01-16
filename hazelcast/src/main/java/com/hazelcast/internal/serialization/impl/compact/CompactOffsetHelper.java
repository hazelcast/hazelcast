/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import static com.hazelcast.internal.serialization.impl.compact.OffsetReader.BYTE_OFFSET_READER;
import static com.hazelcast.internal.serialization.impl.compact.OffsetReader.BYTE_OFFSET_READER_RANGE;
import static com.hazelcast.internal.serialization.impl.compact.OffsetReader.INT_OFFSET_READER;
import static com.hazelcast.internal.serialization.impl.compact.OffsetReader.NULL_OFFSET;
import static com.hazelcast.internal.serialization.impl.compact.OffsetReader.SHORT_OFFSET_READER;
import static com.hazelcast.internal.serialization.impl.compact.OffsetReader.SHORT_OFFSET_READER_RANGE;

public class CompactOffsetHelper {
    private final int dataStartPosition;
    private final int variableOffsetsPosition;
    private final OffsetReader offsetReader;

    public CompactOffsetHelper(BufferObjectDataInput in, Schema schema) {
        try {
            int finalPosition;
            int numberOfVariableLengthFields = schema.getNumberOfVariableSizeFields();
            if (numberOfVariableLengthFields != 0) {
                int dataLength = in.readInt();
                dataStartPosition = in.position();
                variableOffsetsPosition = dataStartPosition + dataLength;
                if (dataLength < BYTE_OFFSET_READER_RANGE) {
                    offsetReader = BYTE_OFFSET_READER;
                    finalPosition = variableOffsetsPosition + numberOfVariableLengthFields;
                } else if (dataLength < SHORT_OFFSET_READER_RANGE) {
                    offsetReader = SHORT_OFFSET_READER;
                    finalPosition = variableOffsetsPosition + (numberOfVariableLengthFields * SHORT_SIZE_IN_BYTES);
                } else {
                    offsetReader = INT_OFFSET_READER;
                    finalPosition = variableOffsetsPosition + (numberOfVariableLengthFields * INT_SIZE_IN_BYTES);
                }
            } else {
                offsetReader = INT_OFFSET_READER;
                variableOffsetsPosition = 0;
                dataStartPosition = in.position();
                finalPosition = dataStartPosition + schema.getFixedSizeFieldsLength();
            }
            //set the position to final so that the next one to read something from `in` can start from
            //correct position
            in.position(finalPosition);
        } catch (IOException e) {
            throw illegalStateException(e);
        }
    }

    public int getDataStartPosition() {
        return dataStartPosition;
    }

    public OffsetReader getOffsetReader() {
        return offsetReader;
    }

    public int readFixedSizePosition(FieldDescriptor fd) {
        int primitiveOffset = fd.getOffset();
        return primitiveOffset + dataStartPosition;
    }

    public int readVariableSizeFieldPosition(BufferObjectDataInput in, FieldDescriptor fd) {
        try {
            int index = fd.getIndex();
            int offset = getOffsetReader().read(in, variableOffsetsPosition, index);
            return offset == NULL_OFFSET ? NULL_OFFSET : offset + getDataStartPosition();
        } catch (IOException e) {
            throw illegalStateException(e);
        }
    }

    private IllegalStateException illegalStateException(IOException e) {
        return new IllegalStateException("IOException is not expected since we get from a well known format and position", e);
    }
}

