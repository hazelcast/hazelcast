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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.nio.Bits;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.serialization.FieldDefinition;
import com.hazelcast.nio.serialization.FieldType;

import java.io.IOException;

final class PortablePositionFactory {

    private static final boolean SINGLE_CELL_ACCESS = true;
    private static final boolean WHOLE_ARRAY_ACCESS = false;

    private PortablePositionFactory() {
    }

    static PortableSinglePosition createSinglePositionForReadAccess(PortableNavigatorContext ctx,
                                                                    PortablePathCursor path,
                                                                    int streamPosition) throws IOException {
        return createSinglePositionForReadAccess(ctx, path, streamPosition, -1);
    }

    static PortableSinglePosition createSinglePositionForReadAccess(PortableNavigatorContext ctx,
                                                                    PortablePathCursor path, int streamPosition,
                                                                    int index)
            throws IOException {
        PortableSinglePosition result = new PortableSinglePosition();
        result.fd = ctx.getCurrentFieldDefinition();
        result.streamPosition = streamPosition;
        result.index = index;
        result.last = path.isLastToken();

        if (!result.isNullOrEmpty()) {
            adjustPositionForReadAccess(ctx.getIn(), result, path.path());
        }
        return result;
    }

    private static void adjustPositionForReadAccess(BufferObjectDataInput in, PortableSinglePosition position, String path)
            throws IOException {
        FieldType type = position.getType();
        if (type.isArrayType()) {
            if (type == FieldType.PORTABLE_ARRAY) {
                if (position.getIndex() >= 0) {
                    adjustForPortableArrayAccess(in, position, SINGLE_CELL_ACCESS);
                } else {
                    adjustForPortableArrayAccess(in, position, WHOLE_ARRAY_ACCESS);
                }
            } else {
                adjustForNonPortableArrayAccess(in, path, type, position);
            }
        } else {
            validateNonArrayPosition(position);
            if (type == FieldType.PORTABLE) {
                adjustForPortableObjectAccess(in, path, position);
            } else {
                adjustForNonPortableArrayAccess(in, path, type, position);
            }
        }

    }

    private static void validateNonArrayPosition(PortableSinglePosition position) {
        if (position.getIndex() >= 0) {
            throw new IllegalArgumentException("Cannot read array cell from non-array");
        }
    }

    private static void adjustForNonPortableArrayAccess(BufferObjectDataInput in, String fieldName, FieldType type,
                                                        PortableSinglePosition position) throws IOException {

        adjustPositionForSingleCellNonPortableArrayAccess(in, fieldName, type, position);
    }

    private static void adjustPositionForSingleCellNonPortableArrayAccess(BufferObjectDataInput in,
                                                                          String fieldName, FieldType type,
                                                                          PortableSinglePosition position)
            throws IOException {
        if (position.index >= 0) {
            in.position(position.getStreamPosition());
            int arrayLen = in.readInt();

            //
            if (arrayLen == Bits.NULL_ARRAY_LENGTH) {
                throw new IllegalArgumentException("The array is null in " + fieldName);
            }
            if (position.index > arrayLen - 1) {
                throw new IllegalArgumentException("Index " + position.index + " out of bound in " + fieldName);
            }

            if (type == FieldType.UTF || type == FieldType.UTF_ARRAY) {
                int currentIndex = 0;
                while (position.index > currentIndex) {
                    int indexElementLen = in.readInt();
                    in.position(in.position() + indexElementLen);
                    currentIndex++;
                }
                position.streamPosition = in.position();
            } else {
                position.streamPosition = in.position() + position.index * type.getSingleElementSize();
            }
        }
    }

    private static PortablePosition adjustForPortableObjectAccess(BufferObjectDataInput in, String fieldName,
                                                                  PortablePosition pos) throws IOException {
        if (pos.getIndex() < 0) {
            return adjustForPortableFieldAccess(in, (PortableSinglePosition) pos);
        } else {
            return adjustForPortableArrayAccess(in, (PortableSinglePosition) pos, SINGLE_CELL_ACCESS);
        }
    }

    private static PortablePosition adjustForPortableFieldAccess(BufferObjectDataInput in, PortableSinglePosition pos)
            throws IOException {
        in.position(pos.streamPosition);
        pos.nil = in.readBoolean();
        pos.factoryId = in.readInt();
        pos.classId = in.readInt();
        pos.streamPosition = in.position();
        checkFactoryAndClass(pos.fd, pos.factoryId, pos.classId);
        return pos;
    }

    private static PortablePosition adjustForPortableArrayAccess(BufferObjectDataInput in, PortableSinglePosition pos,
                                                                 boolean singleCellAccess)
            throws IOException {
        in.position(pos.getStreamPosition());

        int len = in.readInt();
        int factoryId = in.readInt();
        int classId = in.readInt();

        pos.len = len;
        pos.factoryId = factoryId;
        pos.classId = classId;
        pos.streamPosition = in.position();

        checkFactoryAndClass(pos.fd, factoryId, classId);
        if (singleCellAccess) {
            if (pos.getIndex() < len) {
                int offset = in.position() + pos.getIndex() * Bits.INT_SIZE_IN_BYTES;
                in.position(offset);
                pos.streamPosition = in.readInt();
            } else {
                pos.nil = true;
            }
        }

        return pos;
    }

    static void checkFactoryAndClass(FieldDefinition fd, int factoryId, int classId) {
        if (factoryId != fd.getFactoryId()) {
            throw new IllegalArgumentException("Invalid factoryId! Expected: "
                    + fd.getFactoryId() + ", Current: " + factoryId);
        }
        if (classId != fd.getClassId()) {
            throw new IllegalArgumentException("Invalid classId! Expected: "
                    + fd.getClassId() + ", Current: " + classId);
        }
    }

}
