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
import java.util.List;

import static com.hazelcast.internal.serialization.impl.PortableUtils.validateFactoryAndClass;
import static java.util.Arrays.asList;

final class PortablePositionFactory {

    private static final boolean SINGLE_CELL_ACCESS = true;
    private static final boolean WHOLE_ARRAY_ACCESS = false;

    private static final PortableSinglePosition NIL_NOT_LEAF = nil(false);
    private static final PortableSinglePosition NIL_LEAF_ANY = nil(true, true);
    private static final PortableSinglePosition NIL_NOT_LEAF_ANY = nil(false, true);
    private static final PortableSinglePosition EMPTY_LEAF_ANY = empty(true, true);
    private static final PortableSinglePosition EMPTY_NOT_LEAF_ANY = empty(false, true);

    private PortablePositionFactory() {
    }

    // convenience for reusing nil positions without extra allocation:
    static PortablePosition nilAnyPosition(boolean lastToken) {
        return lastToken ? NIL_LEAF_ANY : NIL_NOT_LEAF_ANY;
    }

    // convenience for reusing nil positions without extra allocation:
    static PortablePosition emptyAnyPosition(boolean lastToken) {
        return lastToken ? EMPTY_LEAF_ANY : EMPTY_NOT_LEAF_ANY;
    }

    static PortablePosition nilNotLeafPosition() {
        return NIL_NOT_LEAF;
    }

    static PortableMultiPosition createMultiPosition(PortablePosition position) {
        return new PortableMultiPosition(position);
    }

    static PortableMultiPosition createMultiPosition(List<PortablePosition> positions) {
        return new PortableMultiPosition(positions);
    }

    static PortableSinglePosition createSinglePositionForReadAccess(
            PortableNavigatorContext ctx, PortablePathCursor path, int streamPosition) throws IOException {
        return createSinglePositionForReadAccess(ctx, path, streamPosition, -1);
    }

    static PortableSinglePosition createSinglePositionForReadAccess(
            PortableNavigatorContext ctx, PortablePathCursor path, int streamPosition, int index) throws IOException {
        PortableSinglePosition result = new PortableSinglePosition(ctx.getCurrentFieldDefinition(),
                streamPosition, index, path.isLastToken());
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
                if (position.isArrayCellAccess()) {
                    adjustForPortableArrayAccess(in, position, SINGLE_CELL_ACCESS);
                } else {
                    adjustForPortableArrayAccess(in, position, WHOLE_ARRAY_ACCESS);
                }
            } else {
                if (position.isArrayCellAccess()) {
                    adjustForSingleCellPrimitiveArrayAccess(in, path, type, position);
                }
                // we don't need to adjust anything otherwise - a primitive array can be read without any adjustments
            }
        } else {
            validateNonArrayPosition(position);
            if (type == FieldType.PORTABLE) {
                adjustForPortableFieldAccess(in, position);
            }
            // we don't need to adjust anything otherwise - a primitive field can be read without any adjustments
        }
    }

    private static void validateNonArrayPosition(PortableSinglePosition position) {
        if (position.getIndex() >= 0) {
            throw new IllegalArgumentException("Cannot read array cell from non-array");
        }
    }

    private static void adjustForSingleCellPrimitiveArrayAccess(
            BufferObjectDataInput in, String fieldName, FieldType type, PortableSinglePosition position) throws IOException {
        // assumes position.getIndex() >= 0
        in.position(position.getStreamPosition());
        int arrayLen = in.readInt();

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

    private static PortablePosition adjustForPortableFieldAccess(BufferObjectDataInput in, PortableSinglePosition pos)
            throws IOException {
        in.position(pos.streamPosition);
        pos.nil = in.readBoolean();
        pos.factoryId = in.readInt();
        pos.classId = in.readInt();
        validateFactoryAndClass(pos.fd, pos.factoryId, pos.classId);
        pos.streamPosition = in.position();
        return pos;
    }

    private static PortablePosition adjustForPortableArrayAccess(
            BufferObjectDataInput in, PortableSinglePosition pos, boolean singleCellAccess) throws IOException {
        in.position(pos.getStreamPosition());

        int len = in.readInt();
        int factoryId = in.readInt();
        int classId = in.readInt();

        pos.len = len;
        pos.factoryId = factoryId;
        pos.classId = classId;
        validateFactoryAndClass(pos.fd, pos.factoryId, pos.classId);
        pos.streamPosition = in.position();

        validateFactoryAndClass(pos.fd, factoryId, classId);
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

    static PortableSinglePosition empty(boolean leaf, boolean any) {
        PortableSinglePosition position = new PortableSinglePosition();
        position.len = 0;
        position.leaf = leaf;
        position.any = any;
        potentiallyNullify(position);
        return position;
    }

    private static void potentiallyNullify(PortableSinglePosition position) {
        if (position.isEmpty() && !position.isLeaf()) {
            position.nil = true;
        } else if (position.isEmpty() && position.getIndex() >= 0) {
            position.nil = true;
        }
    }

    static PortableSinglePosition nil(boolean leaf) {
        PortableSinglePosition position = new PortableSinglePosition();
        position.nil = true;
        position.leaf = leaf;
        return position;
    }

    static PortableSinglePosition nil(boolean leaf, boolean any) {
        PortableSinglePosition position = new PortableSinglePosition();
        position.nil = true;
        position.leaf = leaf;
        position.any = any;
        return position;
    }

    private static class PortableSinglePosition implements PortablePosition {

        public PortableSinglePosition() {
        }

        public PortableSinglePosition(FieldDefinition fd, int streamPosition, int index, boolean leaf) {
            this.fd = fd;
            this.streamPosition = streamPosition;
            this.index = index;
            this.leaf = leaf;
        }

        private FieldDefinition fd;
        private int streamPosition;

        private boolean nil;

        // used for arrays only
        private int index = -1;
        private int len = -1;

        // used for portables only
        private int factoryId = -1;
        private int classId = -1;

        private boolean leaf;
        private boolean any;

        @Override
        public int getStreamPosition() {
            return streamPosition;
        }

        @Override
        public int getIndex() {
            return index;
        }

        @Override
        public boolean isArrayCellAccess() {
            return index >= 0;
        }

        @Override
        public boolean isNull() {
            return nil;
        }

        @Override
        public int getLen() {
            return len;
        }

        @Override
        public boolean isEmpty() {
            return len == 0;
        }

        @Override
        public boolean isNullOrEmpty() {
            return isNull() || isEmpty();
        }

        @Override
        public boolean isLeaf() {
            return leaf;
        }

        @Override
        public boolean isAny() {
            return any;
        }

        @Override
        public int getFactoryId() {
            return factoryId;
        }

        @Override
        public int getClassId() {
            return classId;
        }

        @Override
        public boolean isMultiPosition() {
            return false;
        }

        @Override
        public List<PortablePosition> asMultiPosition() {
            throw new IllegalArgumentException("This position is not a multi-position!");
        }

        @Override
        public FieldType getType() {
            if (fd != null) {
                return fd.getType();
            }
            return null;
        }

        public void reset() {
            fd = null;
            streamPosition = 0;
            nil = false;
            index = -1;
            len = -1;
            factoryId = -1;
            classId = -1;
            leaf = false;
        }
    }

    private static class PortableMultiPosition extends PortableSinglePosition {

        private final List<PortablePosition> positions;

        public PortableMultiPosition(PortablePosition position) {
            this.positions = asList(position);
        }

        public PortableMultiPosition(List<PortablePosition> positions) {
            this.positions = positions;
        }

        @Override
        public boolean isMultiPosition() {
            return true;
        }

        @Override
        public FieldType getType() {
            if (positions.isEmpty()) {
                return null;
            } else {
                return positions.iterator().next().getType();
            }
        }

        @Override
        public List<PortablePosition> asMultiPosition() {
            return positions;
        }
    }

}
