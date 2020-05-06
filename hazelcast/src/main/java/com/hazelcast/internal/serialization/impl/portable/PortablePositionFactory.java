/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.serialization.impl.portable;

import com.hazelcast.nio.serialization.FieldDefinition;
import com.hazelcast.nio.serialization.FieldType;

import java.util.Collections;
import java.util.List;

/**
 * Factory for creating private implementations of the {@link PortablePosition} interface.
 */
final class PortablePositionFactory {

    // cache of commonly returned values to avoid extra allocations
    private static final PortableSinglePosition NIL_NOT_LEAF = nil(false);
    private static final PortableSinglePosition NIL_LEAF_ANY = nil(true, true);
    private static final PortableSinglePosition NIL_NOT_LEAF_ANY = nil(false, true);
    private static final PortableSinglePosition EMPTY_LEAF_ANY = empty(true, true);
    private static final PortableSinglePosition EMPTY_NOT_LEAF_ANY = empty(false, true);

    private PortablePositionFactory() {
    }

    // convenience for reusing practically immutable nil or positions without extra allocation
    static PortablePosition nilAnyPosition(boolean lastToken) {
        return lastToken ? NIL_LEAF_ANY : NIL_NOT_LEAF_ANY;
    }

    // convenience for reusing practically immutable nil or positions without extra allocation
    static PortablePosition emptyAnyPosition(boolean lastToken) {
        return lastToken ? EMPTY_LEAF_ANY : EMPTY_NOT_LEAF_ANY;
    }

    // convenience for reusing practically immutable nil or positions without extra allocation
    static PortablePosition nilNotLeafPosition() {
        return NIL_NOT_LEAF;
    }

    static PortableSinglePosition createSinglePrimitivePosition(
            FieldDefinition fd, int streamPosition, int index, boolean leaf) {
        return new PortableSinglePosition(fd, streamPosition, index, leaf);
    }

    static PortableSinglePosition createSinglePortablePosition(
            FieldDefinition fd, int streamPosition, int factoryId, int classId, boolean nil, boolean leaf) {
        PortableSinglePosition position = new PortableSinglePosition(fd, streamPosition, -1, leaf);
        position.factoryId = factoryId;
        position.classId = classId;
        position.nil = nil;
        return position;
    }

    static PortableSinglePosition createSinglePortablePosition(
            FieldDefinition fd, int streamPosition, int factoryId, int classId, int index, int len, boolean leaf) {
        PortableSinglePosition position = new PortableSinglePosition(fd, streamPosition, index, leaf);
        position.factoryId = factoryId;
        position.classId = classId;
        position.len = len;
        position.nil = isEmptyNil(position);
        return position;
    }

    static PortableMultiPosition createMultiPosition(PortablePosition position) {
        return new PortableMultiPosition(position);
    }

    static PortableMultiPosition createMultiPosition(List<PortablePosition> positions) {
        return new PortableMultiPosition(positions);
    }

    static PortableSinglePosition empty(boolean leaf, boolean any) {
        PortableSinglePosition position = new PortableSinglePosition();
        position.len = 0;
        position.leaf = leaf;
        position.any = any;
        position.nil = isEmptyNil(position);
        return position;
    }

    private static boolean isEmptyNil(PortableSinglePosition position) {
        return position.isEmpty() && (!position.isLeaf() || position.getIndex() >= 0);
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

        PortableSinglePosition() {
        }

        PortableSinglePosition(FieldDefinition fd, int streamPosition, int index, boolean leaf) {
            this.fd = fd;
            this.streamPosition = streamPosition;
            this.index = index;
            this.leaf = leaf;
        }

        @Override
        public int getStreamPosition() {
            return streamPosition;
        }

        @Override
        public int getIndex() {
            return index;
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
    }

    private static class PortableMultiPosition extends PortableSinglePosition {

        private final List<PortablePosition> positions;

        PortableMultiPosition(PortablePosition position) {
            this.positions = Collections.singletonList(position);
        }

        PortableMultiPosition(List<PortablePosition> positions) {
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
