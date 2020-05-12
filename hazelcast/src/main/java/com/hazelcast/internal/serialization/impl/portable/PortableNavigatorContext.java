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

import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.FieldDefinition;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.nio.serialization.HazelcastSerializationException;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;

import static com.hazelcast.internal.serialization.impl.portable.PortableUtils.createUnknownFieldException;
import static com.hazelcast.query.impl.getters.ExtractorHelper.extractAttributeNameNameWithoutArguments;

/**
 * Mutable navigator context that holds the state of the navigation in the portable byte stream.
 * Tightly coupled with the {@link PortablePositionNavigator}
 */
final class PortableNavigatorContext {

    // mutable fields that hold the mutable state
    private BufferObjectDataInput in;
    private int offset;
    private FieldDefinition fd;
    private Deque<NavigationFrame> multiPositions;

    private int finalPosition;
    private ClassDefinition cd;
    private PortableSerializer serializer;

    // final fields that hold the initial state for reset
    private final int initPosition;
    private final ClassDefinition initCd;
    private final int initOffset;
    private final int initFinalPosition;
    private final PortableSerializer initSerializer;

    PortableNavigatorContext(BufferObjectDataInput in, ClassDefinition cd, PortableSerializer serializer) {
        this.in = in;
        this.cd = cd;
        this.serializer = serializer;

        initFinalPositionAndOffset(in, cd);

        this.initCd = cd;
        this.initSerializer = serializer;
        this.initPosition = in.position();
        this.initFinalPosition = finalPosition;
        this.initOffset = offset;
    }

    /**
     * Initialises the finalPosition and offset and validates the fieldCount against the given class definition
     */
    private void initFinalPositionAndOffset(BufferObjectDataInput in, ClassDefinition cd) {
        int fieldCount;
        try {
            // final position after portable is read
            finalPosition = in.readInt();
            fieldCount = in.readInt();
        } catch (IOException e) {
            throw new HazelcastSerializationException(e);
        }
        if (fieldCount != cd.getFieldCount()) {
            throw new IllegalStateException("Field count[" + fieldCount + "] in stream does not match " + cd);
        }
        offset = in.position();
    }

    /**
     * Resets the state to the initial state for future reuse.
     */
    void reset() {
        cd = initCd;
        serializer = initSerializer;
        in.position(initPosition);
        finalPosition = initFinalPosition;
        offset = initOffset;
    }

    BufferObjectDataInput getIn() {
        return in;
    }

    int getCurrentOffset() {
        return offset;
    }

    int getCurrentFinalPosition() {
        return finalPosition;
    }

    FieldDefinition getCurrentFieldDefinition() {
        return fd;
    }

    ClassDefinition getCurrentClassDefinition() {
        return cd;
    }

    FieldType getCurrentFieldType() {
        return fd.getType();
    }

    boolean isCurrentFieldOfType(FieldType type) {
        return fd.getType() == type;
    }

    boolean areThereMultiPositions() {
        return multiPositions != null && !multiPositions.isEmpty();
    }

    NavigationFrame pollFirstMultiPosition() {
        return multiPositions.pollFirst();
    }

    /**
     * When we advance in the token navigation we need to re-initialise the class definition with the coordinates
     * of the new portable object in the context of which we will be navigating further.
     */
    void advanceContextToNextPortableToken(int factoryId, int classId, int version) throws IOException {
        cd = serializer.setupPositionAndDefinition(in, factoryId, classId, version);
        initFinalPositionAndOffset(in, cd);
    }

    /**
     * Sets up the stream for the given frame which contains all info required to change to context for a given field.
     */
    void advanceContextToGivenFrame(NavigationFrame frame) {
        in.position(frame.streamPosition);
        offset = frame.streamOffset;
        cd = frame.cd;
    }

    void setupContextForGivenPathToken(PortablePathCursor path) {
        String fieldName = path.token();
        fd = cd.getField(fieldName);
        if (fd != null) {
            return;
        }

        fieldName = extractAttributeNameNameWithoutArguments(path.token());
        fd = cd.getField(fieldName);
        if (fd == null || fieldName == null) {
            throw createUnknownFieldException(this, path.path());
        }
    }

    /**
     * @return true if managed to setup the context and the path was a single token path indeed, false otherwise
     */
    boolean trySetupContextForSingleTokenPath(String path) {
        fd = cd.getField(path);
        return fd != null;
    }

    /**
     * Populates the context with multi-positions that have to be processed later on in the navigation process.
     * The contract is that the cell[0] path is read in the non-multi-position navigation.
     * Cells[1, len-1] are stored in the multi-positions and will be followed up on later on.
     */
    void populateAnyNavigationFrames(int pathTokenIndex, int len) {
        // populate "recursive" multi-positions
        if (multiPositions == null) {
            // lazy-init only if necessary
            multiPositions = new ArrayDeque<NavigationFrame>();
        }
        for (int cellIndex = len - 1; cellIndex > 0; cellIndex--) {
            multiPositions.addFirst(new NavigationFrame(cd, pathTokenIndex, cellIndex, in.position(), offset));
        }
    }

    /**
     * Navigation frame that saves the state of the current navigation
     * It is used when the navigation path diverges, e.g. when [any] operator is used, since then the whole branch
     * has to be read.
     */
    static class NavigationFrame {
        final ClassDefinition cd;

        final int pathTokenIndex;
        final int arrayIndex;

        final int streamPosition;
        final int streamOffset;

        NavigationFrame(ClassDefinition cd, int pathTokenIndex, int arrayIndex, int streamPosition,
                        int streamOffset) {
            this.cd = cd;
            this.pathTokenIndex = pathTokenIndex;
            this.arrayIndex = arrayIndex;
            this.streamPosition = streamPosition;
            this.streamOffset = streamOffset;
        }
    }

}
