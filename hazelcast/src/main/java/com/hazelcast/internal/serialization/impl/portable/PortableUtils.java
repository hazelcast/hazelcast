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

import com.hazelcast.internal.nio.Bits;
import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.FieldDefinition;
import com.hazelcast.nio.serialization.HazelcastSerializationException;

import java.io.IOException;

import static com.hazelcast.query.impl.getters.ExtractorHelper.extractArgumentsFromAttributeName;

/**
 * Utils used in portable extraction mainly
 */
final class PortableUtils {

    private PortableUtils() {
    }

    /**
     * Extracts and validates the quantifier from the given path token
     *
     * @param token    token from which the quantifier is retrieved
     * @param fullPath fullPath to which the token belongs - just for output
     * @return validated quantifier
     */
    static int validateAndGetArrayQuantifierFromCurrentToken(String token, String fullPath) {
        String quantifier = extractArgumentsFromAttributeName(token);
        if (quantifier == null) {
            throw new IllegalArgumentException("Malformed quantifier in " + fullPath);
        }
        int index = Integer.parseInt(quantifier);
        if (index < 0) {
            throw new IllegalArgumentException("Array index " + index + " cannot be negative in " + fullPath);
        }
        return index;
    }

    /**
     * Calculates and reads the position of the Portable object stored in a Portable array under the given index.
     *
     * @param in             data input stream
     * @param streamPosition streamPosition to begin the reading from
     * @param cellIndex      index of the cell
     * @return the position of the given portable object in the stream
     * @throws IOException on any stream errors
     */
    static int getPortableArrayCellPosition(BufferObjectDataInput in, int streamPosition, int cellIndex)
            throws IOException {
        return in.readInt(streamPosition + cellIndex * Bits.INT_SIZE_IN_BYTES);
    }

    /**
     * Calculates the position of the given field in the portable byte stream
     *
     * @param fd     given field definition
     * @param in     data input stream
     * @param offset offset to use while stream reading
     * @return position of the given field
     * @throws IOException on any stream errors
     */
    static int getStreamPositionOfTheField(FieldDefinition fd, BufferObjectDataInput in, int offset) throws IOException {
        int pos = in.readInt(offset + fd.getIndex() * Bits.INT_SIZE_IN_BYTES);
        short len = in.readShort(pos);
        // name + len + type
        return pos + Bits.SHORT_SIZE_IN_BYTES + len + 1;
    }

    /**
     * Reads the length of the given array.
     * It does not validate if the current position is actually an array - has to be taken care of by the caller.
     *
     * @param fd     field of the array
     * @param in     data input stream
     * @param offset offset to use while stream reading
     * @return length of the array
     * @throws IOException on any stream errors
     */
    static int getArrayLengthOfTheField(FieldDefinition fd, BufferObjectDataInput in, int offset) throws IOException {
        int originalPos = in.position();
        try {
            int pos = getStreamPositionOfTheField(fd, in, offset);
            in.position(pos);
            return in.readInt();
        } finally {
            in.position(originalPos);
        }
    }

    /**
     * It doesn't validate the quantifier.
     *
     * @param token given path's token
     * @return true if the current path does not contain a quantifier
     */
    static boolean isCurrentPathTokenWithoutQuantifier(String token) {
        return !token.endsWith("]");
    }

    /**
     * @param token given path's token
     * @return true if the current path token contains a "any" quantifier
     */
    static boolean isCurrentPathTokenWithAnyQuantifier(String token) {
        return token.endsWith("[any]");
    }

    /**
     * @param ctx      portable navigation context
     * @param fullPath full path - just for output
     * @return initialised HazelcastSerializationException with an unknown field at the current path token
     */
    static HazelcastSerializationException createUnknownFieldException(PortableNavigatorContext ctx, String fullPath) {
        return new HazelcastSerializationException("Unknown field name: '" + fullPath
                + "' for ClassDefinition {id: " + ctx.getCurrentClassDefinition().getClassId()
                + ", version: " + ctx.getCurrentClassDefinition().getVersion() + "}");
    }

    /**
     * @param ctx      portable navigation context
     * @param fullPath full path - just for output
     * @return initialised IllegalArgumentException with a wrong use of any operator at the current path token
     */
    static IllegalArgumentException createWrongUseOfAnyOperationException(PortableNavigatorContext ctx, String fullPath) {
        return new IllegalArgumentException("Wrong use of any operator: '" + fullPath
                + "' for ClassDefinition {id: " + ctx.getCurrentClassDefinition().getClassId() + ", version: "
                + ctx.getCurrentClassDefinition().getVersion() + "}");
    }

    /**
     * @param cd       given classDefinition to validate against
     * @param fd       given fieldDefinition to validate against
     * @param fullPath full path - just for output
     * @throws IllegalArgumentException if the current field definition is not of an array type
     */
    static void validateArrayType(ClassDefinition cd, FieldDefinition fd, String fullPath) {
        if (!fd.getType().isArrayType()) {
            throw new IllegalArgumentException("Wrong use of array operator: '" + fullPath
                    + "' for ClassDefinition {id: " + cd.getClassId() + ", version: " + cd.getVersion() + "}");
        }
    }

    /**
     * Validates if the given factoryId and classId match the ones from the fieldDefinition
     *
     * @param fd        given fieldDefinition to validate against
     * @param factoryId given factoryId to validate
     * @param classId   given factoryId to validate
     * @param fullPath  full path - just for output
     */
    static void validateFactoryAndClass(FieldDefinition fd, int factoryId, int classId, String fullPath) {
        if (factoryId != fd.getFactoryId()) {
            throw new IllegalArgumentException("Invalid factoryId! Expected: "
                    + fd.getFactoryId() + ", Current: " + factoryId + " in path " + fullPath);
        }
        if (classId != fd.getClassId()) {
            throw new IllegalArgumentException("Invalid classId! Expected: "
                    + fd.getClassId() + ", Current: " + classId + " in path " + fullPath);
        }
    }
}
