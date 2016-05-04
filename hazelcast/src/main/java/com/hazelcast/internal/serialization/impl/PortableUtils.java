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
     * @param path path cursor from which the given path token is retrieved
     * @return validated quantifier
     */
    static int validateAndGetArrayQuantifierFromCurrentToken(PortablePathCursor path) {
        String quantifier = extractArgumentsFromAttributeName(path.token());
        if (quantifier == null) {
            throw new IllegalArgumentException("Malformed quantifier " + quantifier + " in " + path.path());
        }
        int index = Integer.valueOf(quantifier);
        if (index < 0) {
            throw new IllegalArgumentException("Array index " + index + " cannot be negative in " + path.path());
        }
        return index;
    }

    /**
     * Calculates and reads the position of the Portable object stored in a Portable array under the given index.
     *
     * @param arrayStreamPosition position of the portable array in the stream
     * @param index               index of the cell
     * @param in                  data input stream
     * @return the position of the given portable object in the stream
     * @throws IOException on any stream errors
     */
    static int getPortableArrayCellPosition(int arrayStreamPosition, int index, BufferObjectDataInput in)
            throws IOException {
        return in.readInt(arrayStreamPosition + index * Bits.INT_SIZE_IN_BYTES);
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
     * @param path given path cursor
     * @return true if the current path does not contain a quantifier
     */
    static boolean isCurrentPathTokenWithoutQuantifier(PortablePathCursor path) {
        return !path.token().endsWith("]");
    }

    /**
     * @param path given path cursor
     * @return true if the current path token contains a "any" quantifier
     */
    static boolean isCurrentPathTokenWithAnyQuantifier(PortablePathCursor path) {
        return path.token().endsWith("[any]");
    }

    /**
     * @param ctx  portable navigation context
     * @param path given path cursor
     * @return initialised HazelcastSerializationException with an unknown field at the current path token
     */
    static HazelcastSerializationException unknownFieldException(PortableNavigatorContext ctx, PortablePathCursor path) {
        return new HazelcastSerializationException("Unknown field name: '" + path
                + "' for ClassDefinition {id: " + ctx.getCurrentClassDefinition().getClassId()
                + ", version: " + ctx.getCurrentClassDefinition().getVersion() + "}");
    }

    /**
     * @param ctx  portable navigation context
     * @param path given path cursor
     * @return initialised IllegalArgumentException with a wrong use of any operator at the current path token
     */
    static IllegalArgumentException wrongUseOfAnyOperationException(PortableNavigatorContext ctx, PortablePathCursor path) {
        return new IllegalArgumentException("Wrong use of any operator: '" + path.path()
                + "' for ClassDefinition {id: " + ctx.getCurrentClassDefinition().getClassId() + ", version: "
                + ctx.getCurrentClassDefinition().getVersion() + "}");
    }

    /**
     * @param ctx  portable navigation context
     * @param path given path cursor
     * @throws IllegalArgumentException if the current field definition is not of an array type
     */
    static void validateArrayType(PortableNavigatorContext ctx, PortablePathCursor path) {
        if (!ctx.getCurrentFieldDefinition().getType().isArrayType()) {
            throw new IllegalArgumentException("Wrong use of array operator: '" + path.path()
                    + "' for ClassDefinition {id: " + ctx.getCurrentClassDefinition().getClassId() + ", version: "
                    + ctx.getCurrentClassDefinition().getVersion() + "}");
        }
    }

    /**
     * Validates if the given factoryId and classId match the ones from the fieldDefinition
     * @param fd given fieldDefinition to validate against
     * @param factoryId given factoryId to validate
     * @param classId given factoryId to validate
     */
    static void validateFactoryAndClass(FieldDefinition fd, int factoryId, int classId) {
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
