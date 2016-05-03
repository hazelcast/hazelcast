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
import com.hazelcast.nio.serialization.FieldType;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import static com.hazelcast.internal.serialization.impl.PortableNavigatorContext.NavigationFrame;
import static com.hazelcast.internal.serialization.impl.PortablePositionFactory.checkFactoryAndClass;
import static com.hazelcast.internal.serialization.impl.PortableUtils.isCurrentPathTokenWithAnyQuantifier;
import static com.hazelcast.internal.serialization.impl.PortableUtils.isCurrentPathTokenWithoutQuantifier;
import static com.hazelcast.internal.serialization.impl.PortableUtils.unknownFieldException;
import static com.hazelcast.internal.serialization.impl.PortableUtils.validateAndGetArrayQuantifierFromCurrentToken;
import static com.hazelcast.internal.serialization.impl.PortableUtils.validateArrayType;
import static com.hazelcast.internal.serialization.impl.PortableUtils.wrongUseOfAnyOperationException;

/**
 * Enables navigation in the portable byte stream.
 * The contract is very simple: you give a path to an element, like "person.body.iq" and it returns a
 * {@link PortablePosition} that includes the type of the found element and its location in the stream.
 * <p>
 * The navigator is not stateful - everything is static.
 */
final class PortablePositionNavigator {

    // cache of commonly returned values to avoid extra allocations
    private static final PortableSinglePosition NIL_NOT_LAST = PortableSinglePosition.nil(false);
    private static final PortableSinglePosition NIL_LAST_ANY = PortableSinglePosition.nil(true, true);
    private static final PortableSinglePosition NIL_NOT_LAST_ANY = PortableSinglePosition.nil(false, true);
    private static final PortableSinglePosition EMPTY_LAST_ANY = PortableSinglePosition.empty(true, true);
    private static final PortableSinglePosition EMPTY_NOT_LAST_ANY = PortableSinglePosition.empty(false, true);

    private PortablePositionNavigator() {
    }

    /**
     * Main method that enables navigating in the Portable byte stream to find the element specified in the path.
     * The path may be:
     * - simple -> which means that it includes a single attribute only, like "name"
     * - nested -> which means that it includes more then a single attribute separated with a dot (.). like person.name
     * <p>
     * The path may also includes array cells
     * - specific quantifier, like person.leg[1] -> returns the leg with index 0
     * - wildcard quantifier, like person.leg[any] -> returns all legs
     * <p>
     * The wildcard quantifier may be used a couple of times, like person.leg[any].finger[any] which returns all fingers
     * from all legs.
     * <p>
     * Returns a {@link PortablePosition} that includes the type of the found element and its location in the stream.
     *
     * @param ctx  context of the navigation that encompasses inputStream, classDefinition and all other required fields
     * @param path pathCursor that's required to navigate down the path to the leaf token
     * @return PortablePosition of the found element. It may be a PortableSinglePosition or a PortableMultiPosition
     * @throws IOException in case of any stream reading errors
     */
    static PortablePosition findPositionForReading(
            PortableNavigatorContext ctx, PortablePathCursor path) throws IOException {
        PortablePosition result = navigateThroughAllTokensAndReturnPositionForReading(ctx, path, null);
        if (ctx.areThereMultiPositions()) {
            // If [any] operator is used the path may contains multi-positions.
            // A read with [any] operator may result in returning multiple values, so know we have to iterate over all
            // positions where the path has diverged to get the additional positions where the reader should read the
            // values.
            return processPendingMultiPositionsAndReturnMultiResult(ctx, path, result);
        } else {
            // In this case, there are no multi-position so we can just return the single result.
            return returnSingleResultWhenNoMultiPositions(path, result);
        }
    }

    private static PortablePosition navigateThroughAllTokensAndReturnPositionForReading(
            PortableNavigatorContext ctx, PortablePathCursor path, NavigationFrame frame) throws IOException {
        // Iterates over path tokens and processes them, ex. car.wheel[0].pressure
        // The navigateToPathToken invocation of a non-last token moves the cursor forward.
        // If null or empty position reached on the way it is returned to finish the iteration earlier, since there's no
        // non-null element to continue the iteration from.
        // The navigateToPathToken invocation of the last token returns the position where the reader should read the
        // attribute's value.
        PortablePosition result;
        do {
            result = navigateToPathToken(ctx, path, frame);
            if (result != null && result.isNullOrEmpty()) {
                break;
            }
            frame = null;
        } while (path.advanceToNextToken());

        // All path tokens have been read but no result hast been returned. It means that the element is unknown.
        // Otherwise some non-null, null or empty result would have been returned.
        if (result == null) {
            throw unknownFieldException(ctx, path);
        }
        return result;
    }

    private static PortablePosition navigateToPathToken(
            PortableNavigatorContext ctx, PortablePathCursor path, NavigationFrame frame) throws IOException {
        // first, setup the context for the current path token
        ctx.setupForCurrentPathToken(path);

        if (isCurrentPathTokenWithoutQuantifier(path)) {
            // ex: attribute
            PortablePosition result = navigateToPathTokenWithoutQuantifier(ctx, path);
            if (result != null) {
                return result;
            }
        } else if (isCurrentPathTokenWithAnyQuantifier(path)) {
            // ex: attribute[any]
            PortablePosition result = navigateToPathTokenWithAnyQuantifier(ctx, path, frame);
            if (result != null) {
                return result;
            }
        } else {
            // ex: attribute[2]
            PortablePosition result = navigateToPathTokenWithNumberQuantifier(ctx, path);
            if (result != null) {
                return result;
            }
        }
        return null;
    }

    /**
     * Token without quantifier. It means it's just a simple field, not an array.
     */
    private static PortablePosition navigateToPathTokenWithoutQuantifier(
            PortableNavigatorContext ctx, PortablePathCursor path) throws IOException {
        if (path.isLastToken()) {
            // if it's a token that's on the last position we calculate its direct access position and return it for
            // reading in the value reader.
            return returnPositionOfCurrentToken(ctx, path);
        } else {
            // if it's not a token that's on the last position in the path we advance the position to the next token
            // we also adjust the context, since advancing means that we are in the context of other
            // (and possibly different) portable type.
            if (!navigateContextToNextPortableTokenFromPortableField(ctx)) {
                // we return null if we didn't manage to advance from the current token to the next one.
                // For example: it may happen if the current token points to a null object.
                return NIL_NOT_LAST;
            }
        }
        return null;
    }

    private static PortablePosition returnSingleResultWhenNoMultiPositions(
            PortablePathCursor path, PortablePosition result) {
        // if the position is null or empty we don't need to do any processing, we just return it as-is.
        if (!result.isNullOrEmpty()) {
            // for consistency: [any] queries always return a multi-result, even if there's a single result only.
            // The only case where it returns a PortableSingleResult is when the position is a a single null result.
            // otherwise we always allocate a MultiResult to indicate to the reader that it's a multi-position.
            if (path.isAnyPath()) {
                return new PortableMultiPosition(result);
            }
        }
        return result;
    }

    private static PortablePosition processPendingMultiPositionsAndReturnMultiResult(
            PortableNavigatorContext ctx, PortablePathCursor path, PortablePosition result) throws IOException {
        // we process the all the paths gathered due to [any] quantifiers
        List<PortablePosition> positions = new LinkedList<PortablePosition>();
        // first add the single position to the result gathered in single path naviation
        positions.add(result);

        // then process all multi-positions and gather the results
        while (ctx.areThereMultiPositions()) {
            // setup navigation context and path for the given navigation frame
            NavigationFrame frame = ctx.pollFirstMultiPosition();
            setupContextAndPathWithFrameState(ctx, path, frame);
            // navigate to the last token and gather the result
            result = navigateThroughAllTokensAndReturnPositionForReading(ctx, path, frame);
            positions.add(result);
        }
        return new PortableMultiPosition(positions);
    }

    private static void setupContextAndPathWithFrameState(
            PortableNavigatorContext ctx, PortablePathCursor path, NavigationFrame frame) {
        ctx.setupForFrame(frame);
        path.index(frame.pathTokenIndex);
    }

    // token with [any] quantifier
    private static PortablePosition navigateToPathTokenWithAnyQuantifier(
            PortableNavigatorContext ctx, PortablePathCursor path, NavigationFrame frame) throws IOException {
        // check if the underlying field is of array type
        validateArrayType(ctx, path);

        if (ctx.isFieldOfType(FieldType.PORTABLE_ARRAY)) {
            // the result will be returned if it was the last token of the path, otherwise it has just moved further.
            PortablePosition result = navigateToPathTokenWithAnyQuantifierInPortableArray(ctx, path, frame);
            if (result != null) {
                return result;
            }
        } else {
            // there will always be a result since it's impossible to navigate further from a primitive field.
            return navigateToPathTokenWithAnyQuantifierInPrimitiveArray(ctx, path, frame);
        }
        return null;
    }

    // navigation in PORTABLE array
    private static PortablePosition navigateToPathTokenWithAnyQuantifierInPortableArray(
            PortableNavigatorContext ctx, PortablePathCursor path, NavigationFrame frame) throws IOException {
        // if no frame, we're pursuing the cell[0] case and populating the frames for cell[1 to length-1]
        if (frame == null) {
            // first we check if array null or empty
            int len = getArrayLengthOfTheField(ctx);
            PortablePosition result = doValidateArrayLengthForAnyQuantifier(len, path.isLastToken());
            if (result != null) {
                return result;
            }

            // then we populate frames for cell[1 to length-1]
            ctx.populateAnyNavigationFrames(path.index(), len);

            // pursue navigation to index 0, return result if last token
            int cellIndex = 0;
            result = doNavigateToPortableArrayCell(ctx, path, cellIndex);
            if (result != null) {
                return result;
            }
        } else {
            // pursue navigation to index given by the frame, return result if last token
            // no validation since it index in-bound has been validated while the navigation frames have been populated
            PortablePosition result = doNavigateToPortableArrayCell(ctx, path, frame.arrayIndex);
            if (result != null) {
                return result;
            }
        }
        return null;
    }

    private static PortablePosition doValidateArrayLengthForAnyQuantifier(int len, boolean lastToken) {
        if (len == 0) {
            return emptyPosition(lastToken);
        } else if (len == Bits.NULL_ARRAY_LENGTH) {
            return nilPosition(lastToken);
        }
        return null;
    }

    private static PortablePosition doNavigateToPortableArrayCell(
            PortableNavigatorContext ctx, PortablePathCursor path, int index) throws IOException {
        if (path.isLastToken()) {
            // if last token of the path we return the position for reading
            return returnPositionOfCurrentElementFromArrayCell(ctx, path, index);
        } else {
            // otherwise we navigate further down the path
            navigateContextToNextPortableTokenFromPortableArrayCell(ctx, index);
        }
        return null;
    }

    // navigation in PRIMITIVE array
    private static PortablePosition navigateToPathTokenWithAnyQuantifierInPrimitiveArray(
            PortableNavigatorContext ctx, PortablePathCursor path, NavigationFrame frame) throws IOException {
        // if no frame, we're pursuing the cell[0] case and populating the frames for cell[1 to length-1]
        if (frame == null) {
            if (path.isLastToken()) {
                // first we check if array null or empty
                int len = getArrayLengthOfTheField(ctx);
                PortablePosition result = doValidateArrayLengthForAnyQuantifier(len, path.isLastToken());
                if (result != null) {
                    return result;
                }

                // then we populate frames for cell[1 to length-1]
                ctx.populateAnyNavigationFrames(path.index(), len);

                // finally, we return the cell's position for reading -> cell[0]
                return returnPositionOfCurrentElementFromArrayCell(ctx, path, 0);
            }
            // primitive array cell has to be a last token, there's no furhter navigation from there.
            throw wrongUseOfAnyOperationException(ctx, path);
        } else {
            if (path.isLastToken()) {
                return returnPositionOfCurrentElementFromArrayCell(ctx, path, frame.arrayIndex);
            }
            throw wrongUseOfAnyOperationException(ctx, path);
        }
    }

    /**
     * Token with [number] quantifier. It means we are navigating in an array cell.
     */
    private static PortablePosition navigateToPathTokenWithNumberQuantifier(
            PortableNavigatorContext ctx, PortablePathCursor path) throws IOException {
        // makes sure that the field type is an array and parses the qantifier
        validateArrayType(ctx, path);
        int index = validateAndGetArrayQuantifierFromCurrentToken(path);

        // reads the array length and checks if the index is in-bound
        int len = getArrayLengthOfTheField(ctx);
        if (len == 0) {
            return emptyPosition(path.isLastToken());
        } else if (len == Bits.NULL_ARRAY_LENGTH) {
            return nilPosition(path.isLastToken());
        } else if (index >= len) {
            return nilPosition(path.isLastToken());
        } else {
            // when index in-bound
            if (path.isLastToken()) {
                // if it's a token that's on the last position we calculate its direct access position and return it for
                // reading in the value reader.
                return returnPositionOfCurrentElementFromArrayCell(ctx, path, index);
            } else if (ctx.isFieldOfType(FieldType.PORTABLE_ARRAY)) {
                // otherwise we advance only if the type is a portable_array. We cannot navigate further in a primitive
                // type and the portable arrays may store portable or primitive types only.
                navigateContextToNextPortableTokenFromPortableArrayCell(ctx, index);
            }
        }
        return null;
    }

    // returns true if managed to advance, false if advance failed due to null field
    private static boolean navigateContextToNextPortableTokenFromPortableField(PortableNavigatorContext ctx)
            throws IOException {
        BufferObjectDataInput in = ctx.getIn();

        // find the field position that's stored in the fieldDefinition int the context and navigate to it
        int pos = getStreamPositionOfTheField(ctx);
        in.position(pos);

        // check if it's null, if so return false indicating that the navigation has failed
        boolean isNull = in.readBoolean();
        if (isNull) {
            return false;
        }

        // read factory and class Id and validate if it's the same as expected in the fieldDefinition
        int factoryId = in.readInt();
        int classId = in.readInt();
        int versionId = in.readInt();

        // initialise context with the given portable field for further navigation
        ctx.advanceToNextPortableToken(factoryId, classId, versionId);
        return true;
    }

    // this navigation always succeeds since the caller validates if the index is inbound
    private static void navigateContextToNextPortableTokenFromPortableArrayCell(
            PortableNavigatorContext ctx, int index) throws IOException {
        BufferObjectDataInput in = ctx.getIn();

        // find the array field position that's stored in the fieldDefinition int the context and navigate to it
        int pos = getStreamPositionOfTheField(ctx);
        in.position(pos);

        // read array length and ignore
        in.readInt();

        // read factory and class Id and validate if it's the same as expected in the fieldDefinition
        int factoryId = in.readInt();
        int classId = in.readInt();
        checkFactoryAndClass(ctx.getCurrentFieldDefinition(), factoryId, classId);

        // calculate the offset of the cell given by the index
        final int cellOffset = in.position() + index * Bits.INT_SIZE_IN_BYTES;
        in.position(cellOffset);

        // read the position of the portable addressed in this array cell (array contains portable position only)
        int portablePosition = in.readInt();

        // navigate to portable position and read it's version
        in.position(portablePosition);
        int versionId = in.readInt();

        // initialise context with the given portable field for further navigation
        ctx.advanceToNextPortableToken(factoryId, classId, versionId);
    }

    private static PortablePosition returnPositionOfCurrentToken(PortableNavigatorContext ctx, PortablePathCursor path)
            throws IOException {
        int streamPositionOfThePathToken = getStreamPositionOfTheField(ctx);
        return PortablePositionFactory.createSinglePositionForReadAccess(ctx, path, streamPositionOfThePathToken);
    }

    private static PortablePosition returnPositionOfCurrentElementFromArrayCell(
            PortableNavigatorContext ctx, PortablePathCursor path, int index) throws IOException {
        int streamPosition = getStreamPositionOfTheField(ctx);
        return PortablePositionFactory.createSinglePositionForReadAccess(ctx, path, streamPosition, index);
    }

    // convenience methods:
    private static int getStreamPositionOfTheField(PortableNavigatorContext ctx) throws IOException {
        return PortableUtils.getStreamPositionOfTheField(ctx.getCurrentFieldDefinition(), ctx.getIn(),
                ctx.getCurrentOffset());
    }

    private static int getArrayLengthOfTheField(PortableNavigatorContext ctx) throws IOException {
        return PortableUtils.getArrayLengthOfTheField(ctx.getCurrentFieldDefinition(), ctx.getIn(),
                ctx.getCurrentOffset());
    }

    // convenience for reusing nil positions without extra allocation:
    private static PortablePosition nilPosition(boolean last) {
        return last ? NIL_LAST_ANY : NIL_NOT_LAST_ANY;
    }

    private static PortablePosition emptyPosition(boolean last) {
        return last ? EMPTY_LAST_ANY : EMPTY_NOT_LAST_ANY;
    }


}
