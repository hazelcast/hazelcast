package com.hazelcast.internal.serialization.impl;

import com.hazelcast.nio.Bits;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.FieldDefinition;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.nio.serialization.HazelcastSerializationException;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

import static com.hazelcast.internal.serialization.impl.PortableHelper.extractArgumentsFromAttributeName;
import static com.hazelcast.internal.serialization.impl.PortableHelper.extractAttributeNameNameWithoutArguments;
import static com.hazelcast.nio.Bits.NULL_ARRAY_LENGTH;

public class PortablePositionNavigator {

    private static final Pattern NESTED_PATH_SPLITTER = Pattern.compile("\\.");

    private static final boolean SINGLE_CELL_ACCESS = true;
    private static final boolean WHOLE_ARRAY_ACCESS = false;

    private BufferObjectDataInput in;
    private int offset;
    private int finalPosition;
    private ClassDefinition cd;
    private PortableSerializer serializer;

    // PortableSinglePosition singleResult = new PortableSinglePosition();
    // PortableMultiPosition multiResult = new PortableMultiPosition();
    Deque<NavigationFrame> multiPositions = new ArrayDeque<NavigationFrame>();

    /**
     * @param in
     * @param cd
     * @param serializer
     */
    public void init(BufferObjectDataInput in, ClassDefinition cd, PortableSerializer serializer) {
        this.in = in;
        this.cd = cd;
        this.serializer = serializer;
        initFieldCountAndOffset(in, cd);
    }

    public void setup(BufferObjectDataInput in, int offset, int finalPosition, ClassDefinition cd,
                      PortableSerializer serializer) {
        this.in = in;
        this.offset = offset;
        this.finalPosition = finalPosition;
        this.cd = cd;
        this.serializer = serializer;
    }

    private void initFieldCountAndOffset(BufferObjectDataInput in, ClassDefinition cd) {
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
     * @param fieldName
     * @param type
     * @return
     * @throws IOException
     */
    public PortablePosition findPositionOfPrimitiveObject(String fieldName, FieldType type) throws IOException {
        PortablePosition position = findFieldPosition(fieldName, type);
        adjustForNonPortableArrayAccess(fieldName, type, (PortableSinglePosition) position);
        return position;
    }

    /**
     * @param fieldName
     * @param type
     * @return
     * @throws IOException
     */
    public PortablePosition findPositionOfPrimitiveArray(String fieldName, FieldType type) throws IOException {
        PortablePosition position = findFieldPosition(fieldName, type);
        adjustForNonPortableArrayAccess(fieldName, type, (PortableSinglePosition) position);
        return position;
    }

    private void adjustForNonPortableArrayAccess(String fieldName, FieldType type, PortableSinglePosition position) throws IOException {
        if (position.isMultiPosition()) {
            adjustPositionForMultiCellNonPortableArrayAccess(fieldName, type, position);
        } else {
            adjustPositionForSingleCellNonPortableArrayAccess(fieldName, type, position);
        }
    }

    /**
     * IMPORTANT:
     * Multi-position given as method parameter may contain positions from multiple arrays.
     * Due to array access optimisations This method expects that the positions among all positions of a
     * single array are sorted by stream-offset.
     */
    private void adjustPositionForMultiCellNonPortableArrayAccess(String fieldName, FieldType type, PortablePosition position) throws IOException {
        List<PortablePosition> positions = position.asMultiPosition();

        int arrayPosition = -1;
        int arrayIndex = 0;
        int arrayLen = 0;
        for (int i = 0; i < positions.size(); i++) {
            PortableSinglePosition p = (PortableSinglePosition) positions.get(i);
            if (p.index < 0) {
                continue;
            }

            if (arrayPosition != p.getStreamPosition()) {
                // advance to next array, begin from index 0
                arrayPosition = p.getStreamPosition();

                // validate the arrays length
                in.position(p.getStreamPosition());
                arrayLen = in.readInt();
            }
            validatePositionIndexInBound(fieldName, arrayLen, p);

            if (type == FieldType.UTF || type == FieldType.UTF_ARRAY) {
                while (p.index > arrayIndex) {
                    int indexElementLen = in.readInt();
                    in.position(in.position() + indexElementLen);
                    arrayIndex++;
                }
                p.position = in.position();
            } else {
                p.position = in.position() + p.index * getTypeElementSizeInBytes(type);
            }
        }
    }

    private void adjustPositionForSingleCellNonPortableArrayAccess(String fieldName, FieldType type, PortableSinglePosition position) throws IOException {
        if (position.index >= 0) {
            in.position(position.getStreamPosition());
            int arrayLen = in.readInt();
            validatePositionIndexInBound(fieldName, arrayLen, position);

            if (type == FieldType.UTF || type == FieldType.UTF_ARRAY) {
                int currentIndex = 0;
                while (position.index > currentIndex) {
                    int indexElementLen = in.readInt();
                    in.position(in.position() + indexElementLen);
                    currentIndex++;
                }
                position.position = in.position();
            } else {
                position.position = in.position() + position.index * getTypeElementSizeInBytes(type);
            }
        }
    }

    private void validatePositionIndexInBound(String fieldName, int arrayLen, PortableSinglePosition position) throws IOException {
        if (arrayLen == NULL_ARRAY_LENGTH) {
            throw new HazelcastSerializationException("The array " + fieldName + " is null!");
        }
        if (position.index > arrayLen) {
            throw new IllegalArgumentException("Index " + position.index + " out of bound in " + fieldName);
        }
    }

    // TODO -> translate plural types to singular types and validate
    static int getTypeElementSizeInBytes(FieldType type) {
        switch (type) {
            case BYTE:
                return Bits.BYTE_SIZE_IN_BYTES;
            case BYTE_ARRAY:
                return Bits.BYTE_SIZE_IN_BYTES;
            case SHORT:
                return Bits.SHORT_SIZE_IN_BYTES;
            case SHORT_ARRAY:
                return Bits.SHORT_SIZE_IN_BYTES;
            case INT:
                return Bits.INT_SIZE_IN_BYTES;
            case INT_ARRAY:
                return Bits.INT_SIZE_IN_BYTES;
            case LONG:
                return Bits.LONG_SIZE_IN_BYTES;
            case LONG_ARRAY:
                return Bits.LONG_SIZE_IN_BYTES;
            case FLOAT:
                return Bits.FLOAT_SIZE_IN_BYTES;
            case FLOAT_ARRAY:
                return Bits.FLOAT_SIZE_IN_BYTES;
            case DOUBLE:
                return Bits.DOUBLE_SIZE_IN_BYTES;
            case DOUBLE_ARRAY:
                return Bits.DOUBLE_SIZE_IN_BYTES;
            case BOOLEAN:
                return Bits.BOOLEAN_SIZE_IN_BYTES;
            case BOOLEAN_ARRAY:
                return Bits.BOOLEAN_SIZE_IN_BYTES;
            case CHAR:
                return Bits.CHAR_SIZE_IN_BYTES;
            case CHAR_ARRAY:
                return Bits.CHAR_SIZE_IN_BYTES;
            default:
                throw new RuntimeException("Unsupported type " + type);
        }
    }

    /**
     * @param fieldName
     * @return
     * @throws IOException
     */
    public PortablePosition findPositionOfPortableObject(String fieldName) throws IOException {
        PortablePosition pos = findFieldPosition(fieldName, FieldType.PORTABLE);
        if (pos.getIndex() < 0) {
            return adjustForPortableFieldAccess((PortableSinglePosition) pos);
        } else {
            return adjustForPortableArrayAccess((PortableSinglePosition) pos, SINGLE_CELL_ACCESS, fieldName);
        }
    }

    private PortablePosition adjustForPortableFieldAccess(PortableSinglePosition pos) throws IOException {
        in.position(pos.position);

        pos.isNull = in.readBoolean();
        pos.factoryId = in.readInt();
        pos.classId = in.readInt();
        pos.position = in.position();

        // TODO -> we need the read FieldDefinition here
        // checkFactoryAndClass(pos.fd, pos.factoryId, pos.classId);

        return pos;
    }

    private PortablePosition adjustForPortableArrayAccess(PortableSinglePosition pos, boolean singleCellAccess,
                                                          String path) throws IOException {
        in.position(pos.getStreamPosition());

        int len = in.readInt();
        int factoryId = in.readInt();
        int classId = in.readInt();

        pos.len = len;
        pos.factoryId = factoryId;
        pos.classId = classId;
        pos.position = in.position();

//        checkFactoryAndClass(fd, factoryId, classId);
        if (singleCellAccess) {
            if (len == Bits.NULL_ARRAY_LENGTH) {
                throw new HazelcastSerializationException("The array " + path + " is null!");
            } else if (len > 0) {
                int offset = in.position() + pos.getIndex() * Bits.INT_SIZE_IN_BYTES;
                in.position(offset);
                pos.position = in.readInt(); // portable position
            } else {
                throw new HazelcastSerializationException("The array " + path + " is empty!");
            }
        }
        return pos;
    }

    /**
     * @param fieldName
     * @return
     * @throws IOException
     */
    public PortablePosition findPositionOfPortableArray(String fieldName) throws IOException {
        PortableSinglePosition position = (PortableSinglePosition) findFieldPosition(fieldName, FieldType.PORTABLE_ARRAY);
        if (position.isMultiPosition()) {
            List<PortablePosition> positions = position.asMultiPosition();
            for (int i = 0; i < positions.size(); i++) {
                PortableSinglePosition pos = (PortableSinglePosition) positions.get(i);
                if (pos.getIndex() < 0) {
                    adjustForPortableFieldAccess(pos);
                } else {
                    adjustForPortableArrayAccess(pos, SINGLE_CELL_ACCESS, fieldName);
                }
            }
        } else {
            adjustForPortableArrayAccess(position, WHOLE_ARRAY_ACCESS, fieldName);
        }
        return position;
    }

    private int readPositionFromMetadata(FieldDefinition fd) throws IOException {
        int pos = in.readInt(offset + fd.getIndex() * Bits.INT_SIZE_IN_BYTES);
        short len = in.readShort(pos);
        // name + len + type
        return pos + Bits.SHORT_SIZE_IN_BYTES + len + 1;
    }

    private PortablePosition findFieldPosition(String nestedPath, FieldType type) throws IOException {
        String[] pathTokens = NESTED_PATH_SPLITTER.split(nestedPath);

        PortablePosition result = null;
        for (int i = 0; i < pathTokens.length; i++) {
            result = processPath(pathTokens, i, nestedPath, null);
            if (result != null && result.isMultiPosition() && result.asMultiPosition().size() == 0) {
                break;
            }
        }
        if (result == null && multiPositions.isEmpty()) {
            throw unknownFieldException(nestedPath);
        }


        if (multiPositions.isEmpty()) {
            //        if (fd.getType() != type) {
            //            throw new HazelcastSerializationException("Not a '" + type + "' field: " + fieldName);
            //        }
            return result;
        } else {

            List<PortablePosition> positions = new LinkedList<PortablePosition>();
            if (result != null && result.isMultiPosition() && result.asMultiPosition().size() == 0) {
//                break;
            } else if (result != null) {
                positions.add(result);
            }

            while (!multiPositions.isEmpty()) {
                NavigationFrame frame = multiPositions.pollFirst();
                setupForFrame(frame);
                for (int i = frame.pathTokenIndex; i < pathTokens.length; i++) {
                    result = processPath(pathTokens, i, nestedPath, frame);
                    if (result != null && result.isMultiPosition() && result.asMultiPosition().size() == 0) {
                        continue;
                    }
                    frame = null;
                }
                if (result == null) {
                    throw unknownFieldException(nestedPath);
                }
                if (result.isMultiPosition() && result.asMultiPosition().size() == 0) {
                    continue;
                }
                positions.add(result);
            }

            // TODO -> enable type checking
//        if (fd.getType() != type) {
//            throw new HazelcastSerializationException("Not a '" + type + "' field: " + fieldName);
//        }
            return new PortableMultiPosition(positions);
        }
    }

    private void setupForFrame(NavigationFrame frame) {
        offset = frame.streamOffset;
        cd = frame.cd;
    }

    private PortablePosition processPath(String[] pathTokens, int pathTokenIndex, String nestedPath,
                                         NavigationFrame frame) throws IOException {
        String token = pathTokens[pathTokenIndex];
        String field = extractAttributeNameNameWithoutArguments(token);
        FieldDefinition fd = cd.getField(field);
        boolean last = (pathTokenIndex == pathTokens.length - 1);

        if (fd == null || token == null) {
            throw unknownFieldException(field + " in " + nestedPath);
        }

        if (isPathTokenWithoutQuantifier(token)) {
            if (last) {
                return readPositionOfCurrentElement(new PortableSinglePosition(), fd);
            }
            advanceToNextTokenFromNonArrayElement(fd, token);
        } else if (isPathTokenWithAnyQuantifier(token)) {
            // this is an optimisation for the case where there's one [any] and it's at the end
//            if (last && multiPositions.isEmpty()) {
//                // [any] at the end -> we just return the whole array as if without [any]
//                return readPositionOfCurrentElement(new PortableSinglePosition(), fd);
//            }
            if (fd.getType() == FieldType.PORTABLE_ARRAY) {
                if (frame == null) {
                    int len = getCurrentArrayLength(fd);
                    if (len == 0) {
                        // dead pill -> [any] used with empty array, so no need to process further
                        return new PortableMultiPosition(Collections.<PortablePosition>emptyList());
                    } else {
                        populatePendingNavigationFrames(pathTokenIndex, len);
                        if (last) {
                            return readPositionOfCurrentElement(new PortableSinglePosition(), fd, 0);
                        }
                        advanceToNextTokenFromPortableArrayElement(fd, 0, field);
                    }
                } else {
                    if (last) {
                        return readPositionOfCurrentElement(new PortableSinglePosition(), fd, frame.arrayIndex);
                    }
                    advanceToNextTokenFromPortableArrayElement(fd, frame.arrayIndex, field);
                }
            } else {
                if (frame == null) {
                    if (last) {
                        int len = getCurrentArrayLength(fd);
                        if (len == 0) {
                            // dead pill -> [any] used with empty array, so no need to process further
                            return new PortableMultiPosition(Collections.<PortablePosition>emptyList());
                        } else {
                            populatePendingNavigationFrames(pathTokenIndex, len);
                            return readPositionOfCurrentElement(new PortableSinglePosition(), fd, 0);
                        }
                    }
                    throw wrongUseOfAnyOperationException(nestedPath);
                } else {
                    if (last) {
                        return readPositionOfCurrentElement(new PortableSinglePosition(), fd, frame.arrayIndex);
                    }
                    throw wrongUseOfAnyOperationException(nestedPath);
                }
            }
        } else {
            int index = Integer.valueOf(extractArgumentsFromAttributeName(token));
            if (last) {
                return readPositionOfCurrentElement(new PortableSinglePosition(), fd, index);
            }
            if (fd.getType() == FieldType.PORTABLE_ARRAY) {
                advanceToNextTokenFromPortableArrayElement(fd, index, field);
            }
        }

        return null;
    }

    private void populatePendingNavigationFrames(int pathTokenIndex, int len) {
        // populate "recursive" multi-positions
        for (int k = 1; k < len; k++) {
            multiPositions.add(new NavigationFrame(cd, pathTokenIndex, k, in.position(), this.offset));
        }
    }

    private int getCurrentArrayLength(FieldDefinition fd) throws IOException {
        int originalPos = in.position();
        try {
            int pos = readPositionFromMetadata(fd);
            in.position(pos);
            return in.readInt();
        } finally {
            in.position(originalPos);
        }
    }

    private boolean isPathTokenWithoutQuantifier(String pathToken) {
        return !pathToken.endsWith("]");
    }

    private boolean isPathTokenWithAnyQuantifier(String pathToken) {
        return pathToken.endsWith("[any]");
    }


    private PortablePosition readPositionOfCurrentElement(PortableSinglePosition result, FieldDefinition fd) throws IOException {
        result.fd = fd;
        result.position = readPositionFromMetadata(fd);
        return result;
    }

    private PortablePosition readPositionOfCurrentElement(PortableSinglePosition result, FieldDefinition fd, int index) throws IOException {
        result.fd = fd;
        result.position = readPositionFromMetadata(fd);
        result.index = index;
        return result;
    }

    private void advanceToNextTokenFromPortableArrayElement(FieldDefinition fd, int index, String field) throws IOException {
        int pos = readPositionFromMetadata(fd);
        in.position(pos);

        int len = in.readInt();
        int factoryId = in.readInt();
        int classId = in.readInt();
        if (len == Bits.NULL_ARRAY_LENGTH) {
            throw new HazelcastSerializationException("The array " + field + " is null!");
        }

        checkFactoryAndClass(fd, factoryId, classId);
        if (len > 0) {
            final int coffset = in.position() + index * Bits.INT_SIZE_IN_BYTES;
            in.position(coffset);
            int portablePosition = in.readInt();
            in.position(portablePosition);
            int versionId = in.readInt();

            advance(factoryId, classId, versionId);

        } else {
            throw new HazelcastSerializationException("The array " + field + " is empty!");
        }
    }

    private void advanceToNextTokenFromNonArrayElement(FieldDefinition fd, String token) throws IOException {
        int pos = readPositionFromMetadata(fd);
        in.position(pos);
        boolean isNull = in.readBoolean();
        if (isNull) {
            throw new NullPointerException("Parent field is null: " + token);
        }

        int factoryId = in.readInt();
        int classId = in.readInt();
        int version = in.readInt();
        advance(factoryId, classId, version);
    }

    private void advance(int factoryId, int classId, int version) throws IOException {
        cd = serializer.setupPositionAndDefinition(in, factoryId, classId, version);
        initFieldCountAndOffset(in, cd);
    }

    static int getArrayCellPosition(PortablePosition arrayPosition, int index, BufferObjectDataInput in)
            throws IOException {
        return in.readInt(arrayPosition.getStreamPosition() + index * Bits.INT_SIZE_IN_BYTES);
    }

    private HazelcastSerializationException unknownFieldException(String fieldName) {
        return new HazelcastSerializationException("Unknown field name: '" + fieldName
                + "' for ClassDefinition {id: " + cd.getClassId() + ", version: " + cd.getVersion() + "}");
    }

    private HazelcastSerializationException wrongUseOfAnyOperationException(String fieldName) {
        return new HazelcastSerializationException("Wrong use of any operator: '" + fieldName
                + "' for ClassDefinition {id: " + cd.getClassId() + ", version: " + cd.getVersion() + "}");
    }

    private void checkFactoryAndClass(FieldDefinition fd, int factoryId, int classId) {
        if (factoryId != fd.getFactoryId()) {
            throw new IllegalArgumentException("Invalid factoryId! Expected: "
                    + fd.getFactoryId() + ", Current: " + factoryId);
        }
        if (classId != fd.getClassId()) {
            throw new IllegalArgumentException("Invalid classId! Expected: "
                    + fd.getClassId() + ", Current: " + classId);
        }
    }

    public int getFinalPosition() {
        return finalPosition;
    }

    public int getOffset() {
        return offset;
    }

    private static class NavigationFrame {
        final ClassDefinition cd;

        final int pathTokenIndex;
        final int arrayIndex;

        final int streamPosition;
        final int streamOffset;

        public NavigationFrame(ClassDefinition cd, int pathTokenIndex, int arrayIndex, int streamPosition, int streamOffset) {
            this.cd = cd;
            this.pathTokenIndex = pathTokenIndex;
            this.arrayIndex = arrayIndex;
            this.streamPosition = streamPosition;
            this.streamOffset = streamOffset;
        }
    }

}
