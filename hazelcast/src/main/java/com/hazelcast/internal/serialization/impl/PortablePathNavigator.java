package com.hazelcast.internal.serialization.impl;

import com.hazelcast.nio.Bits;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.FieldDefinition;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.nio.serialization.HazelcastSerializationException;

import java.io.IOException;
import java.util.regex.Pattern;

import static com.hazelcast.internal.serialization.impl.PortableHelper.extractArgumentsFromAttributeName;
import static com.hazelcast.internal.serialization.impl.PortableHelper.extractAttributeNameNameWithoutArguments;
import static com.hazelcast.nio.Bits.NULL_ARRAY_LENGTH;

public class PortablePathNavigator {

    private static final Pattern NESTED_PATH_SPLITTER = Pattern.compile("\\.");

    static class Position {
        // used for all field types
        FieldDefinition fd;
        int position;
        int index = -1;

        // used for portables only
        boolean isNull = false;
        int len = -1;
        int factoryId;
        int classId;
    }

    BufferObjectDataInput in;
    int offset;
    int finalPosition;
    ClassDefinition cd;
    PortableSerializer serializer;
    Position result = new Position();

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

    public Position findPositionOfPrimitiveObject(String fieldName, FieldType type) throws IOException {
        PortablePathNavigator.Position position = findFieldPosition(fieldName, type);
        adjustForPrimitiveArrayAccess(fieldName, position);
        return position;
    }

    public Position findPositionOfPrimitiveArray(String fieldName, FieldType type) throws IOException {
        PortablePathNavigator.Position position = findFieldPosition(fieldName, type);
        return position;
    }

    public Position findPositionOfPortableObject(String fieldName) throws IOException {
        PortablePathNavigator.Position pos = findFieldPosition(fieldName, FieldType.PORTABLE);
        if (pos.index < 0) {
            return readPortable(pos);
        } else {
            return readPortableFromArray(pos, fieldName);
        }
    }

    private Position readPortable(PortablePathNavigator.Position pos) throws IOException {
        in.position(pos.position);
        result.isNull = in.readBoolean();
        result.factoryId = in.readInt();
        result.classId = in.readInt();
        result.position = in.position();

        // TODO -> we need the read FieldDefinition here
        // checkFactoryAndClass(fd, factoryId, classId);

        return result;
    }

    private Position readPortableFromArray(PortablePathNavigator.Position pos, String path) throws IOException {
//        int pos = readPosition(fd);
//        in.position(pos);

        in.position(pos.position);
        int len = in.readInt();
        int factoryId = in.readInt();
        int classId = in.readInt();
        if (len == Bits.NULL_ARRAY_LENGTH) {
            throw new HazelcastSerializationException("The array " + path + " is null!");
        }

//        checkFactoryAndClass(fd, factoryId, classId);
        if (len > 0) {
            final int offset = in.position() + pos.index * Bits.INT_SIZE_IN_BYTES;
            in.position(offset);
            int portablePosition = in.readInt();
            in.position(portablePosition);

            result.len = len;
            result.factoryId = factoryId;
            result.classId = classId;
            result.position = portablePosition;
            return result;

        } else {
            throw new HazelcastSerializationException("The array " + path + " is empty!");
        }
    }

    public Position findPositionOfPortableArray(String fieldName) throws IOException {
        PortablePathNavigator.Position position = findFieldPosition(fieldName, FieldType.PORTABLE_ARRAY);
        in.position(position.position);

        final int currentPos = in.position();
        try {
            int len = in.readInt();
            int factoryId = in.readInt();
            int classId = in.readInt();

            position.len = len;
            position.factoryId = factoryId;
            position.classId = classId;
            position.position = in.position();

            // TODO
//            checkFactoryAndClass(fd, factoryId, classId);
        } finally {
            in.position(currentPos);
        }

        return position;
    }

    private void adjustForPrimitiveArrayAccess(String fieldName, PortablePathNavigator.Position position) throws IOException {
        if (position.index >= 0) {
            in.position(position.position);
            int len = in.readInt();
            if (len == NULL_ARRAY_LENGTH) {
                throw new HazelcastSerializationException("The array " + fieldName + " is null!");
            }
            final int coffset = in.position() + position.index * Bits.INT_SIZE_IN_BYTES;
            // We are returning since you cannot extract further from a primitive field
            position.position = coffset;
        }
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

    private int readPositionFromMetadata(FieldDefinition fd) throws IOException {
        int pos = in.readInt(offset + fd.getIndex() * Bits.INT_SIZE_IN_BYTES);
        short len = in.readShort(pos);
        // name + len + type
        return pos + Bits.SHORT_SIZE_IN_BYTES + len + 1;
    }

    private HazelcastSerializationException throwUnknownFieldException(String fieldName) {
        return new HazelcastSerializationException("Unknown field name: '" + fieldName
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

    private boolean isPathTokenWithoutArrayOperator(String pathToken) {
        return !pathToken.endsWith("]");
    }

    public Position findFieldPosition(String nestedPath, FieldType type) throws IOException {
        String[] pathTokens = NESTED_PATH_SPLITTER.split(nestedPath);
        FieldDefinition fd;

        for (int i = 0; i < pathTokens.length; i++) {
            String token = pathTokens[i];
            String field = extractAttributeNameNameWithoutArguments(token);
            fd = cd.getField(field);

            if (fd == null || token == null) {
                throw throwUnknownFieldException(field + " in " + nestedPath);
            }

            if (isPathTokenWithoutArrayOperator(token)) {
                if (i == pathTokens.length - 1) {
                    return readPositionOfCurrentElement(fd);
                }
                advanceToNextTokenFromNonArrayElement(fd, token);
            } else {
                int index = Integer.valueOf(extractArgumentsFromAttributeName(token));
                if (i == pathTokens.length - 1) {
                    return readPositionOfCurrentElement(fd, index);
                }
                if (fd.getType() == FieldType.PORTABLE_ARRAY) {
                    advanceToNextTokenFromPortableArrayElement(fd, index, field);
                }
            }

        }
        // TODO -> enable type checking
//        if (fd.getType() != type) {
//            throw new HazelcastSerializationException("Not a '" + type + "' field: " + fieldName);
//        }
        throw throwUnknownFieldException(nestedPath);

    }

    private Position readPositionOfCurrentElement(FieldDefinition fd) throws IOException {
        result.fd = fd;
        result.position = readPositionFromMetadata(fd);
        return result;
    }

    private Position readPositionOfCurrentElement(FieldDefinition fd, int index) throws IOException {
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

}
