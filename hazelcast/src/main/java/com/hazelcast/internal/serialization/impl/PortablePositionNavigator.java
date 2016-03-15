package com.hazelcast.internal.serialization.impl;

import com.hazelcast.nio.Bits;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.FieldDefinition;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.nio.serialization.HazelcastSerializationException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import java.util.regex.Pattern;

import static com.hazelcast.internal.serialization.impl.PortableHelper.extractArgumentsFromAttributeName;
import static com.hazelcast.internal.serialization.impl.PortableHelper.extractAttributeNameNameWithoutArguments;
import static com.hazelcast.nio.Bits.NULL_ARRAY_LENGTH;

public class PortablePositionNavigator {

    private static final Pattern NESTED_PATH_SPLITTER = Pattern.compile("\\.");

    private BufferObjectDataInput in;
    private int offset;
    private int finalPosition;
    private ClassDefinition cd;
    private PortableSerializer serializer;

    public int getFinalPosition() {
        return finalPosition;
    }

    public int getOffset() {
        return offset;
    }

    //    PortableSinglePosition singleResult = new PortableSinglePosition();
//    PortableMultiPosition multiResult = new PortableMultiPosition();
    Stack<NavigationFrame> frames = new Stack<NavigationFrame>();

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

    public PortablePosition findPositionOfPrimitiveObject(String fieldName, FieldType type) throws IOException {
        PortablePosition position = findFieldPosition(fieldName, type);
        adjustForPrimitiveArrayAccess(fieldName, (PortableSinglePosition) position);
        return position;
    }

    public PortablePosition findPositionOfPrimitiveArray(String fieldName, FieldType type) throws IOException {
        PortablePosition position = findFieldPosition(fieldName, type);
        adjustForPrimitiveArrayAccess(fieldName, (PortableSinglePosition) position);
        return position;
    }

    public PortablePosition findPositionOfPortableObject(String fieldName) throws IOException {
        PortablePosition pos = findFieldPosition(fieldName, FieldType.PORTABLE);
        if (pos.isMultiPosition()) {
            throw new RuntimeException("Not a PortableSinglePosition");
        }
        if (pos.getIndex() < 0) {
            return readPortable((PortableSinglePosition) pos);
        } else {
            return readPortableFromArray((PortableSinglePosition) pos, fieldName);
        }
    }

    private PortablePosition readPortable(PortableSinglePosition pos) throws IOException {
        in.position(pos.position);

        pos.isNull = in.readBoolean();
        pos.factoryId = in.readInt();
        pos.classId = in.readInt();
        pos.position = in.position();

        // TODO -> we need the read FieldDefinition here
        // checkFactoryAndClass(fd, factoryId, classId);

        return pos;
    }

    private PortablePosition readPortableFromArray(PortableSinglePosition pos, String path) throws IOException {

        in.position(pos.getStreamPosition());

        int len = in.readInt();
        int factoryId = in.readInt();
        int classId = in.readInt();
        if (len == Bits.NULL_ARRAY_LENGTH) {
            throw new HazelcastSerializationException("The array " + path + " is null!");
        }

//        checkFactoryAndClass(fd, factoryId, classId);
        if (len > 0) {
            final int offset = in.position() + pos.getIndex() * Bits.INT_SIZE_IN_BYTES;
            in.position(offset);
            int portablePosition = in.readInt();
            in.position(portablePosition);

            pos.len = len;
            pos.factoryId = factoryId;
            pos.classId = classId;
            pos.position = portablePosition;
            return pos;

        } else {
            throw new HazelcastSerializationException("The array " + path + " is empty!");
        }
    }

    public PortablePosition findPositionOfPortableArray(String fieldName) throws IOException {
        PortableSinglePosition position = (PortableSinglePosition) findFieldPosition(fieldName, FieldType.PORTABLE_ARRAY);
        if (position.isMultiPosition()) {
            for (int i = 0; i < position.asMultiPosition().size(); i++) {
                PortableSinglePosition p = (PortableSinglePosition) position.asMultiPosition().get(i);
                if(p.index >= 0) {
                    adjustForPortableArrayAccess(p);
                } else {
                    readPortable(p);
                }
            }
        } else {
            adjustForPortableArrayAccess(position);
        }

        return position;
    }

    private void adjustForPortableArrayAccess(PortableSinglePosition position) throws IOException {
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
    }

    private void adjustForPrimitiveArrayAccess(String fieldName, PortableSinglePosition position) throws IOException {
        if (position.isMultiPosition()) {
            for (int i = 0; i < position.asMultiPosition().size(); i++) {
                PortableSinglePosition p = (PortableSinglePosition) position.asMultiPosition().get(i);
                adjustPositionForPrimitiveArrayAccess(fieldName, p);
            }
        } else {
            adjustPositionForPrimitiveArrayAccess(fieldName, position);
        }
    }

    private void adjustPositionForPrimitiveArrayAccess(String fieldName, PortableSinglePosition position) throws IOException {
        if (position.index >= 0) {
            in.position(position.getStreamPosition());
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

    public PortablePosition findFieldPosition(String nestedPath, FieldType type) throws IOException {
        String[] pathTokens = NESTED_PATH_SPLITTER.split(nestedPath);
        List<PortablePosition> positions = new ArrayList<PortablePosition>();

        PortablePosition p = null;
        for (int i = 0; i < pathTokens.length; i++) {
            p = x(pathTokens, i, nestedPath, frames, null);
        }
        if (p != null) {
            positions.add(p);
        }

        while (!frames.isEmpty()) {
            NavigationFrame frame = frames.pop();

            for (int i = frame.pathTokenIndex; i < pathTokens.length; i++) {
                p = x(pathTokens, i, nestedPath, frames, frame);
                frame = null;
            }
            if (p != null) {
                positions.add(p);
            }
        }

        // TODO -> enable type checking
//        if (fd.getType() != type) {
//            throw new HazelcastSerializationException("Not a '" + type + "' field: " + fieldName);
//        }

        // TODO
        if (positions.size() == 1) {
            return positions.get(0);
        } else {
            return new PortableMultiPosition(positions);
        }


        // TODO
        // throw unknownFieldException(nestedPath);
    }

    private PortablePosition x(String[] pathTokens, int pathTokenIndex, String nestedPath, Stack<NavigationFrame> stack, NavigationFrame element) throws IOException {
        if (element != null) {
            offset = element.streamOffset;
            cd = element.cd;
        }


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
            if (last) {
                // [any] at the end -> we just return the whole array as if without [any]
                return readPositionOfCurrentElement(new PortableSinglePosition(), fd);
            }
            if (fd.getType() == FieldType.PORTABLE_ARRAY) {

                if (element == null) {
                    int len = getCurrentArrayLength(fd);
                    if (len == 0) {
                        // return empty array indicator
                    } else if (len == 1) {
                        advanceToNextTokenFromPortableArrayElement(fd, 0, field);
                    } else {
                        for (int k = 1; k < len; k++) {

                            NavigationFrame frame = new NavigationFrame(cd, pathTokenIndex, k, in.position(), this.offset);
                            stack.add(frame);
                        }
                        advanceToNextTokenFromPortableArrayElement(fd, 0, field);
                    }
                } else {
                    advanceToNextTokenFromPortableArrayElement(fd, element.arrayIndex, field);
                }

            } else {
                throw wrongUseOfAnyOperationException(nestedPath);
            }
        } else {
            int kindex = Integer.valueOf(extractArgumentsFromAttributeName(token));
            if (last) {
                return readPositionOfCurrentElement(new PortableSinglePosition(), fd, kindex);
            }
            if (fd.getType() == FieldType.PORTABLE_ARRAY) {
                advanceToNextTokenFromPortableArrayElement(fd, kindex, field);
            }
        }

        return null;
    }

    private int getCurrentArrayLength(FieldDefinition fd) throws IOException {
        int cpos = in.position();
        try {
            int pos = readPositionFromMetadata(fd);
            in.position(pos);
            return in.readInt();
        } finally {
            in.position(cpos);
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


}
