/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.nio.Bits;
import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.query.extractor.ValueCallback;
import com.hazelcast.query.extractor.ValueCollector;
import com.hazelcast.query.extractor.ValueReader;
import com.hazelcast.query.extractor.ValueReadingException;
import com.hazelcast.query.impl.getters.ImmutableMultiResult;
import com.hazelcast.query.impl.getters.MultiResult;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static com.hazelcast.internal.serialization.impl.PortableUtils.getPortableArrayCellPosition;

/**
 * Can't be accessed concurrently.
 */
public class DefaultPortableReader implements ValueReader, PortableReader {

    private static final MultiResult NULL_EMPTY_TARGET_MULTIRESULT;

    static {
        MultiResult<Object> result = new MultiResult<Object>();
        result.addNullOrEmptyTarget();
        NULL_EMPTY_TARGET_MULTIRESULT = new ImmutableMultiResult<Object>(result);
    }

    protected final ClassDefinition cd;
    protected final PortableSerializer serializer;

    private final BufferObjectDataInput in;
    private final int finalPosition;
    private final int offset;

    private final PortableNavigatorContext ctx;
    private final PortablePathCursor pathCursor;

    private boolean raw;

    DefaultPortableReader(PortableSerializer serializer, BufferObjectDataInput in, ClassDefinition cd) {
        this.in = in;
        this.serializer = serializer;
        this.cd = cd;

        this.ctx = new PortableNavigatorContext(in, cd, serializer);
        this.pathCursor = new PortablePathCursor();

        this.finalPosition = ctx.getCurrentFinalPosition();
        this.offset = ctx.getCurrentOffset();
    }

    @Override
    public int getVersion() {
        return cd.getVersion();
    }

    @Override
    public boolean hasField(String fieldName) {
        return cd.hasField(fieldName);
    }

    @Override
    public Set<String> getFieldNames() {
        return cd.getFieldNames();
    }

    @Override
    public FieldType getFieldType(String fieldName) {
        return cd.getFieldType(fieldName);
    }

    @Override
    public int getFieldClassId(String fieldName) {
        return cd.getFieldClassId(fieldName);
    }

    @Override
    public ObjectDataInput getRawDataInput() throws IOException {
        if (!raw) {
            int pos = in.readInt(offset + cd.getFieldCount() * Bits.INT_SIZE_IN_BYTES);
            in.position(pos);
        }
        raw = true;
        return in;
    }

    final void end() {
        in.position(finalPosition);
    }

    @Override
    public byte readByte(String path) throws IOException {
        PortablePosition pos = findPositionForReading(path);
        validatePrimitive(pos, FieldType.BYTE);
        return in.readByte(pos.getStreamPosition());
    }

    @Override
    public short readShort(String path) throws IOException {
        PortablePosition pos = findPositionForReading(path);
        validatePrimitive(pos, FieldType.SHORT);
        return in.readShort(pos.getStreamPosition());
    }

    @Override
    public int readInt(String path) throws IOException {
        PortablePosition pos = findPositionForReading(path);
        validatePrimitive(pos, FieldType.INT);
        return in.readInt(pos.getStreamPosition());
    }

    @Override
    public long readLong(String path) throws IOException {
        PortablePosition pos = findPositionForReading(path);
        validatePrimitive(pos, FieldType.LONG);
        return in.readLong(pos.getStreamPosition());
    }

    @Override
    public float readFloat(String path) throws IOException {
        PortablePosition pos = findPositionForReading(path);
        validatePrimitive(pos, FieldType.FLOAT);
        return in.readFloat(pos.getStreamPosition());
    }

    @Override
    public double readDouble(String path) throws IOException {
        PortablePosition pos = findPositionForReading(path);
        validatePrimitive(pos, FieldType.DOUBLE);
        return in.readDouble(pos.getStreamPosition());
    }

    @Override
    public boolean readBoolean(String path) throws IOException {
        PortablePosition pos = findPositionForReading(path);
        validatePrimitive(pos, FieldType.BOOLEAN);
        return in.readBoolean(pos.getStreamPosition());
    }

    @Override
    public char readChar(String path) throws IOException {
        PortablePosition pos = findPositionForReading(path);
        validatePrimitive(pos, FieldType.CHAR);
        return in.readChar(pos.getStreamPosition());
    }

    @Override
    public String readUTF(String path) throws IOException {
        final int currentPos = in.position();
        try {
            PortablePosition position = findPositionForReading(path);
            if (position.isNullOrEmpty()) {
                return null;
            }
            validateNotMultiPosition(position);
            validateType(position, FieldType.UTF);
            in.position(position.getStreamPosition());
            return in.readUTF();
        } finally {
            in.position(currentPos);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Portable readPortable(String path) throws IOException {
        final int currentPos = in.position();
        try {
            PortablePosition position = findPositionForReading(path);
            if (position.isNullOrEmpty()) {
                return null;
            }
            validateNotMultiPosition(position);
            validateType(position, FieldType.PORTABLE);
            in.position(position.getStreamPosition());
            return serializer.readAndInitialize(in, position.getFactoryId(), position.getClassId());
        } finally {
            in.position(currentPos);
        }
    }

    @Override
    public byte[] readByteArray(String path) throws IOException {
        final int currentPos = in.position();
        try {
            PortablePosition position = findPositionForReading(path);
            if (position.isNullOrEmpty()) {
                return null;
            } else if (position.isMultiPosition()) {
                return readMultiByteArray(position.asMultiPosition());
            } else {
                return readSingleByteArray(position);
            }
        } finally {
            in.position(currentPos);
        }
    }

    private byte[] readMultiByteArray(List<PortablePosition> positions) throws IOException {
        byte[] result = new byte[positions.size()];
        for (int i = 0; i < result.length; i++) {
            PortablePosition position = positions.get(i);
            validateNonNullOrEmptyPosition(position);
            validateType(position, FieldType.BYTE);
            result[i] = in.readByte(position.getStreamPosition());
        }
        return result;
    }

    private byte[] readSingleByteArray(PortablePosition position) throws IOException {
        validateType(position, FieldType.BYTE_ARRAY);
        in.position(position.getStreamPosition());
        return in.readByteArray();
    }

    @Override
    public boolean[] readBooleanArray(String path) throws IOException {
        final int currentPos = in.position();
        try {
            PortablePosition position = findPositionForReading(path);
            if (position.isNullOrEmpty()) {
                return null;
            } else if (position.isMultiPosition()) {
                return readMultiBooleanArray(position.asMultiPosition());
            } else {
                return readSingleBooleanArray(position);
            }
        } finally {
            in.position(currentPos);
        }
    }

    private boolean[] readMultiBooleanArray(List<PortablePosition> positions) throws IOException {
        boolean[] result = new boolean[positions.size()];
        for (int i = 0; i < result.length; i++) {
            PortablePosition position = positions.get(i);
            validateNonNullOrEmptyPosition(position);
            validateType(position, FieldType.BOOLEAN);
            result[i] = in.readBoolean(position.getStreamPosition());
        }
        return result;
    }

    private boolean[] readSingleBooleanArray(PortablePosition position) throws IOException {
        validateType(position, FieldType.BOOLEAN_ARRAY);
        in.position(position.getStreamPosition());
        return in.readBooleanArray();
    }

    @Override
    public char[] readCharArray(String path) throws IOException {
        final int currentPos = in.position();
        try {
            PortablePosition position = findPositionForReading(path);
            if (position.isNullOrEmpty()) {
                return null;
            } else if (position.isMultiPosition()) {
                return readMultiCharArray(position.asMultiPosition());
            } else {
                return readSingleCharArray(position);
            }
        } finally {
            in.position(currentPos);
        }
    }

    private char[] readMultiCharArray(List<PortablePosition> positions) throws IOException {
        char[] result = new char[positions.size()];
        for (int i = 0; i < result.length; i++) {
            PortablePosition position = positions.get(i);
            validateNonNullOrEmptyPosition(position);
            validateType(position, FieldType.CHAR);
            result[i] = in.readChar(position.getStreamPosition());
        }
        return result;
    }

    private char[] readSingleCharArray(PortablePosition position) throws IOException {
        validateType(position, FieldType.CHAR_ARRAY);
        in.position(position.getStreamPosition());
        return in.readCharArray();
    }

    @Override
    public int[] readIntArray(String path) throws IOException {
        final int currentPos = in.position();
        try {
            PortablePosition position = findPositionForReading(path);
            if (position.isNullOrEmpty()) {
                return null;
            } else if (position.isMultiPosition()) {
                return readMultiIntArray(position.asMultiPosition());
            } else {
                return readSingleIntArray(position);
            }
        } finally {
            in.position(currentPos);
        }
    }

    private int[] readMultiIntArray(List<PortablePosition> positions) throws IOException {
        int[] result = new int[positions.size()];
        for (int i = 0; i < result.length; i++) {
            PortablePosition position = positions.get(i);
            validateNonNullOrEmptyPosition(position);
            validateType(position, FieldType.INT);
            result[i] = in.readInt(position.getStreamPosition());
        }
        return result;
    }

    private int[] readSingleIntArray(PortablePosition position) throws IOException {
        validateType(position, FieldType.INT_ARRAY);
        in.position(position.getStreamPosition());
        return in.readIntArray();
    }

    @Override
    public long[] readLongArray(String path) throws IOException {
        final int currentPos = in.position();
        try {
            PortablePosition position = findPositionForReading(path);
            if (position.isNullOrEmpty()) {
                return null;
            } else if (position.isMultiPosition()) {
                return readMultiLongArray(position.asMultiPosition());
            } else {
                return readSingleLongArray(position);
            }
        } finally {
            in.position(currentPos);
        }
    }

    private long[] readMultiLongArray(List<PortablePosition> positions) throws IOException {
        long[] result = new long[positions.size()];
        for (int i = 0; i < result.length; i++) {
            PortablePosition position = positions.get(i);
            validateNonNullOrEmptyPosition(position);
            validateType(position, FieldType.LONG);
            result[i] = in.readLong(position.getStreamPosition());
        }
        return result;
    }

    private long[] readSingleLongArray(PortablePosition position) throws IOException {
        validateType(position, FieldType.LONG_ARRAY);
        in.position(position.getStreamPosition());
        return in.readLongArray();
    }

    @Override
    public double[] readDoubleArray(String path) throws IOException {
        final int currentPos = in.position();
        try {
            PortablePosition position = findPositionForReading(path);
            if (position.isNullOrEmpty()) {
                return null;
            } else if (position.isMultiPosition()) {
                return readMultiDoubleArray(position.asMultiPosition());
            } else {
                return readSingleDoubleArray(position);
            }
        } finally {
            in.position(currentPos);
        }
    }

    private double[] readMultiDoubleArray(List<PortablePosition> positions) throws IOException {
        double[] result = new double[positions.size()];
        for (int i = 0; i < result.length; i++) {
            PortablePosition position = positions.get(i);
            validateNonNullOrEmptyPosition(position);
            validateType(position, FieldType.DOUBLE);
            result[i] = in.readDouble(position.getStreamPosition());
        }
        return result;
    }

    private double[] readSingleDoubleArray(PortablePosition position) throws IOException {
        validateType(position, FieldType.DOUBLE_ARRAY);
        in.position(position.getStreamPosition());
        return in.readDoubleArray();
    }

    @Override
    public float[] readFloatArray(String path) throws IOException {
        final int currentPos = in.position();
        try {
            PortablePosition position = findPositionForReading(path);
            if (position.isNullOrEmpty()) {
                return null;
            } else if (position.isMultiPosition()) {
                return readMultiFloatArray(position.asMultiPosition());
            } else {
                return readSingleFloatArray(position);
            }
        } finally {
            in.position(currentPos);
        }
    }

    private float[] readMultiFloatArray(List<PortablePosition> positions) throws IOException {
        float[] result = new float[positions.size()];
        for (int i = 0; i < result.length; i++) {
            PortablePosition position = positions.get(i);
            validateNonNullOrEmptyPosition(position);
            validateType(position, FieldType.FLOAT);
            result[i] = in.readFloat(position.getStreamPosition());
        }
        return result;
    }

    private float[] readSingleFloatArray(PortablePosition position) throws IOException {
        validateType(position, FieldType.FLOAT_ARRAY);
        in.position(position.getStreamPosition());
        return in.readFloatArray();
    }

    @Override
    public short[] readShortArray(String path) throws IOException {
        final int currentPos = in.position();
        try {
            PortablePosition position = findPositionForReading(path);
            if (position.isNullOrEmpty()) {
                return null;
            } else if (position.isMultiPosition()) {
                return readMultiShortArray(position.asMultiPosition());
            } else {
                return readSingleShortArray(position);
            }
        } finally {
            in.position(currentPos);
        }
    }

    private short[] readMultiShortArray(List<PortablePosition> positions) throws IOException {
        short[] result = new short[positions.size()];
        for (int i = 0; i < result.length; i++) {
            PortablePosition position = positions.get(i);
            validateNonNullOrEmptyPosition(position);
            validateType(position, FieldType.SHORT);
            result[i] = in.readShort(position.getStreamPosition());
        }
        return result;
    }

    private short[] readSingleShortArray(PortablePosition position) throws IOException {
        validateType(position, FieldType.SHORT_ARRAY);
        in.position(position.getStreamPosition());
        return in.readShortArray();
    }

    @Override
    public String[] readUTFArray(String path) throws IOException {
        final int currentPos = in.position();
        try {
            PortablePosition position = findPositionForReading(path);
            if (position.isNullOrEmpty()) {
                return null;
            } else if (position.isMultiPosition()) {
                return readMultiUTFArray(position.asMultiPosition());
            } else {
                return readSingleUTFArray(position);
            }
        } finally {
            in.position(currentPos);
        }
    }

    private String[] readMultiUTFArray(List<PortablePosition> positions) throws IOException {
        String[] result = new String[positions.size()];
        for (int i = 0; i < result.length; i++) {
            PortablePosition position = positions.get(i);
            if (!position.isNullOrEmpty()) {
                validateType(position, FieldType.UTF);
                in.position(position.getStreamPosition());
                result[i] = in.readUTF();
            }
        }
        return result;
    }

    private String[] readSingleUTFArray(PortablePosition position) throws IOException {
        validateType(position, FieldType.UTF_ARRAY);
        in.position(position.getStreamPosition());
        return in.readUTFArray();
    }

    @Override
    public Portable[] readPortableArray(String fieldName) throws IOException {
        final int currentPos = in.position();
        try {
            PortablePosition position = findPositionForReading(fieldName);
            if (position.isMultiPosition()) {
                return readMultiPortableArray(position.asMultiPosition());
            } else if (position.isNull()) {
                return null;
            } else if (position.isEmpty() && position.isAny()) {
                return null;
            } else {
                return readSinglePortableArray(position);
            }
        } finally {
            in.position(currentPos);
        }
    }

    private Portable[] readSinglePortableArray(PortablePosition position) throws IOException {
        in.position(position.getStreamPosition());
        if (position.getLen() == Bits.NULL_ARRAY_LENGTH) {
            return null;
        }

        validateType(position, FieldType.PORTABLE_ARRAY);
        final Portable[] portables = new Portable[position.getLen()];
        for (int index = 0; index < position.getLen(); index++) {
            in.position(getPortableArrayCellPosition(in, position.getStreamPosition(), index));
            portables[index] = serializer.readAndInitialize(in, position.getFactoryId(), position.getClassId());
        }
        return portables;
    }

    private Portable[] readMultiPortableArray(List<PortablePosition> positions) throws IOException {
        final Portable[] portables = new Portable[positions.size()];
        for (int i = 0; i < portables.length; i++) {
            PortablePosition position = positions.get(i);
            if (!position.isNullOrEmpty()) {
                validateType(position, FieldType.PORTABLE);
                in.position(position.getStreamPosition());
                portables[i] = serializer.readAndInitialize(in, position.getFactoryId(), position.getClassId());
            }
        }
        return portables;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void read(String path, ValueCallback callback) {
        try {
            Object result = read(path);
            if (result instanceof MultiResult) {
                MultiResult multiResult = (MultiResult) result;
                for (Object singleResult : multiResult.getResults()) {
                    callback.onResult(singleResult);
                }
            } else {
                callback.onResult(result);
            }
        } catch (IOException e) {
            throw new ValueReadingException(e.getMessage(), e);
        } catch (RuntimeException e) {
            throw new ValueReadingException(e.getMessage(), e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void read(String path, ValueCollector collector) {
        try {
            Object result = read(path);
            if (result instanceof MultiResult) {
                MultiResult multiResult = (MultiResult) result;
                for (Object singleResult : multiResult.getResults()) {
                    collector.addObject(singleResult);
                }
            } else {
                collector.addObject(result);
            }
        } catch (IOException e) {
            throw new ValueReadingException(e.getMessage(), e);
        } catch (RuntimeException e) {
            throw new ValueReadingException(e.getMessage(), e);
        }
    }

    public Object read(String path) throws IOException {
        final int currentPos = in.position();
        try {
            PortablePosition position = findPositionForReading(path);
            if (position.isMultiPosition()) {
                return readMultiPosition(position.asMultiPosition());
            } else if (position.isNull()) {
                if (position.isAny()) {
                    return NULL_EMPTY_TARGET_MULTIRESULT;
                }
                return null;
            } else if (position.isEmpty()) {
                if (position.isLeaf() && position.getType() != null) {
                    return readSinglePosition(position);
                } else {
                    if (position.isAny()) {
                        return NULL_EMPTY_TARGET_MULTIRESULT;
                    }
                    return null;
                }
            } else {
                return readSinglePosition(position);
            }
        } finally {
            in.position(currentPos);
        }
    }

    private <T> MultiResult<T> readMultiPosition(List<PortablePosition> positions) throws IOException {
        MultiResult<T> result = new MultiResult<T>();
        for (PortablePosition position : positions) {
            if (!position.isNullOrEmpty()) {
                T read = readSinglePosition(position);
                result.add(read);
            } else {
                result.addNullOrEmptyTarget();
            }
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private <T> T readSinglePosition(PortablePosition position) throws IOException {
        if (position.getIndex() >= 0) {
            return readSinglePositionFromArray(position);
        }
        return readSinglePositionFromNonArray(position);
    }

    private PortablePosition findPositionForReading(String path) throws IOException {
        if (raw) {
            throw new HazelcastSerializationException("Cannot read Portable fields after getRawDataInput() is called!");
        }
        try {
            return PortablePositionNavigator.findPositionForReading(ctx, path, pathCursor);
        } finally {
            // The context is reset each time to enable its reuse in consecutive calls and avoid allocation
            ctx.reset();
        }
    }

    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:returncount", "unchecked"})
    private <T> T readSinglePositionFromArray(PortablePosition position) throws IOException {
        assert position.getType() != null : "Unsupported type read: null";
        switch (position.getType()) {
            case BYTE:
            case BYTE_ARRAY:
                return (T) Byte.valueOf(in.readByte(position.getStreamPosition()));
            case SHORT:
            case SHORT_ARRAY:
                return (T) Short.valueOf(in.readShort(position.getStreamPosition()));
            case INT:
            case INT_ARRAY:
                return (T) Integer.valueOf(in.readInt(position.getStreamPosition()));
            case LONG:
            case LONG_ARRAY:
                return (T) Long.valueOf(in.readLong(position.getStreamPosition()));
            case FLOAT:
            case FLOAT_ARRAY:
                return (T) Float.valueOf(in.readFloat(position.getStreamPosition()));
            case DOUBLE:
            case DOUBLE_ARRAY:
                return (T) Double.valueOf(in.readDouble(position.getStreamPosition()));
            case BOOLEAN:
            case BOOLEAN_ARRAY:
                return (T) Boolean.valueOf(in.readBoolean(position.getStreamPosition()));
            case CHAR:
            case CHAR_ARRAY:
                return (T) Character.valueOf(in.readChar(position.getStreamPosition()));
            case UTF:
            case UTF_ARRAY:
                in.position(position.getStreamPosition());
                return (T) in.readUTF();
            case PORTABLE:
            case PORTABLE_ARRAY:
                in.position(position.getStreamPosition());
                return (T) serializer.readAndInitialize(in, position.getFactoryId(), position.getClassId());
            default:
                throw new IllegalArgumentException("Unsupported type: " + position.getType());
        }
    }

    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:returncount", "unchecked"})
    private <T> T readSinglePositionFromNonArray(PortablePosition position) throws IOException {
        assert position.getType() != null : "Unsupported type read: null";
        switch (position.getType()) {
            case BYTE:
                return (T) Byte.valueOf(in.readByte(position.getStreamPosition()));
            case BYTE_ARRAY:
                return (T) readSingleByteArray(position);
            case SHORT:
                return (T) Short.valueOf(in.readShort(position.getStreamPosition()));
            case SHORT_ARRAY:
                return (T) readSingleShortArray(position);
            case INT:
                return (T) Integer.valueOf(in.readInt(position.getStreamPosition()));
            case INT_ARRAY:
                return (T) readSingleIntArray(position);
            case LONG:
                return (T) Long.valueOf(in.readLong(position.getStreamPosition()));
            case LONG_ARRAY:
                return (T) readSingleLongArray(position);
            case FLOAT:
                return (T) Float.valueOf(in.readFloat(position.getStreamPosition()));
            case FLOAT_ARRAY:
                return (T) readSingleFloatArray(position);
            case DOUBLE:
                return (T) Double.valueOf(in.readDouble(position.getStreamPosition()));
            case DOUBLE_ARRAY:
                return (T) readSingleDoubleArray(position);
            case BOOLEAN:
                return (T) Boolean.valueOf(in.readBoolean(position.getStreamPosition()));
            case BOOLEAN_ARRAY:
                return (T) readSingleBooleanArray(position);
            case CHAR:
                return (T) Character.valueOf(in.readChar(position.getStreamPosition()));
            case CHAR_ARRAY:
                return (T) readSingleCharArray(position);
            case UTF:
                in.position(position.getStreamPosition());
                return (T) in.readUTF();
            case UTF_ARRAY:
                return (T) readSingleUTFArray(position);
            case PORTABLE:
                in.position(position.getStreamPosition());
                return (T) serializer.readAndInitialize(in, position.getFactoryId(), position.getClassId());
            case PORTABLE_ARRAY:
                return (T) readSinglePortableArray(position);
            default:
                throw new IllegalArgumentException("Unsupported type " + position.getType());
        }
    }

    private void validatePrimitive(PortablePosition position, FieldType expectedType) {
        validateNonNullOrEmptyPosition(position);
        validateNotMultiPosition(position);
        validateType(position, expectedType);
    }

    private void validateNonNullOrEmptyPosition(PortablePosition position) {
        if (position.isNullOrEmpty()) {
            throw new IllegalArgumentException("Primitive type cannot be returned since the result is/contains null.");
        }
    }

    private void validateNotMultiPosition(PortablePosition position) {
        if (position.isMultiPosition()) {
            throw new IllegalArgumentException("The method expected a single result but multiple results have been returned."
                    + " Did you use the [any] quantifier? If so, use the readArray method family.");
        }
    }

    private void validateType(PortablePosition position, FieldType expectedType) {
        FieldType returnedType = position.getType();
        if (position.getIndex() >= 0) {
            returnedType = returnedType != null ? returnedType.getSingleType() : null;
        }
        if (expectedType != returnedType) {
            String name = returnedType != null ? returnedType.name() : null;
            throw new IllegalArgumentException("Wrong type read! Actual: " + name + " Expected: " + expectedType.name()
                    + ". Did you use a correct read method? E.g. readInt() for int.");
        }
    }
}
