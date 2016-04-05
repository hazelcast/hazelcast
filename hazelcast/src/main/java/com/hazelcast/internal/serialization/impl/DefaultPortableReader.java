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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.query.impl.getters.MultiResult;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Can't be accessed concurrently
 */
public class DefaultPortableReader implements PortableReader {

    private final BufferObjectDataInput in;
    protected final ClassDefinition cd;
    protected final PortableSerializer serializer;

    private final PortablePositionNavigator navigator;
    private final int finalPosition;
    private final int offset;
    private boolean raw;

    private IllegalArgumentException PRIMITIVE_NULL_EXCEPTION = new PrimitiveNullException();

    public DefaultPortableReader(PortableSerializer serializer, BufferObjectDataInput in, ClassDefinition cd) {
        this.in = in;
        this.serializer = serializer;
        this.cd = cd;

        this.navigator = new PortablePositionNavigator();
        this.navigator.init(in, cd, serializer);

        this.finalPosition = navigator.getFinalPosition();
        this.offset = navigator.getOffset();
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
    public byte readByte(String path) throws IOException {
        validateQuantifier(path);
        PortablePosition pos = navigator.findPositionOfPrimitiveObject(path, FieldType.BYTE);
        validatePrimitiveNull(pos);
        return in.readByte(pos.getStreamPosition());
    }

    @Override
    public short readShort(String path) throws IOException {
        validateQuantifier(path);
        PortablePosition pos = navigator.findPositionOfPrimitiveObject(path, FieldType.SHORT);
        validatePrimitiveNull(pos);
        return in.readShort(pos.getStreamPosition());
    }

    @Override
    public int readInt(String path) throws IOException {
        validateQuantifier(path);
        PortablePosition pos = navigator.findPositionOfPrimitiveObject(path, FieldType.INT);
        validatePrimitiveNull(pos);
        return in.readInt(pos.getStreamPosition());
    }

    @Override
    public long readLong(String path) throws IOException {
        validateQuantifier(path);
        PortablePosition pos = navigator.findPositionOfPrimitiveObject(path, FieldType.LONG);
        validatePrimitiveNull(pos);
        return in.readLong(pos.getStreamPosition());
    }

    @Override
    public float readFloat(String path) throws IOException {
        validateQuantifier(path);
        PortablePosition pos = navigator.findPositionOfPrimitiveObject(path, FieldType.FLOAT);
        validatePrimitiveNull(pos);
        return in.readFloat(pos.getStreamPosition());
    }

    @Override
    public double readDouble(String path) throws IOException {
        validateQuantifier(path);
        PortablePosition pos = navigator.findPositionOfPrimitiveObject(path, FieldType.DOUBLE);
        validatePrimitiveNull(pos);
        return in.readDouble(pos.getStreamPosition());
    }

    @Override
    public boolean readBoolean(String path) throws IOException {
        validateQuantifier(path);
        PortablePosition pos = navigator.findPositionOfPrimitiveObject(path, FieldType.BOOLEAN);
        validatePrimitiveNull(pos);
        return in.readBoolean(pos.getStreamPosition());
    }

    @Override
    public char readChar(String path) throws IOException {
        validateQuantifier(path);
        PortablePosition pos = navigator.findPositionOfPrimitiveObject(path, FieldType.CHAR);
        validatePrimitiveNull(pos);
        return in.readChar(pos.getStreamPosition());
    }

    @Override
    public String readUTF(String path) throws IOException {
        validateQuantifier(path);
        final int currentPos = in.position();
        try {
            PortablePosition position = navigator.findPositionOfPrimitiveObject(path, FieldType.UTF);
            if (position.isNullOrEmpty()) {
                return null;
            }
            in.position(position.getStreamPosition());
            return in.readUTF();
        } finally {
            in.position(currentPos);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Portable readPortable(String path) throws IOException {
        validateQuantifier(path);
        final int currentPos = in.position();
        try {
            PortablePosition position = navigator.findPositionOfPortableObject(path);
            if (position.isNullOrEmpty()) {
                return null;
            }
            in.position(position.getStreamPosition());
            return serializer.readAndInitialize(in, position.getFactoryId(), position.getClassId());
        } finally {
            in.position(currentPos);
        }
    }

    private void validateQuantifier(String path) {
        if (path.contains("[any]")) {
            throw new IllegalArgumentException("Invalid method for [any] quantifier. Use the readArray method family.");
        }
    }

    private void validatePrimitiveNull(PortablePosition position) {
        if (position.isNullOrEmpty()) {
            throw PRIMITIVE_NULL_EXCEPTION;
        }
    }

    @Override
    public byte[] readByteArray(String path) throws IOException {
        final int currentPos = in.position();
        try {
            PortablePosition position = navigator.findPositionOfPrimitiveArray(path, FieldType.BYTE_ARRAY);
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
            result[i] = in.readByte(positions.get(i).getStreamPosition());
        }
        return result;
    }

    private byte[] readSingleByteArray(PortablePosition position) throws IOException {
        in.position(position.getStreamPosition());
        return in.readByteArray();
    }

    @Override
    public boolean[] readBooleanArray(String path) throws IOException {
        final int currentPos = in.position();
        try {
            PortablePosition position = navigator.findPositionOfPrimitiveArray(path, FieldType.BOOLEAN_ARRAY);
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
            result[i] = in.readBoolean(positions.get(i).getStreamPosition());
        }
        return result;
    }

    private boolean[] readSingleBooleanArray(PortablePosition position) throws IOException {
        in.position(position.getStreamPosition());
        return in.readBooleanArray();
    }

    @Override
    public char[] readCharArray(String path) throws IOException {
        final int currentPos = in.position();
        try {
            PortablePosition position = navigator.findPositionOfPrimitiveArray(path, FieldType.CHAR_ARRAY);
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
            result[i] = in.readChar(positions.get(i).getStreamPosition());
        }
        return result;
    }

    private char[] readSingleCharArray(PortablePosition position) throws IOException {
        in.position(position.getStreamPosition());
        return in.readCharArray();
    }

    @Override
    public int[] readIntArray(String path) throws IOException {
        final int currentPos = in.position();
        try {
            PortablePosition position = navigator.findPositionOfPrimitiveArray(path, FieldType.INT_ARRAY);
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
            result[i] = in.readInt(positions.get(i).getStreamPosition());
        }
        return result;
    }

    private int[] readSingleIntArray(PortablePosition position) throws IOException {
        in.position(position.getStreamPosition());
        return in.readIntArray();
    }

    @Override
    public long[] readLongArray(String path) throws IOException {
        final int currentPos = in.position();
        try {
            PortablePosition position = navigator.findPositionOfPrimitiveArray(path, FieldType.LONG_ARRAY);
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
            // TODO
//            if (!position.isNull()) {
                result[i] = in.readLong(position.getStreamPosition());
//            }
        }
        return result;
    }

    private PortablePosition validateNullPosition(PortablePosition position) {
        if (position.isNullOrEmpty()) {
            throw PRIMITIVE_NULL_EXCEPTION;
        }
        return position;
    }

    private long[] readSingleLongArray(PortablePosition position) throws IOException {
        in.position(position.getStreamPosition());
        return in.readLongArray();
    }

    @Override
    public double[] readDoubleArray(String path) throws IOException {
        final int currentPos = in.position();
        try {
            PortablePosition position = navigator.findPositionOfPrimitiveArray(path, FieldType.DOUBLE_ARRAY);
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
            result[i] = in.readDouble(positions.get(i).getStreamPosition());
        }
        return result;
    }

    private double[] readSingleDoubleArray(PortablePosition position) throws IOException {
        in.position(position.getStreamPosition());
        return in.readDoubleArray();
    }

    @Override
    public float[] readFloatArray(String path) throws IOException {
        final int currentPos = in.position();
        try {
            PortablePosition position = navigator.findPositionOfPrimitiveArray(path, FieldType.FLOAT_ARRAY);
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
            result[i] = in.readFloat(positions.get(i).getStreamPosition());
        }
        return result;
    }

    private float[] readSingleFloatArray(PortablePosition position) throws IOException {
        in.position(position.getStreamPosition());
        return in.readFloatArray();
    }

    @Override
    public short[] readShortArray(String path) throws IOException {
        final int currentPos = in.position();
        try {
            PortablePosition position = navigator.findPositionOfPrimitiveArray(path, FieldType.SHORT_ARRAY);
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
            result[i] = in.readShort(positions.get(i).getStreamPosition());
        }
        return result;
    }

    private short[] readSingleShortArray(PortablePosition position) throws IOException {
        in.position(position.getStreamPosition());
        return in.readShortArray();
    }

    @Override
    public String[] readUTFArray(String path) throws IOException {
        final int currentPos = in.position();
        try {
            PortablePosition position = navigator.findPositionOfPrimitiveArray(path, FieldType.UTF_ARRAY);
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
            in.position(positions.get(i).getStreamPosition());
            result[i] = in.readUTF();
        }
        return result;
    }

    private String[] readSingleUTFArray(PortablePosition position) throws IOException {
        in.position(position.getStreamPosition());
        return in.readUTFArray();
    }

    @Override
    public Portable[] readPortableArray(String fieldName) throws IOException {
        final int currentPos = in.position();
        try {
            PortablePosition position = navigator.findPositionOfPortableArray(fieldName);
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

        final Portable[] portables = new Portable[position.getLen()];
        for (int index = 0; index < position.getLen(); index++) {
            in.position(PortablePositionNavigator.getArrayCellPosition(position, index, in));
            portables[index] = serializer.readAndInitialize(in, position.getFactoryId(), position.getClassId());
        }
        return portables;
    }

    private Portable[] readMultiPortableArray(List<PortablePosition> positions) throws IOException {
        final Portable[] portables = new Portable[positions.size()];
        for (int i = 0; i < portables.length; i++) {
            PortablePosition position = positions.get(i);
            if (!position.isNullOrEmpty()) {
                in.position(position.getStreamPosition());
                portables[i] = serializer.readAndInitialize(in, position.getFactoryId(), position.getClassId());
            }
        }
        return portables;
    }

    public Object read(String path) throws IOException {
        final int currentPos = in.position();
        try {
            PortablePosition position = navigator.findPositionOf(path);
            if (position.isMultiPosition()) {
                return readMultiPosition(position.asMultiPosition());
            } else if (position.isNull()) {
                return null;
            } else if (position.isEmpty()) {
                if(position.isLast() && position.getType() != null) {
                    return readSinglePosition(position);
                } else {
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
            T read = null;
            if (!position.isNullOrEmpty()) {
                read = readSinglePosition(position);
            }
            result.add(read);
        }
        return result;
    }

    // TODO -> refactor to O(1) lookup of proper strategy
    @SuppressWarnings("unchecked")
    private <T> T readSinglePosition(PortablePosition position) throws IOException {
        if(position.getIndex() >= 0) {
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
                    throw new IllegalArgumentException("Unsupported type " + position.getType());
            }
        }
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
                return (T) serializer.readAndInitialize(in, position.getFactoryId(), position.getClassId());
            case PORTABLE_ARRAY:
                return (T) readSinglePortableArray(position);
            default:
                throw new IllegalArgumentException("Unsupported type " + position.getType());
        }
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

    final void end() throws IOException {
        in.position(finalPosition);
    }

    // TODO - document
    // TODO - maybe
    private static final class PrimitiveNullException extends IllegalArgumentException {
        public PrimitiveNullException() {
            super("Primitive type cannot be returned since the result is null");
        }
    }

}
