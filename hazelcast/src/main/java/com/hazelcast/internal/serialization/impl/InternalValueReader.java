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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.nio.Bits;
import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.query.extractor.ValueCallback;
import com.hazelcast.query.extractor.ValueCollector;
import com.hazelcast.query.extractor.ValueReader;
import com.hazelcast.query.extractor.ValueReadingException;
import com.hazelcast.query.impl.getters.ImmutableMultiResult;
import com.hazelcast.query.impl.getters.MultiResult;

import java.io.IOException;
import java.util.List;

import static com.hazelcast.internal.serialization.impl.PortableUtils.getPortableArrayCellPosition;

/**
 * Can't be accessed concurrently.
 */
public final class InternalValueReader implements ValueReader {

    private static final MultiResult NULL_EMPTY_TARGET_MULTIRESULT;

    static {
        MultiResult<Object> result = new MultiResult<Object>();
        result.addNullOrEmptyTarget();
        NULL_EMPTY_TARGET_MULTIRESULT = new ImmutableMultiResult<Object>(result);
    }

    private final PortableSerializer serializer;

    private final BufferObjectDataInput in;

    private final PortableNavigatorContext ctx;
    private final PortablePathCursor pathCursor;

    InternalValueReader(PortableSerializer serializer, BufferObjectDataInput in, ClassDefinition cd) {
        this.in = in;
        this.serializer = serializer;
        this.ctx = new PortableNavigatorContext(in, cd, serializer);
        this.pathCursor = new PortablePathCursor();
    }

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

    private Portable[] readPortableArray(PortablePosition position) throws IOException {
        if (position.getLen() == Bits.NULL_ARRAY_LENGTH) {
            return null;
        }

        final Portable[] portables = new Portable[position.getLen()];
        for (int index = 0; index < position.getLen(); index++) {
            in.position(getPortableArrayCellPosition(in, position.getStreamPosition(), index));
            portables[index] = serializer.readAndInitialize(in, position.getFactoryId(), position.getClassId());
        }
        return portables;
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
        in.position(position.getStreamPosition());
        switch (position.getType()) {
            case BYTE:
            case BYTE_ARRAY:
                return (T) Byte.valueOf(in.readByte());
            case SHORT:
            case SHORT_ARRAY:
                return (T) Short.valueOf(in.readShort());
            case INT:
            case INT_ARRAY:
                return (T) Integer.valueOf(in.readInt());
            case LONG:
            case LONG_ARRAY:
                return (T) Long.valueOf(in.readLong());
            case FLOAT:
            case FLOAT_ARRAY:
                return (T) Float.valueOf(in.readFloat());
            case DOUBLE:
            case DOUBLE_ARRAY:
                return (T) Double.valueOf(in.readDouble());
            case BOOLEAN:
            case BOOLEAN_ARRAY:
                return (T) Boolean.valueOf(in.readBoolean());
            case CHAR:
            case CHAR_ARRAY:
                return (T) Character.valueOf(in.readChar());
            case UTF:
            case UTF_ARRAY:
                return (T) in.readUTF();
            case PORTABLE:
            case PORTABLE_ARRAY:
                return (T) serializer.readAndInitialize(in, position.getFactoryId(), position.getClassId());
            default:
                throw new IllegalArgumentException("Unsupported type: " + position.getType());
        }
    }

    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:returncount", "unchecked"})
    private <T> T readSinglePositionFromNonArray(PortablePosition position) throws IOException {
        assert position.getType() != null : "Unsupported type read: null";
        in.position(position.getStreamPosition());
        switch (position.getType()) {
            case BYTE:
                return (T) Byte.valueOf(in.readByte());
            case BYTE_ARRAY:
                return (T) in.readByteArray();
            case SHORT:
                return (T) Short.valueOf(in.readShort());
            case SHORT_ARRAY:
                return (T) in.readShortArray();
            case INT:
                return (T) Integer.valueOf(in.readInt());
            case INT_ARRAY:
                return (T) in.readIntArray();
            case LONG:
                return (T) Long.valueOf(in.readLong());
            case LONG_ARRAY:
                return (T) in.readLongArray();
            case FLOAT:
                return (T) Float.valueOf(in.readFloat());
            case FLOAT_ARRAY:
                return (T) in.readFloatArray();
            case DOUBLE:
                return (T) Double.valueOf(in.readDouble());
            case DOUBLE_ARRAY:
                return (T) in.readDoubleArray();
            case BOOLEAN:
                return (T) Boolean.valueOf(in.readBoolean());
            case BOOLEAN_ARRAY:
                return (T) in.readBooleanArray();
            case CHAR:
                return (T) Character.valueOf(in.readChar());
            case CHAR_ARRAY:
                return (T) in.readCharArray();
            case UTF:
                return (T) in.readUTF();
            case UTF_ARRAY:
                return (T) in.readUTFArray();
            case PORTABLE:
                return (T) serializer.readAndInitialize(in, position.getFactoryId(), position.getClassId());
            case PORTABLE_ARRAY:
                return (T) readPortableArray(position);
            default:
                throw new IllegalArgumentException("Unsupported type " + position.getType());
        }
    }
}
