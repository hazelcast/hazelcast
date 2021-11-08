/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.serialization.impl.compact;

import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.impl.FieldOperations;
import com.hazelcast.internal.serialization.impl.InternalGenericRecord;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.serialization.FieldKind;
import com.hazelcast.nio.serialization.GenericRecord;
import com.hazelcast.nio.serialization.GenericRecordBuilder;
import com.hazelcast.nio.serialization.HazelcastSerializationException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Set;
import java.util.function.Function;

import static com.hazelcast.internal.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.NULL_ARRAY_LENGTH;
import static com.hazelcast.internal.nio.Bits.SHORT_SIZE_IN_BYTES;
import static com.hazelcast.internal.serialization.impl.compact.CompactUtil.exceptionForUnexpectedNullValue;
import static com.hazelcast.internal.serialization.impl.compact.CompactUtil.exceptionForUnexpectedNullValueInArray;
import static com.hazelcast.internal.serialization.impl.compact.OffsetReader.BYTE_OFFSET_READER;
import static com.hazelcast.internal.serialization.impl.compact.OffsetReader.BYTE_OFFSET_READER_RANGE;
import static com.hazelcast.internal.serialization.impl.compact.OffsetReader.INT_OFFSET_READER;
import static com.hazelcast.internal.serialization.impl.compact.OffsetReader.NULL_OFFSET;
import static com.hazelcast.internal.serialization.impl.compact.OffsetReader.SHORT_OFFSET_READER;
import static com.hazelcast.internal.serialization.impl.compact.OffsetReader.SHORT_OFFSET_READER_RANGE;
import static com.hazelcast.internal.util.Preconditions.checkNotNegative;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_BOOLEANS;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_BYTES;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_COMPACTS;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_DATES;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_DECIMALS;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_DOUBLES;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_FLOATS;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_INTS;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_LONGS;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_BOOLEANS;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_BYTES;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_DOUBLES;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_FLOATS;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_INTS;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_LONGS;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_SHORTS;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_SHORTS;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_STRINGS;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_TIMES;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_TIMESTAMPS;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_TIMESTAMP_WITH_TIMEZONES;
import static com.hazelcast.nio.serialization.FieldKind.BOOLEAN;
import static com.hazelcast.nio.serialization.FieldKind.COMPACT;
import static com.hazelcast.nio.serialization.FieldKind.DATE;
import static com.hazelcast.nio.serialization.FieldKind.DECIMAL;
import static com.hazelcast.nio.serialization.FieldKind.STRING;
import static com.hazelcast.nio.serialization.FieldKind.TIME;
import static com.hazelcast.nio.serialization.FieldKind.TIMESTAMP;
import static com.hazelcast.nio.serialization.FieldKind.TIMESTAMP_WITH_TIMEZONE;

/**
 * A base class that has the capability of representing Compact serialized
 * objects as {@link InternalGenericRecord}s. This class is not instantiated
 * directly, but its subclass {@link DefaultCompactReader} is used in the
 * query system, as well as in returning a GenericRecord to the user.
 */
public class CompactInternalGenericRecord extends CompactGenericRecord implements InternalGenericRecord {

    private final OffsetReader offsetReader;
    private final Schema schema;
    private final BufferObjectDataInput in;
    private final int finalPosition;
    private final int dataStartPosition;
    private final int variableOffsetsPosition;
    private final CompactStreamSerializer serializer;
    private final boolean schemaIncludedInBinary;
    private final @Nullable
    Class associatedClass;

    public CompactInternalGenericRecord(CompactStreamSerializer serializer, BufferObjectDataInput in, Schema schema,
                                        @Nullable Class associatedClass, boolean schemaIncludedInBinary) {
        this.in = in;
        this.serializer = serializer;
        this.schema = schema;
        this.associatedClass = associatedClass;
        this.schemaIncludedInBinary = schemaIncludedInBinary;
        try {
            int numberOfVariableLengthFields = schema.getNumberOfVariableSizeFields();
            if (numberOfVariableLengthFields != 0) {
                int dataLength = in.readInt();
                dataStartPosition = in.position();
                variableOffsetsPosition = dataStartPosition + dataLength;
                if (dataLength < BYTE_OFFSET_READER_RANGE) {
                    offsetReader = BYTE_OFFSET_READER;
                    finalPosition = variableOffsetsPosition + numberOfVariableLengthFields;
                } else if (dataLength < SHORT_OFFSET_READER_RANGE) {
                    offsetReader = SHORT_OFFSET_READER;
                    finalPosition = variableOffsetsPosition + (numberOfVariableLengthFields * SHORT_SIZE_IN_BYTES);
                } else {
                    offsetReader = INT_OFFSET_READER;
                    finalPosition = variableOffsetsPosition + (numberOfVariableLengthFields * INT_SIZE_IN_BYTES);
                }
                //set the position to final so that the next one to read something from `in` can start from
                //correct position
                in.position(finalPosition);
            } else {
                offsetReader = INT_OFFSET_READER;
                variableOffsetsPosition = 0;
                dataStartPosition = in.position();
                finalPosition = dataStartPosition + schema.getFixedSizeFieldsLength();
            }
        } catch (IOException e) {
            throw illegalStateException(e);
        }
    }

    @Nullable
    public Class getAssociatedClass() {
        return associatedClass;
    }

    public BufferObjectDataInput getIn() {
        return in;
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

    @Override
    @Nonnull
    public GenericRecordBuilder newBuilder() {
        return serializer.createGenericRecordBuilder(schema);
    }

    @Override
    @Nonnull
    public GenericRecordBuilder cloneWithBuilder() {
        return serializer.createGenericRecordCloner(schema, this);
    }

    @Override
    @Nonnull
    public FieldKind getFieldKind(@Nonnull String fieldName) {
        return schema.getField(fieldName).getKind();
    }

    @Override
    public boolean hasField(@Nonnull String fieldName) {
        return schema.hasField(fieldName);
    }

    @Nonnull
    @Override
    public Set<String> getFieldNames() {
        return schema.getFieldNames();
    }

    @Override
    public boolean getBoolean(@Nonnull String fieldName) {
        FieldDescriptor fd = getFieldDefinition(fieldName);
        FieldKind fieldKind = fd.getKind();
        switch (fieldKind) {
            case BOOLEAN:
                return getBoolean(fd);
            case NULLABLE_BOOLEAN:
                return getVariableSizeAsNonNull(fd, ObjectDataInput::readBoolean, "Boolean");
            default:
                throw unexpectedFieldKind(BOOLEAN, fieldName);
        }
    }

    private boolean getBoolean(FieldDescriptor fd) {
        try {
            int booleanOffset = fd.getOffset();
            int bitOffset = fd.getBitOffset();
            int getOffset = booleanOffset + dataStartPosition;
            byte lastByte = in.readByte(getOffset);
            return ((lastByte >>> bitOffset) & 1) != 0;
        } catch (IOException e) {
            throw illegalStateException(e);
        }
    }

    @Override
    public byte getByte(@Nonnull String fieldName) {
        FieldDescriptor fd = getFieldDefinition(fieldName);
        FieldKind fieldKind = fd.getKind();
        switch (fieldKind) {
            case BYTE:
                try {
                    return in.readByte(readFixedSizePosition(fd));
                } catch (IOException e) {
                    throw illegalStateException(e);
                }
            case NULLABLE_BYTE:
                return getVariableSizeAsNonNull(fd, ObjectDataInput::readByte, "Byte");
            default:
                throw unexpectedFieldKind(fieldKind, fieldName);
        }
    }

    @Override
    public short getShort(@Nonnull String fieldName) {
        FieldDescriptor fd = getFieldDefinition(fieldName);
        FieldKind fieldKind = fd.getKind();
        switch (fieldKind) {
            case SHORT:
                try {
                    return in.readShort(readFixedSizePosition(fd));
                } catch (IOException e) {
                    throw illegalStateException(e);
                }
            case NULLABLE_SHORT:
                return getVariableSizeAsNonNull(fd, ObjectDataInput::readShort, "Short");
            default:
                throw unexpectedFieldKind(fieldKind, fieldName);
        }
    }

    @Override
    public int getInt(@Nonnull String fieldName) {
        FieldDescriptor fd = getFieldDefinition(fieldName);
        FieldKind fieldKind = fd.getKind();
        switch (fieldKind) {
            case INT:
                try {
                    return in.readInt(readFixedSizePosition(fd));
                } catch (IOException e) {
                    throw illegalStateException(e);
                }
            case NULLABLE_INT:
                return getVariableSizeAsNonNull(fd, ObjectDataInput::readInt, "Int");
            default:
                throw unexpectedFieldKind(fieldKind, fieldName);
        }
    }

    @Override
    public long getLong(@Nonnull String fieldName) {
        FieldDescriptor fd = getFieldDefinition(fieldName);
        FieldKind fieldKind = fd.getKind();
        switch (fieldKind) {
            case LONG:
                try {
                    return in.readLong(readFixedSizePosition(fd));
                } catch (IOException e) {
                    throw illegalStateException(e);
                }
            case NULLABLE_LONG:
                return getVariableSizeAsNonNull(fd, ObjectDataInput::readLong, "Long");
            default:
                throw unexpectedFieldKind(fieldKind, fieldName);
        }
    }

    @Override
    public float getFloat(@Nonnull String fieldName) {
        FieldDescriptor fd = getFieldDefinition(fieldName);
        FieldKind fieldKind = fd.getKind();
        switch (fieldKind) {
            case FLOAT:
                try {
                    return in.readFloat(readFixedSizePosition(fd));
                } catch (IOException e) {
                    throw illegalStateException(e);
                }
            case NULLABLE_FLOAT:
                return getVariableSizeAsNonNull(fd, ObjectDataInput::readFloat, "Float");
            default:
                throw unexpectedFieldKind(fieldKind, fieldName);
        }
    }

    @Override
    public double getDouble(@Nonnull String fieldName) {
        FieldDescriptor fd = getFieldDefinition(fieldName);
        FieldKind fieldKind = fd.getKind();
        switch (fieldKind) {
            case DOUBLE:
                try {
                    return in.readDouble(readFixedSizePosition(fd));
                } catch (IOException e) {
                    throw illegalStateException(e);
                }
            case NULLABLE_DOUBLE:
                return getVariableSizeAsNonNull(fd, ObjectDataInput::readDouble, "Double");
            default:
                throw unexpectedFieldKind(fieldKind, fieldName);
        }
    }

    @Override
    public char getChar(@Nonnull String fieldName) {
        throw new UnsupportedOperationException("Compact format does not support reading a char field");
    }

    @Override
    public String getString(@Nonnull String fieldName) {
        return getVariableSize(fieldName, STRING, BufferObjectDataInput::readString);
    }

    private <T> T getVariableSize(FieldDescriptor fieldDescriptor,
                                  Reader<T> reader) {
        int currentPos = in.position();
        try {
            int pos = readVariableSizeFieldPosition(fieldDescriptor);
            if (pos == NULL_OFFSET) {
                return null;
            }
            in.position(pos);
            return reader.read(in);
        } catch (IOException e) {
            throw illegalStateException(e);
        } finally {
            in.position(currentPos);
        }
    }

    private <T> T getVariableSizeAsNonNull(FieldDescriptor fieldDescriptor,
                                           Reader<T> reader, String methodSuffix) {
        T value = getVariableSize(fieldDescriptor, reader);
        if (value == null) {
            throw exceptionForUnexpectedNullValue(fieldDescriptor.getFieldName(), methodSuffix);
        }
        return value;
    }

    private <T> T getVariableSize(@Nonnull String fieldName, FieldKind fieldKind,
                                  Reader<T> reader) {
        int currentPos = in.position();
        try {
            int pos = readVariableSizeFieldPosition(fieldName, fieldKind);
            if (pos == NULL_OFFSET) {
                return null;
            }
            in.position(pos);
            return reader.read(in);
        } catch (IOException e) {
            throw illegalStateException(e);
        } finally {
            in.position(currentPos);
        }
    }

    @Override
    public BigDecimal getDecimal(@Nonnull String fieldName) {
        return getVariableSize(fieldName, DECIMAL, IOUtil::readBigDecimal);
    }

    @Override
    @Nullable
    public LocalTime getTime(@Nonnull String fieldName) {
        return getVariableSize(fieldName, TIME, IOUtil::readLocalTime);
    }

    @Override
    @Nullable
    public LocalDate getDate(@Nonnull String fieldName) {
        return getVariableSize(fieldName, DATE, IOUtil::readLocalDate);
    }

    @Override
    @Nullable
    public LocalDateTime getTimestamp(@Nonnull String fieldName) {
        return getVariableSize(fieldName, TIMESTAMP, IOUtil::readLocalDateTime);
    }

    @Override
    @Nullable
    public OffsetDateTime getTimestampWithTimezone(@Nonnull String fieldName) {
        return getVariableSize(fieldName, TIMESTAMP_WITH_TIMEZONE, IOUtil::readOffsetDateTime);
    }


    @Override
    @Nullable
    public GenericRecord getGenericRecord(@Nonnull String fieldName) {
        return getVariableSize(fieldName, COMPACT, in -> serializer.readGenericRecord(in, schemaIncludedInBinary));
    }

    @Override
    @Nullable
    public <T> T getObject(@Nonnull String fieldName) {
        return (T) getVariableSize(fieldName, COMPACT, in -> serializer.read(in, schemaIncludedInBinary));
    }

    @Override
    @Nullable
    public boolean[] getArrayOfBooleans(@Nonnull String fieldName) {
        FieldDescriptor fd = getFieldDefinition(fieldName);
        FieldKind fieldKind = fd.getKind();
        switch (fieldKind) {
            case ARRAY_OF_BOOLEANS:
                return getVariableSize(fd, CompactInternalGenericRecord::readBooleanBits);
            case ARRAY_OF_NULLABLE_BOOLEANS:
                return getNullableArrayAsPrimitiveArray(fd, ObjectDataInput::readBooleanArray, "Booleans");
            default:
                throw unexpectedFieldKind(fieldKind, fieldName);
        }
    }

    @Override
    @Nullable
    public byte[] getArrayOfBytes(@Nonnull String fieldName) {
        return getArrayOfPrimitives(fieldName, ObjectDataInput::readByteArray, ARRAY_OF_BYTES, ARRAY_OF_NULLABLE_BYTES, "Bytes");
    }

    @Override
    @Nullable
    public char[] getArrayOfChars(@Nonnull String fieldName) {
        throw new UnsupportedOperationException("Compact format does not support reading an array of chars field");
    }

    @Override
    @Nullable
    public short[] getArrayOfShorts(@Nonnull String fieldName) {
        return getArrayOfPrimitives(fieldName, ObjectDataInput::readShortArray, ARRAY_OF_SHORTS,
                ARRAY_OF_NULLABLE_SHORTS, "Shorts");
    }

    @Override
    @Nullable
    public int[] getArrayOfInts(@Nonnull String fieldName) {
        return getArrayOfPrimitives(fieldName, ObjectDataInput::readIntArray, ARRAY_OF_INTS, ARRAY_OF_NULLABLE_INTS, "Ints");
    }

    @Override
    @Nullable
    public long[] getArrayOfLongs(@Nonnull String fieldName) {
        return getArrayOfPrimitives(fieldName, ObjectDataInput::readLongArray, ARRAY_OF_LONGS, ARRAY_OF_NULLABLE_LONGS, "Longs");
    }

    @Override
    @Nullable
    public float[] getArrayOfFloats(@Nonnull String fieldName) {
        return getArrayOfPrimitives(fieldName, ObjectDataInput::readFloatArray, ARRAY_OF_FLOATS,
                ARRAY_OF_NULLABLE_FLOATS, "Floats");
    }

    @Override
    @Nullable
    public double[] getArrayOfDoubles(@Nonnull String fieldName) {
        return getArrayOfPrimitives(fieldName, ObjectDataInput::readDoubleArray, ARRAY_OF_DOUBLES,
                ARRAY_OF_NULLABLE_DOUBLES, "Doubles");
    }

    @Override
    @Nullable
    public String[] getArrayOfStrings(@Nonnull String fieldName) {
        return getArrayOfVariableSizes(fieldName, ARRAY_OF_STRINGS, String[]::new, ObjectDataInput::readString);
    }

    @Override
    @Nullable
    public BigDecimal[] getArrayOfDecimals(@Nonnull String fieldName) {
        return getArrayOfVariableSizes(fieldName, ARRAY_OF_DECIMALS, BigDecimal[]::new, IOUtil::readBigDecimal);
    }

    @Override
    @Nullable
    public LocalTime[] getArrayOfTimes(@Nonnull String fieldName) {
        return getArrayOfVariableSizes(fieldName, ARRAY_OF_TIMES, LocalTime[]::new, IOUtil::readLocalTime);
    }

    @Override
    @Nullable
    public LocalDate[] getArrayOfDates(@Nonnull String fieldName) {
        return getArrayOfVariableSizes(fieldName, ARRAY_OF_DATES, LocalDate[]::new, IOUtil::readLocalDate);
    }

    @Override
    @Nullable
    public LocalDateTime[] getArrayOfTimestamps(@Nonnull String fieldName) {
        return getArrayOfVariableSizes(fieldName, ARRAY_OF_TIMESTAMPS, LocalDateTime[]::new, IOUtil::readLocalDateTime);
    }

    @Override
    @Nullable
    public OffsetDateTime[] getArrayOfTimestampWithTimezones(@Nonnull String fieldName) {
        return getArrayOfVariableSizes(fieldName, ARRAY_OF_TIMESTAMP_WITH_TIMEZONES,
                OffsetDateTime[]::new, IOUtil::readOffsetDateTime);
    }

    @Override
    @Nullable
    public GenericRecord[] getArrayOfGenericRecords(@Nonnull String fieldName) {
        return getArrayOfVariableSizes(fieldName, ARRAY_OF_COMPACTS, GenericRecord[]::new,
                in -> serializer.readGenericRecord(in, schemaIncludedInBinary));
    }

    private <T> T getArrayOfPrimitives(@Nonnull String fieldName, Reader<T> reader, FieldKind primitiveKind,
                                       FieldKind nullableKind, String methodSuffix) {
        FieldDescriptor fd = getFieldDefinition(fieldName);
        FieldKind fieldKind = fd.getKind();
        if (fieldKind == primitiveKind) {
            return getVariableSize(fd, reader);
        } else if (fieldKind == nullableKind) {
            return getNullableArrayAsPrimitiveArray(fd, reader, methodSuffix);
        }
        throw unexpectedFieldKind(fieldKind, fieldName);
    }

    private <T> T getNullableArrayAsPrimitiveArray(FieldDescriptor fd, Reader<T> reader, String methodSuffix) {
        int currentPos = in.position();
        try {
            int position = readVariableSizeFieldPosition(fd);
            if (position == NULL_ARRAY_LENGTH) {
                return null;
            }
            in.position(position);
            int dataLength = in.readInt();
            int itemCount = in.readInt();
            int dataStartPosition = in.position();

            OffsetReader offsetReader = getOffsetReader(dataLength);
            int offsetsPosition = dataStartPosition + dataLength;
            for (int i = 0; i < itemCount; i++) {
                int offset = offsetReader.getOffset(in, offsetsPosition, i);
                if (offset == NULL_ARRAY_LENGTH) {
                    throw exceptionForUnexpectedNullValueInArray(fd.getFieldName(), methodSuffix);
                }
            }
            in.position(dataStartPosition - INT_SIZE_IN_BYTES);
            return reader.read(in);
        } catch (IOException e) {
            throw illegalStateException(e);
        } finally {
            in.position(currentPos);
        }
    }

    @Nullable
    @Override
    public Boolean getNullableBoolean(@Nonnull String fieldName) {
        FieldDescriptor fd = getFieldDefinition(fieldName);
        FieldKind fieldKind = fd.getKind();
        switch (fieldKind) {
            case BOOLEAN:
                return getBoolean(fd);
            case NULLABLE_BOOLEAN:
                return getVariableSize(fd, ObjectDataInput::readBoolean);
            default:
                throw unexpectedFieldKind(fieldKind, fieldName);
        }
    }

    @Nullable
    @Override
    public Byte getNullableByte(@Nonnull String fieldName) {
        FieldDescriptor fd = getFieldDefinition(fieldName);
        FieldKind fieldKind = fd.getKind();
        switch (fieldKind) {
            case BYTE:
                try {
                    return in.readByte(readFixedSizePosition(fd));
                } catch (IOException e) {
                    throw illegalStateException(e);
                }
            case NULLABLE_BYTE:
                return getVariableSize(fd, ObjectDataInput::readByte);
            default:
                throw unexpectedFieldKind(fieldKind, fieldName);
        }
    }

    @Nullable
    @Override
    public Short getNullableShort(@Nonnull String fieldName) {
        FieldDescriptor fd = getFieldDefinition(fieldName);
        FieldKind fieldKind = fd.getKind();
        switch (fieldKind) {
            case SHORT:
                try {
                    return in.readShort(readFixedSizePosition(fd));
                } catch (IOException e) {
                    throw illegalStateException(e);
                }
            case NULLABLE_SHORT:
                return getVariableSize(fd, ObjectDataInput::readShort);
            default:
                throw unexpectedFieldKind(fieldKind, fieldName);
        }
    }

    @Nullable
    @Override
    public Integer getNullableInt(@Nonnull String fieldName) {
        FieldDescriptor fd = getFieldDefinition(fieldName);
        FieldKind fieldKind = fd.getKind();
        switch (fieldKind) {
            case INT:
                try {
                    return in.readInt(readFixedSizePosition(fd));
                } catch (IOException e) {
                    throw illegalStateException(e);
                }
            case NULLABLE_INT:
                return getVariableSize(fd, ObjectDataInput::readInt);
            default:
                throw unexpectedFieldKind(fieldKind, fieldName);
        }
    }

    @Nullable
    @Override
    public Long getNullableLong(@Nonnull String fieldName) {
        FieldDescriptor fd = getFieldDefinition(fieldName);
        FieldKind fieldKind = fd.getKind();
        switch (fieldKind) {
            case LONG:
                try {
                    return in.readLong(readFixedSizePosition(fd));
                } catch (IOException e) {
                    throw illegalStateException(e);
                }
            case NULLABLE_LONG:
                return getVariableSize(fd, ObjectDataInput::readLong);
            default:
                throw unexpectedFieldKind(fieldKind, fieldName);
        }
    }

    @Nullable
    @Override
    public Float getNullableFloat(@Nonnull String fieldName) {
        FieldDescriptor fd = getFieldDefinition(fieldName);
        FieldKind fieldKind = fd.getKind();
        switch (fieldKind) {
            case FLOAT:
                try {
                    return in.readFloat(readFixedSizePosition(fd));
                } catch (IOException e) {
                    throw illegalStateException(e);
                }
            case NULLABLE_FLOAT:
                return getVariableSize(fd, ObjectDataInput::readFloat);
            default:
                throw unexpectedFieldKind(fieldKind, fieldName);
        }
    }

    @Nullable
    @Override
    public Double getNullableDouble(@Nonnull String fieldName) {
        FieldDescriptor fd = getFieldDefinition(fieldName);
        FieldKind fieldKind = fd.getKind();
        switch (fieldKind) {
            case DOUBLE:
                try {
                    return in.readDouble(readFixedSizePosition(fd));
                } catch (IOException e) {
                    throw illegalStateException(e);
                }
            case NULLABLE_DOUBLE:
                return getVariableSize(fd, ObjectDataInput::readDouble);
            default:
                throw unexpectedFieldKind(fieldKind, fieldName);
        }
    }

    @Nullable
    @Override
    public Boolean[] getArrayOfNullableBooleans(@Nonnull String fieldName) {
        FieldDescriptor fd = getFieldDefinition(fieldName);
        FieldKind fieldKind = fd.getKind();
        switch (fieldKind) {
            case ARRAY_OF_BOOLEANS:
                return getVariableSize(fieldName, ARRAY_OF_BOOLEANS, CompactInternalGenericRecord::readBooleanBitsAsNullables);
            case ARRAY_OF_NULLABLE_BOOLEANS:
                return getArrayOfVariableSizes(fieldName, ARRAY_OF_NULLABLE_BOOLEANS,
                        Boolean[]::new, ObjectDataInput::readBoolean);
            default:
                throw unexpectedFieldKind(fieldKind, fieldName);
        }
    }

    @Nullable
    @Override
    public Byte[] getArrayOfNullableBytes(@Nonnull String fieldName) {
        return getArrayOfNullables(fieldName, ObjectDataInput::readByte, Byte[]::new, ARRAY_OF_BYTES, ARRAY_OF_NULLABLE_BYTES);
    }

    @Nullable
    @Override
    public Short[] getArrayOfNullableShorts(@Nonnull String fieldName) {
        return getArrayOfNullables(fieldName, ObjectDataInput::readShort, Short[]::new, ARRAY_OF_SHORTS,
                ARRAY_OF_NULLABLE_SHORTS);
    }

    @Nullable
    @Override
    public Integer[] getArrayOfNullableInts(@Nonnull String fieldName) {
        return getArrayOfNullables(fieldName, ObjectDataInput::readInt, Integer[]::new, ARRAY_OF_INTS, ARRAY_OF_NULLABLE_INTS);
    }

    @Nullable
    @Override
    public Long[] getArrayOfNullableLongs(@Nonnull String fieldName) {
        return getArrayOfNullables(fieldName, ObjectDataInput::readLong, Long[]::new, ARRAY_OF_LONGS, ARRAY_OF_NULLABLE_LONGS);
    }

    @Nullable
    @Override
    public Float[] getArrayOfNullableFloats(@Nonnull String fieldName) {
        return getArrayOfNullables(fieldName, ObjectDataInput::readFloat, Float[]::new, ARRAY_OF_FLOATS,
                ARRAY_OF_NULLABLE_FLOATS);
    }

    @Nullable
    @Override
    public Double[] getArrayOfNullableDoubles(@Nonnull String fieldName) {
        return getArrayOfNullables(fieldName, ObjectDataInput::readDouble, Double[]::new, ARRAY_OF_DOUBLES,
                ARRAY_OF_NULLABLE_DOUBLES);
    }

    private <T> T[] getArrayOfNullables(@Nonnull String fieldName, Reader<T> reader,
                                        Function<Integer, T[]> constructor, FieldKind primitiveKind,
                                        FieldKind nullableKind) {
        FieldDescriptor fd = getFieldDefinition(fieldName);
        FieldKind fieldKind = fd.getKind();
        if (fieldKind == primitiveKind) {
            return getPrimitiveArrayAsNullableArray(fd, constructor, reader);
        } else if (fieldKind == nullableKind) {
            return getArrayOfVariableSizes(fd, constructor, reader);
        }
        throw unexpectedFieldKind(fieldKind, fieldName);
    }

    @Override
    public <T> T[] getArrayOfObjects(@Nonnull String fieldName, Class<T> componentType) {
        return (T[]) getArrayOfVariableSizes(fieldName, ARRAY_OF_COMPACTS,
                length -> (T[]) Array.newInstance(componentType, length),
                in -> serializer.read(in, schemaIncludedInBinary));
    }

    protected interface Reader<R> {
        R read(BufferObjectDataInput t) throws IOException;
    }

    private <T> T[] getPrimitiveArrayAsNullableArray(FieldDescriptor fieldDescriptor,
                                                     Function<Integer, T[]> constructor,
                                                     Reader<T> reader) {
        int currentPos = in.position();
        try {
            int pos = readVariableSizeFieldPosition(fieldDescriptor);
            if (pos == NULL_OFFSET) {
                return null;
            }
            in.position(pos);
            int itemCount = in.readInt();
            T[] values = constructor.apply(itemCount);

            for (int i = 0; i < itemCount; i++) {
                values[i] = reader.read(in);
            }
            return values;
        } catch (IOException e) {
            throw illegalStateException(e);
        } finally {
            in.position(currentPos);
        }
    }

    private <T> T[] getArrayOfVariableSizes(FieldDescriptor fieldDescriptor,
                                            Function<Integer, T[]> constructor,
                                            Reader<T> reader) {
        int currentPos = in.position();
        try {
            int position = readVariableSizeFieldPosition(fieldDescriptor);
            if (position == NULL_ARRAY_LENGTH) {
                return null;
            }
            in.position(position);
            int dataLength = in.readInt();
            int itemCount = in.readInt();
            int dataStartPosition = in.position();
            T[] values = constructor.apply(itemCount);

            OffsetReader offsetReader = getOffsetReader(dataLength);
            int offsetsPosition = dataStartPosition + dataLength;
            for (int i = 0; i < itemCount; i++) {
                int offset = offsetReader.getOffset(in, offsetsPosition, i);
                if (offset != NULL_ARRAY_LENGTH) {
                    in.position(offset + dataStartPosition);
                    values[i] = reader.read(in);
                }
            }
            return values;
        } catch (IOException e) {
            throw illegalStateException(e);
        } finally {
            in.position(currentPos);
        }
    }


    private <T> T[] getArrayOfVariableSizes(@Nonnull String fieldName, FieldKind fieldKind,
                                            Function<Integer, T[]> constructor,
                                            Reader<T> reader) {
        FieldDescriptor fieldDefinition = getFieldDefinition(fieldName, fieldKind);
        return getArrayOfVariableSizes(fieldDefinition, constructor, reader);
    }

    private static OffsetReader getOffsetReader(int dataLength) {
        if (dataLength < BYTE_OFFSET_READER_RANGE) {
            return BYTE_OFFSET_READER;
        } else if (dataLength < SHORT_OFFSET_READER_RANGE) {
            return SHORT_OFFSET_READER;
        } else {
            return INT_OFFSET_READER;
        }
    }

    private int readFixedSizePosition(FieldDescriptor fd) {
        int primitiveOffset = fd.getOffset();
        return primitiveOffset + dataStartPosition;
    }

    @Nonnull
    private FieldDescriptor getFieldDefinition(@Nonnull String fieldName) {
        FieldDescriptor fd = schema.getField(fieldName);
        if (fd == null) {
            throw throwUnknownFieldException(fieldName);
        }
        return fd;
    }

    @Nonnull
    private FieldDescriptor getFieldDefinition(@Nonnull String fieldName, FieldKind fieldKind) {
        FieldDescriptor fd = getFieldDefinition(fieldName);
        if (fd.getKind() != fieldKind) {
            throw unexpectedFieldKind(fd.getKind(), fieldName);
        }
        return fd;
    }

    private int readVariableSizeFieldPosition(@Nonnull String fieldName, FieldKind fieldKind) {
        try {
            FieldDescriptor fd = getFieldDefinition(fieldName, fieldKind);
            int index = fd.getIndex();
            int offset = offsetReader.getOffset(in, variableOffsetsPosition, index);
            return offset == NULL_OFFSET ? NULL_OFFSET : offset + dataStartPosition;
        } catch (IOException e) {
            throw illegalStateException(e);
        }
    }

    private int readVariableSizeFieldPosition(FieldDescriptor fd) {
        try {
            int index = fd.getIndex();
            int offset = offsetReader.getOffset(in, variableOffsetsPosition, index);
            return offset == NULL_OFFSET ? NULL_OFFSET : offset + dataStartPosition;
        } catch (IOException e) {
            throw illegalStateException(e);
        }
    }

    private HazelcastSerializationException throwUnknownFieldException(@Nonnull String fieldName) {
        return new HazelcastSerializationException("Unknown field name: '" + fieldName
                + "' for " + schema);
    }

    //indexed methods//

    private int readLength(int beginPosition) {
        try {
            return in.readInt(beginPosition);
        } catch (IOException e) {
            throw illegalStateException(e);
        }
    }

    public Byte getByteFromArray(@Nonnull String fieldName, int index) {
        return getFixedSizeFieldFromArray(fieldName, ARRAY_OF_BYTES, ObjectDataInput::readByte, index);
    }

    public Boolean getBooleanFromArray(@Nonnull String fieldName, int index) {
        int position = readVariableSizeFieldPosition(fieldName, ARRAY_OF_BOOLEANS);
        if (position == NULL_OFFSET) {
            return null;
        }
        if (readLength(position) <= index) {
            return null;
        }
        int currentPos = in.position();
        try {
            int booleanOffsetInBytes = index / Byte.SIZE;
            int booleanOffsetWithinLastByte = index % Byte.SIZE;
            byte b = in.readByte(INT_SIZE_IN_BYTES + position + booleanOffsetInBytes);
            return ((b >>> booleanOffsetWithinLastByte) & 1) != 0;
        } catch (IOException e) {
            throw illegalStateException(e);
        } finally {
            in.position(currentPos);
        }
    }

    public Character getCharFromArray(@Nonnull String fieldName, int index) {
        throw new UnsupportedOperationException("Compact format does not support reading from an array of chars field");
    }

    public Integer getIntFromArray(@Nonnull String fieldName, int index) {
        return getFixedSizeFieldFromArray(fieldName, ARRAY_OF_INTS, ObjectDataInput::readInt, index);
    }

    public Long getLongFromArray(@Nonnull String fieldName, int index) {
        return getFixedSizeFieldFromArray(fieldName, ARRAY_OF_LONGS, ObjectDataInput::readLong, index);
    }

    public Double getDoubleFromArray(@Nonnull String fieldName, int index) {
        return getFixedSizeFieldFromArray(fieldName, ARRAY_OF_DOUBLES, ObjectDataInput::readDouble, index);
    }

    public Float getFloatFromArray(@Nonnull String fieldName, int index) {
        return getFixedSizeFieldFromArray(fieldName, ARRAY_OF_FLOATS, ObjectDataInput::readFloat, index);
    }

    public Short getShortFromArray(@Nonnull String fieldName, int index) {
        return getFixedSizeFieldFromArray(fieldName, ARRAY_OF_SHORTS, ObjectDataInput::readShort, index);
    }

    private <T> T getFixedSizeFieldFromArray(@Nonnull String fieldName, FieldKind fieldKind,
                                             Reader<T> reader, int index) {
        checkNotNegative(index, "Array indexes can not be negative");

        int position = readVariableSizeFieldPosition(fieldName, fieldKind);
        if (position == NULL_OFFSET) {
            return null;
        }
        if (readLength(position) <= index) {
            return null;
        }
        int currentPos = in.position();
        try {
            FieldKind singleKind = FieldOperations.getSingleKind(fieldKind);
            int kindSize = FieldOperations.fieldOperations(singleKind).kindSizeInBytes();
            in.position(INT_SIZE_IN_BYTES + position + index * kindSize);
            return reader.read(in);
        } catch (IOException e) {
            throw illegalStateException(e);
        } finally {
            in.position(currentPos);
        }
    }

    @Override
    public String getStringFromArray(@Nonnull String fieldName, int index) {
        return getVariableSizeFromArray(fieldName, ARRAY_OF_STRINGS, BufferObjectDataInput::readString, index);
    }

    @Override
    public GenericRecord getGenericRecordFromArray(@Nonnull String fieldName, int index) {
        return getVariableSizeFromArray(fieldName, ARRAY_OF_COMPACTS,
                in -> serializer.readGenericRecord(in, schemaIncludedInBinary), index);
    }

    @Override
    public BigDecimal getDecimalFromArray(@Nonnull String fieldName, int index) {
        return getVariableSizeFromArray(fieldName, ARRAY_OF_DECIMALS, IOUtil::readBigDecimal, index);
    }

    @Nullable
    @Override
    public LocalTime getTimeFromArray(@Nonnull String fieldName, int index) {
        return getVariableSizeFromArray(fieldName, ARRAY_OF_TIMES, IOUtil::readLocalTime, index);
    }

    @Nullable
    @Override
    public LocalDate getDateFromArray(@Nonnull String fieldName, int index) {
        return getVariableSizeFromArray(fieldName, ARRAY_OF_DATES, IOUtil::readLocalDate, index);
    }

    @Nullable
    @Override
    public LocalDateTime getTimestampFromArray(@Nonnull String fieldName, int index) {
        return getVariableSizeFromArray(fieldName, ARRAY_OF_TIMESTAMPS, IOUtil::readLocalDateTime, index);
    }

    @Nullable
    @Override
    public OffsetDateTime getTimestampWithTimezoneFromArray(@Nonnull String fieldName, int index) {
        return getVariableSizeFromArray(fieldName, ARRAY_OF_TIMESTAMP_WITH_TIMEZONES, IOUtil::readOffsetDateTime, index);
    }

    @Nullable
    @Override
    public Byte getNullableByteFromArray(@Nonnull String fieldName, int index) {
        return getVariableSizeFromArray(fieldName, ARRAY_OF_NULLABLE_BYTES, ObjectDataInput::readByte, index);
    }

    @Nullable
    @Override
    public Boolean getNullableBooleanFromArray(@Nonnull String fieldName, int index) {
        return getVariableSizeFromArray(fieldName, ARRAY_OF_NULLABLE_BOOLEANS, ObjectDataInput::readBoolean, index);
    }

    @Nullable
    @Override
    public Integer getNullableIntFromArray(@Nonnull String fieldName, int index) {
        return getVariableSizeFromArray(fieldName, ARRAY_OF_NULLABLE_INTS, ObjectDataInput::readInt, index);
    }

    @Nullable
    @Override
    public Long getNullableLongFromArray(@Nonnull String fieldName, int index) {
        return getVariableSizeFromArray(fieldName, ARRAY_OF_NULLABLE_LONGS, ObjectDataInput::readLong, index);
    }

    @Nullable
    @Override
    public Float getNullableFloatFromArray(@Nonnull String fieldName, int index) {
        return getVariableSizeFromArray(fieldName, ARRAY_OF_NULLABLE_FLOATS, ObjectDataInput::readFloat, index);
    }

    @Nullable
    @Override
    public Double getNullableDoubleFromArray(@Nonnull String fieldName, int index) {
        return getVariableSizeFromArray(fieldName, ARRAY_OF_NULLABLE_DOUBLES, ObjectDataInput::readDouble, index);
    }

    @Nullable
    @Override
    public Short getNullableShortFromArray(@Nonnull String fieldName, int index) {
        return getVariableSizeFromArray(fieldName, ARRAY_OF_NULLABLE_SHORTS, ObjectDataInput::readShort, index);
    }

    @Override
    @Nullable
    public <T> T getObjectFromArray(@Nonnull String fieldName, int index) {
        return (T) getVariableSizeFromArray(fieldName, ARRAY_OF_COMPACTS,
                in -> serializer.read(in, schemaIncludedInBinary), index);
    }

    private <T> T getVariableSizeFromArray(@Nonnull String fieldName, FieldKind fieldKind,
                                           Reader<T> reader, int index) {
        int currentPos = in.position();
        try {
            int pos = readVariableSizeFieldPosition(fieldName, fieldKind);

            if (pos == NULL_OFFSET) {
                return null;
            }
            int dataLength = in.readInt(pos);
            int itemCount = in.readInt(pos + INT_SIZE_IN_BYTES);
            checkNotNegative(index, "Array index can not be negative");
            if (itemCount <= index) {
                return null;
            }
            int dataStartPosition = pos + (2 * INT_SIZE_IN_BYTES);
            OffsetReader offsetReader = getOffsetReader(dataLength);
            int offsetsPosition = dataStartPosition + dataLength;
            int indexedItemOffset = offsetReader.getOffset(in, offsetsPosition, index);
            if (indexedItemOffset != NULL_OFFSET) {
                in.position(indexedItemOffset + dataStartPosition);
                return reader.read(in);
            }
            return null;
        } catch (IOException e) {
            throw illegalStateException(e);
        } finally {
            in.position(currentPos);
        }
    }

    @Override
    protected Object getClassIdentifier() {
        return schema.getTypeName();
    }

    protected IllegalStateException illegalStateException(IOException e) {
        return new IllegalStateException("IOException is not expected since we get from a well known format and position", e);
    }

    private HazelcastSerializationException unexpectedFieldKind(FieldKind actualFieldKind,
                                                                String fieldName) {
        throw new HazelcastSerializationException("Unexpected fieldKind '" + actualFieldKind + "' for field: " + fieldName);
    }


    private static boolean[] readBooleanBits(BufferObjectDataInput input) throws IOException {
        int len = input.readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len == 0) {
            return new boolean[0];
        }
        boolean[] values = new boolean[len];
        int index = 0;
        byte currentByte = input.readByte();
        for (int i = 0; i < len; i++) {
            if (index == Byte.SIZE) {
                index = 0;
                currentByte = input.readByte();
            }
            boolean result = ((currentByte >>> index) & 1) != 0;
            index++;
            values[i] = result;
        }
        return values;
    }

    private static Boolean[] readBooleanBitsAsNullables(BufferObjectDataInput input) throws IOException {
        int len = input.readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len == 0) {
            return new Boolean[0];
        }
        Boolean[] values = new Boolean[len];
        int index = 0;
        byte currentByte = input.readByte();
        for (int i = 0; i < len; i++) {
            if (index == Byte.SIZE) {
                index = 0;
                currentByte = input.readByte();
            }
            boolean result = ((currentByte >>> index) & 1) != 0;
            index++;
            values[i] = result;
        }
        return values;
    }

    boolean isFieldExists(@Nonnull String fieldName, @Nonnull FieldKind kind) {
        FieldDescriptor field = schema.getField(fieldName);
        if (field == null) {
            return false;
        }
        return field.getKind() == kind;
    }
}
