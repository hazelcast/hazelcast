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

package com.hazelcast.internal.serialization.impl.portable;

import com.hazelcast.core.ManagedContext;
import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.nio.BufferObjectDataOutput;
import com.hazelcast.internal.serialization.impl.InternalGenericRecord;
import com.hazelcast.internal.serialization.impl.SerializationConstants;
import com.hazelcast.internal.serialization.impl.SerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.GenericRecord;
import com.hazelcast.nio.serialization.GenericRecordBuilder;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public final class PortableSerializer implements StreamSerializer<Object> {

    private final PortableContextImpl context;
    private final Map<Integer, PortableFactory> factories = new HashMap<>();

    public PortableSerializer(PortableContextImpl context, Map<Integer, ? extends PortableFactory> portableFactories) {
        this.context = context;
        factories.putAll(portableFactories);
    }

    @Override
    public int getTypeId() {
        return SerializationConstants.CONSTANT_TYPE_PORTABLE;
    }

    @Override
    public void write(ObjectDataOutput out, Object o) throws IOException {
        if (o instanceof Portable) {
            Portable p = (Portable) o;
            if (!(out instanceof BufferObjectDataOutput)) {
                throw new IllegalArgumentException("ObjectDataOutput must be instance of BufferObjectDataOutput!");
            }
            if (p.getClassId() == 0) {
                throw new IllegalArgumentException("Portable class ID cannot be zero!");
            }

            out.writeInt(p.getFactoryId());
            out.writeInt(p.getClassId());
            writeInternal((BufferObjectDataOutput) out, p);
            return;
        }
        if (o instanceof PortableGenericRecord) {
            writePortableGenericRecord(out, (PortableGenericRecord) o);
            return;
        }
        throw new IllegalArgumentException("PortableSerializer can only write Portable and PortableGenericRecord");
    }


    void writeInternal(BufferObjectDataOutput out, Portable p) throws IOException {
        ClassDefinition cd = context.lookupOrRegisterClassDefinition(p);
        out.writeInt(cd.getVersion());

        DefaultPortableWriter writer = new DefaultPortableWriter(this, out, cd);
        p.writePortable(writer);
        writer.end();
    }

    @Override
    public Object read(ObjectDataInput in) throws IOException {
        if (!(in instanceof BufferObjectDataInput)) {
            throw new IllegalArgumentException("ObjectDataInput must be instance of BufferObjectDataInput!");
        }

        int factoryId = in.readInt();
        int classId = in.readInt();

        BufferObjectDataInput input = (BufferObjectDataInput) in;
        return read(input, factoryId, classId);
    }

    private int findPortableVersion(int factoryId, int classId, Portable portable) {
        int currentVersion = context.getClassVersion(factoryId, classId);
        if (currentVersion < 0) {
            currentVersion = SerializationUtil.getPortableVersion(portable, context.getVersion());
            if (currentVersion > 0) {
                context.setClassVersion(factoryId, classId, currentVersion);
            }
        }
        return currentVersion;
    }

    private Portable createNewPortableInstance(int factoryId, int classId) {
        final PortableFactory portableFactory = factories.get(factoryId);
        if (portableFactory == null) {
            return null;
        }
        return portableFactory.create(classId);
    }

    public InternalGenericRecord readAsInternalGenericRecord(ObjectDataInput in) throws IOException {
        int factoryId = in.readInt();
        int classId = in.readInt();
        int version = in.readInt();

        BufferObjectDataInput input = (BufferObjectDataInput) in;
        ClassDefinition cd = setupPositionAndDefinition(input, factoryId, classId, version);
        return new PortableInternalGenericRecord(this, input, cd, true);
    }

    DefaultPortableReader createMorphingReader(BufferObjectDataInput in) throws IOException {
        int factoryId = in.readInt();
        int classId = in.readInt();
        int version = in.readInt();

        Portable portable = createNewPortableInstance(factoryId, classId);
        int portableVersion = findPortableVersion(factoryId, classId, portable);

        return createReader(in, factoryId, classId, version, portableVersion);
    }

    public ClassDefinition setupPositionAndDefinition(BufferObjectDataInput in, int factoryId, int classId, int version)
            throws IOException {

        int effectiveVersion = version;
        if (effectiveVersion < 0) {
            effectiveVersion = context.getVersion();
        }

        ClassDefinition cd = context.lookupClassDefinition(factoryId, classId, effectiveVersion);
        if (cd == null) {
            int begin = in.position();
            cd = context.readClassDefinition(in, factoryId, classId, effectiveVersion);
            in.position(begin);
        }

        return cd;
    }

    public DefaultPortableReader createReader(BufferObjectDataInput in, int factoryId, int classId, int version,
                                              int portableVersion) throws IOException {

        ClassDefinition cd = setupPositionAndDefinition(in, factoryId, classId, version);
        DefaultPortableReader reader;
        if (portableVersion == cd.getVersion()) {
            reader = new DefaultPortableReader(this, in, cd);
        } else {
            reader = new MorphingPortableReader(this, in, cd);
        }
        return reader;
    }

    @Override
    public void destroy() {
        factories.clear();
    }

    //Portable Generic Record

    private void writePortableGenericRecord(ObjectDataOutput out, PortableGenericRecord record) throws IOException {
        ClassDefinition cd = record.getClassDefinition();
        out.writeInt(cd.getFactoryId());
        out.writeInt(cd.getClassId());
        writePortableGenericRecordInternal(out, record);
    }

    @SuppressWarnings({"checkstyle:MethodLength", "checkstyle:CyclomaticComplexity"})
    void writePortableGenericRecordInternal(ObjectDataOutput out, PortableGenericRecord record) throws IOException {
        ClassDefinition cd = record.getClassDefinition();
        // Class definition compatibility will be checked implicitly on the
        // register call below.
        context.registerClassDefinition(cd, context.shouldCheckClassDefinitionErrors());
        out.writeInt(cd.getVersion());

        BufferObjectDataOutput output = (BufferObjectDataOutput) out;
        DefaultPortableWriter writer = new DefaultPortableWriter(this, output, cd);
        Set<String> fieldNames = cd.getFieldNames();
        for (String fieldName : fieldNames) {
            switch (cd.getFieldType(fieldName)) {
                case PORTABLE:
                    writer.writeGenericRecord(fieldName, record.getGenericRecord(fieldName));
                    break;
                case BYTE:
                    writer.writeByte(fieldName, record.getByte(fieldName));
                    break;
                case BOOLEAN:
                    writer.writeBoolean(fieldName, record.getBoolean(fieldName));
                    break;
                case CHAR:
                    writer.writeChar(fieldName, record.getChar(fieldName));
                    break;
                case SHORT:
                    writer.writeShort(fieldName, record.getShort(fieldName));
                    break;
                case INT:
                    writer.writeInt(fieldName, record.getInt(fieldName));
                    break;
                case LONG:
                    writer.writeLong(fieldName, record.getLong(fieldName));
                    break;
                case FLOAT:
                    writer.writeFloat(fieldName, record.getFloat(fieldName));
                    break;
                case DOUBLE:
                    writer.writeDouble(fieldName, record.getDouble(fieldName));
                    break;
                case UTF:
                    writer.writeString(fieldName, record.getString(fieldName));
                    break;
                case DECIMAL:
                    writer.writeDecimal(fieldName, record.getDecimal(fieldName));
                    break;
                case TIME:
                    writer.writeTime(fieldName, record.getTime(fieldName));
                    break;
                case DATE:
                    writer.writeDate(fieldName, record.getDate(fieldName));
                    break;
                case TIMESTAMP:
                    writer.writeTimestamp(fieldName, record.getTimestamp(fieldName));
                    break;
                case TIMESTAMP_WITH_TIMEZONE:
                    writer.writeTimestampWithTimezone(fieldName, record.getTimestampWithTimezone(fieldName));
                    break;
                case PORTABLE_ARRAY:
                    writer.writeGenericRecordArray(fieldName, record.getArrayOfGenericRecords(fieldName));
                    break;
                case BYTE_ARRAY:
                    writer.writeByteArray(fieldName, record.getArrayOfBytes(fieldName));
                    break;
                case BOOLEAN_ARRAY:
                    writer.writeBooleanArray(fieldName, record.getArrayOfBooleans(fieldName));
                    break;
                case CHAR_ARRAY:
                    writer.writeCharArray(fieldName, record.getArrayOfChars(fieldName));
                    break;
                case SHORT_ARRAY:
                    writer.writeShortArray(fieldName, record.getArrayOfShorts(fieldName));
                    break;
                case INT_ARRAY:
                    writer.writeIntArray(fieldName, record.getArrayOfInts(fieldName));
                    break;
                case LONG_ARRAY:
                    writer.writeLongArray(fieldName, record.getArrayOfLongs(fieldName));
                    break;
                case FLOAT_ARRAY:
                    writer.writeFloatArray(fieldName, record.getArrayOfFloats(fieldName));
                    break;
                case DOUBLE_ARRAY:
                    writer.writeDoubleArray(fieldName, record.getArrayOfDoubles(fieldName));
                    break;
                case UTF_ARRAY:
                    writer.writeStringArray(fieldName, record.getArrayOfStrings(fieldName));
                    break;
                case DECIMAL_ARRAY:
                    writer.writeDecimalArray(fieldName, record.getArrayOfDecimals(fieldName));
                    break;
                case TIME_ARRAY:
                    writer.writeTimeArray(fieldName, record.getArrayOfTimes(fieldName));
                    break;
                case DATE_ARRAY:
                    writer.writeDateArray(fieldName, record.getArrayOfDates(fieldName));
                    break;
                case TIMESTAMP_ARRAY:
                    writer.writeTimestampArray(fieldName, record.getArrayOfTimestamps(fieldName));
                    break;
                case TIMESTAMP_WITH_TIMEZONE_ARRAY:
                    writer.writeTimestampWithTimezoneArray(fieldName, record.getArrayOfTimestampWithTimezones(fieldName));
                    break;
                default:
                    throw new IllegalStateException("Unexpected field type: " + cd.getFieldType(fieldName));
            }
        }
        writer.end();
    }

    /**
     * Tries to construct the user's Portable object first via given factory config.
     * If it can not find the related factory, this will return GenericRecord representation of the object.
     */
    <T> T read(BufferObjectDataInput in, int factoryId, int classId) throws IOException {
        Portable portable = createNewPortableInstance(factoryId, classId);
        if (portable != null) {
            int writeVersion = in.readInt();
            int readVersion = findPortableVersion(factoryId, classId, portable);
            DefaultPortableReader reader = createReader(in, factoryId, classId, writeVersion, readVersion);
            portable.readPortable(reader);
            reader.end();

            final ManagedContext managedContext = context.getManagedContext();
            return managedContext != null ? (T) managedContext.initialize(portable) : (T) portable;
        }
        GenericRecord genericRecord = readPortableGenericRecord(in, factoryId, classId);
        assert genericRecord instanceof PortableGenericRecord;
        return (T) genericRecord;
    }

    <T> T readAsGenericRecord(BufferObjectDataInput in, int factoryId, int classId,
                              boolean readGenericLazy) throws IOException {
        if (readGenericLazy) {
            int version = in.readInt();
            ClassDefinition cd = setupPositionAndDefinition(in, factoryId, classId, version);
            PortableInternalGenericRecord reader = new PortableInternalGenericRecord(this, in, cd, true);
            return (T) reader;
        }
        return readPortableGenericRecord(in, factoryId, classId);
    }

    @SuppressWarnings({"checkstyle:MethodLength", "checkstyle:CyclomaticComplexity"})
    private <T> T readPortableGenericRecord(BufferObjectDataInput in, int factoryId, int classId) throws IOException {
        int version = in.readInt();
        ClassDefinition cd = setupPositionAndDefinition(in, factoryId, classId, version);
        PortableInternalGenericRecord reader = new PortableInternalGenericRecord(this, in, cd, false);
        GenericRecordBuilder genericRecordBuilder = GenericRecordBuilder.portable(cd);
        for (String fieldName : cd.getFieldNames()) {
            switch (cd.getFieldType(fieldName)) {
                case PORTABLE:
                    genericRecordBuilder.setGenericRecord(fieldName, reader.getGenericRecord(fieldName));
                    break;
                case BYTE:
                    genericRecordBuilder.setByte(fieldName, reader.getByte(fieldName));
                    break;
                case BOOLEAN:
                    genericRecordBuilder.setBoolean(fieldName, reader.getBoolean(fieldName));
                    break;
                case CHAR:
                    genericRecordBuilder.setChar(fieldName, reader.getChar(fieldName));
                    break;
                case SHORT:
                    genericRecordBuilder.setShort(fieldName, reader.getShort(fieldName));
                    break;
                case INT:
                    genericRecordBuilder.setInt(fieldName, reader.getInt(fieldName));
                    break;
                case LONG:
                    genericRecordBuilder.setLong(fieldName, reader.getLong(fieldName));
                    break;
                case FLOAT:
                    genericRecordBuilder.setFloat(fieldName, reader.getFloat(fieldName));
                    break;
                case DOUBLE:
                    genericRecordBuilder.setDouble(fieldName, reader.getDouble(fieldName));
                    break;
                case UTF:
                    genericRecordBuilder.setString(fieldName, reader.getString(fieldName));
                    break;
                case DECIMAL:
                    genericRecordBuilder.setDecimal(fieldName, reader.getDecimal(fieldName));
                    break;
                case TIME:
                    genericRecordBuilder.setTime(fieldName, reader.getTime(fieldName));
                    break;
                case DATE:
                    genericRecordBuilder.setDate(fieldName, reader.getDate(fieldName));
                    break;
                case TIMESTAMP:
                    genericRecordBuilder.setTimestamp(fieldName, reader.getTimestamp(fieldName));
                    break;
                case TIMESTAMP_WITH_TIMEZONE:
                    genericRecordBuilder.setTimestampWithTimezone(fieldName, reader.getTimestampWithTimezone(fieldName));
                    break;
                case PORTABLE_ARRAY:
                    genericRecordBuilder.setArrayOfGenericRecords(fieldName, reader.getArrayOfGenericRecords(fieldName));
                    break;
                case BYTE_ARRAY:
                    genericRecordBuilder.setArrayOfBytes(fieldName, reader.getArrayOfBytes(fieldName));
                    break;
                case BOOLEAN_ARRAY:
                    genericRecordBuilder.setArrayOfBooleans(fieldName, reader.getArrayOfBooleans(fieldName));
                    break;
                case CHAR_ARRAY:
                    genericRecordBuilder.setArrayOfChars(fieldName, reader.getArrayOfChars(fieldName));
                    break;
                case SHORT_ARRAY:
                    genericRecordBuilder.setArrayOfShorts(fieldName, reader.getArrayOfShorts(fieldName));
                    break;
                case INT_ARRAY:
                    genericRecordBuilder.setArrayOfInts(fieldName, reader.getArrayOfInts(fieldName));
                    break;
                case LONG_ARRAY:
                    genericRecordBuilder.setArrayOfLongs(fieldName, reader.getArrayOfLongs(fieldName));
                    break;
                case FLOAT_ARRAY:
                    genericRecordBuilder.setArrayOfFloats(fieldName, reader.getArrayOfFloats(fieldName));
                    break;
                case DOUBLE_ARRAY:
                    genericRecordBuilder.setArrayOfDoubles(fieldName, reader.getArrayOfDoubles(fieldName));
                    break;
                case UTF_ARRAY:
                    genericRecordBuilder.setArrayOfStrings(fieldName, reader.getArrayOfStrings(fieldName));
                    break;
                case DECIMAL_ARRAY:
                    genericRecordBuilder.setArrayOfDecimals(fieldName, reader.getArrayOfDecimals(fieldName));
                    break;
                case TIME_ARRAY:
                    genericRecordBuilder.setArrayOfTimes(fieldName, reader.getArrayOfTimes(fieldName));
                    break;
                case DATE_ARRAY:
                    genericRecordBuilder.setArrayOfDates(fieldName, reader.getArrayOfDates(fieldName));
                    break;
                case TIMESTAMP_ARRAY:
                    genericRecordBuilder.setArrayOfTimestamps(fieldName, reader.getArrayOfTimestamps(fieldName));
                    break;
                case TIMESTAMP_WITH_TIMEZONE_ARRAY:
                    genericRecordBuilder.setArrayOfTimestampWithTimezones(fieldName,
                            reader.getArrayOfTimestampWithTimezones(fieldName));
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + cd.getFieldType(fieldName));
            }
        }
        reader.end();
        return (T) genericRecordBuilder.build();
    }
}
