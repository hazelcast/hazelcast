/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.serialization;

import com.hazelcast.internal.serialization.impl.portable.ClassDefinitionImpl;
import com.hazelcast.internal.serialization.impl.portable.FieldDefinitionImpl;
import com.hazelcast.spi.annotation.PrivateApi;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * ClassDefinitionBuilder is used to build and register ClassDefinitions manually.
 *
 * @see ClassDefinition
 * @see com.hazelcast.nio.serialization.Portable
 * @see com.hazelcast.config.SerializationConfig#addClassDefinition(ClassDefinition)
 */
public final class ClassDefinitionBuilder {

    private final int factoryId;
    private final int classId;
    private final int version;
    private final List<FieldDefinitionImpl> fieldDefinitions = new ArrayList<FieldDefinitionImpl>();
    private final Set<String> addedFieldNames = new HashSet<>();
    private int index;
    private boolean done;

    /**
     * IMPORTANT: It uses a default portableVersion (0) for non-versioned classes.
     * Make sure to specify the portableVersion in the constructor if you override the default portableVersion
     * in the SerializationService
     *
     * @param factoryId factoryId to use
     * @param classId   classId to use
     */
    public ClassDefinitionBuilder(int factoryId, int classId) {
        this.factoryId = factoryId;
        this.classId = classId;
        this.version = 0;
    }

    /**
     * IMPORTANT: Make sure that the version matches the portableVersion in the SerializationService
     *
     * @param factoryId factoryId to use
     * @param classId   classId to use
     * @param version   portableVersion to use
     */
    public ClassDefinitionBuilder(int factoryId, int classId, int version) {
        this.factoryId = factoryId;
        this.classId = classId;
        this.version = version;
    }

    /**
     * @param fieldName name of the field that will be added to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     */
    @Nonnull
    public ClassDefinitionBuilder addIntField(@Nonnull String fieldName) {
        return addField(fieldName, FieldType.INT);
    }

    /**
     * @param fieldName name of the field that will be added to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     */
    @Nonnull
    public ClassDefinitionBuilder addLongField(@Nonnull String fieldName) {
        return addField(fieldName, FieldType.LONG);
    }

    /**
     * @param fieldName name of the field that will be added to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     * @deprecated for the sake of better naming. Use {@link #addStringField(String)} instead.
     */
    @Nonnull
    @Deprecated
    public ClassDefinitionBuilder addUTFField(@Nonnull String fieldName) {
        return addStringField(fieldName);
    }

    /**
     * @param fieldName name of the field that will be added to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     */
    @Nonnull
    public ClassDefinitionBuilder addStringField(@Nonnull String fieldName) {
        return addField(fieldName, FieldType.UTF);
    }

    /**
     * @param fieldName name of the field that will be added to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     */
    @Nonnull
    public ClassDefinitionBuilder addBooleanField(@Nonnull String fieldName) {
        return addField(fieldName, FieldType.BOOLEAN);
    }

    /**
     * @param fieldName name of the field that will be added to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     */
    @Nonnull
    public ClassDefinitionBuilder addByteField(@Nonnull String fieldName) {
        return addField(fieldName, FieldType.BYTE);
    }

    /**
     * @param fieldName name of the field that will be added to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     */
    @Nonnull
    public ClassDefinitionBuilder addBooleanArrayField(@Nonnull String fieldName) {
        return addField(fieldName, FieldType.BOOLEAN_ARRAY);
    }

    /**
     * @param fieldName name of the field that will be added to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     */
    @Nonnull
    public ClassDefinitionBuilder addCharField(@Nonnull String fieldName) {
        return addField(fieldName, FieldType.CHAR);
    }

    /**
     * @param fieldName name of the field that will be added to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     */
    @Nonnull
    public ClassDefinitionBuilder addDoubleField(@Nonnull String fieldName) {
        return addField(fieldName, FieldType.DOUBLE);
    }

    /**
     * @param fieldName name of the field that will be added to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     */
    @Nonnull
    public ClassDefinitionBuilder addFloatField(@Nonnull String fieldName) {
        return addField(fieldName, FieldType.FLOAT);
    }

    /**
     * @param fieldName name of the field that will be added to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     */
    @Nonnull
    public ClassDefinitionBuilder addShortField(@Nonnull String fieldName) {
        return addField(fieldName, FieldType.SHORT);
    }

    /**
     * Adds a decimal which is arbitrary precision and scale floating-point number to the class definition
     *
     * @param fieldName name of the field that will be added to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     */
    @Nonnull
    public ClassDefinitionBuilder addDecimalField(@Nonnull String fieldName) {
        return addField(fieldName, FieldType.DECIMAL);
    }

    /**
     * Adds a time field consisting of hour, minute, seconds and nanos parts to the class definition
     *
     * @param fieldName name of the field that will be added to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     */
    @Nonnull
    public ClassDefinitionBuilder addTimeField(@Nonnull String fieldName) {
        return addField(fieldName, FieldType.TIME);
    }

    /**
     * Adds a date field consisting of year, month of the year and day of the month to the class definition
     *
     * @param fieldName name of the field that will be added to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     */
    @Nonnull
    public ClassDefinitionBuilder addDateField(@Nonnull String fieldName) {
        return addField(fieldName, FieldType.DATE);
    }

    /**
     * Adds a timestamp field consisting of
     * year, month of the year, day of the month, hour, minute, seconds, nanos parts to the class definition
     *
     * @param fieldName name of the field that will be added to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     */
    @Nonnull
    public ClassDefinitionBuilder addTimestampField(@Nonnull String fieldName) {
        return addField(fieldName, FieldType.TIMESTAMP);
    }

    /**
     * Adds a timestamp with timezone field consisting of
     * year, month of the year, day of the month, offset seconds, hour, minute, seconds, nanos parts
     * to the class definition
     *
     * @param fieldName name of the field that will be added to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     */
    @Nonnull
    public ClassDefinitionBuilder addTimestampWithTimezoneField(@Nonnull String fieldName) {
        return addField(fieldName, FieldType.TIMESTAMP_WITH_TIMEZONE);
    }

    /**
     * @param fieldName name of the field that will be added to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     */
    @Nonnull
    public ClassDefinitionBuilder addByteArrayField(@Nonnull String fieldName) {
        return addField(fieldName, FieldType.BYTE_ARRAY);
    }

    /**
     * @param fieldName name of the field that will be added to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     */
    @Nonnull
    public ClassDefinitionBuilder addCharArrayField(@Nonnull String fieldName) {
        return addField(fieldName, FieldType.CHAR_ARRAY);
    }

    /**
     * @param fieldName name of the field that will be added to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     */
    @Nonnull
    public ClassDefinitionBuilder addIntArrayField(@Nonnull String fieldName) {
        return addField(fieldName, FieldType.INT_ARRAY);
    }

    /**
     * @param fieldName name of the field that will be added to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     */
    @Nonnull
    public ClassDefinitionBuilder addLongArrayField(@Nonnull String fieldName) {
        return addField(fieldName, FieldType.LONG_ARRAY);
    }

    /**
     * @param fieldName name of the field that will be added to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     */
    @Nonnull
    public ClassDefinitionBuilder addDoubleArrayField(@Nonnull String fieldName) {
        return addField(fieldName, FieldType.DOUBLE_ARRAY);
    }

    /**
     * @param fieldName name of the field that will be added to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     */
    @Nonnull
    public ClassDefinitionBuilder addFloatArrayField(@Nonnull String fieldName) {
        return addField(fieldName, FieldType.FLOAT_ARRAY);
    }

    /**
     * @param fieldName name of the field that will be added to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     */
    @Nonnull
    public ClassDefinitionBuilder addShortArrayField(@Nonnull String fieldName) {
        return addField(fieldName, FieldType.SHORT_ARRAY);
    }

    /**
     * @param fieldName name of the field that will be added to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     * @deprecated for the sake of better naming. Use {@link #addStringArrayField(String)} instead.
     */
    @Nonnull
    @Deprecated
    public ClassDefinitionBuilder addUTFArrayField(@Nonnull String fieldName) {
        return addStringArrayField(fieldName);
    }

    /**
     * @param fieldName name of the field that will be added to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     */
    @Nonnull
    public ClassDefinitionBuilder addStringArrayField(@Nonnull String fieldName) {
        return addField(fieldName, FieldType.UTF_ARRAY);
    }

    /**
     * @param fieldName name of the field that will be added to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     */
    @Nonnull
    public ClassDefinitionBuilder addPortableField(@Nonnull String fieldName, ClassDefinition def) {
        if (def.getClassId() == 0) {
            throw new IllegalArgumentException("Portable class ID cannot be zero!");
        }
        check(fieldName);
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName,
                FieldType.PORTABLE, def.getFactoryId(), def.getClassId(), def.getVersion()));
        return this;
    }

    /**
     * @param fieldName       name of the field that will be add to this class definition
     * @param classDefinition class definition of the nested portable that will be add to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     */
    @Nonnull
    public ClassDefinitionBuilder addPortableArrayField(@Nonnull String fieldName, ClassDefinition classDefinition) {
        if (classDefinition.getClassId() == 0) {
            throw new IllegalArgumentException("Portable class ID cannot be zero!");
        }
        check(fieldName);
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.PORTABLE_ARRAY,
                classDefinition.getFactoryId(), classDefinition.getClassId(), classDefinition.getVersion()));
        return this;
    }

    /**
     * Adds an array of Decimal's to the class definition
     *
     * @param fieldName name of the field that will be added to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     * @see #addDecimalField(String)
     */
    @Nonnull
    public ClassDefinitionBuilder addDecimalArrayField(@Nonnull String fieldName) {
        return addField(fieldName, FieldType.DECIMAL_ARRAY);
    }

    /**
     * Adds an array of Time's to the class definition
     *
     * @param fieldName name of the field that will be added to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     * @see #addTimeField(String)
     */
    @Nonnull
    public ClassDefinitionBuilder addTimeArrayField(@Nonnull String fieldName) {
        return addField(fieldName, FieldType.TIME_ARRAY);
    }

    /**
     * Adds an array of Date's to the class definition
     *
     * @param fieldName name of the field that will be added to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     * @see #addDateField(String)
     */
    @Nonnull
    public ClassDefinitionBuilder addDateArrayField(@Nonnull String fieldName) {
        return addField(fieldName, FieldType.DATE_ARRAY);
    }

    /**
     * Adds an array of Timestamp's to the class definition
     *
     * @param fieldName name of the field that will be added to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     * @see #addTimestampField(String)
     */
    @Nonnull
    public ClassDefinitionBuilder addTimestampArrayField(@Nonnull String fieldName) {
        return addField(fieldName, FieldType.TIMESTAMP_ARRAY);
    }

    /**
     * Adds an array of TimestampWithTimezone's to the class definition
     *
     * @param fieldName name of the field that will be added to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     * @see #addTimestampWithTimezoneField(String)
     */
    @Nonnull
    public ClassDefinitionBuilder addTimestampWithTimezoneArrayField(@Nonnull String fieldName) {
        return addField(fieldName, FieldType.TIMESTAMP_WITH_TIMEZONE_ARRAY);
    }

    private ClassDefinitionBuilder addField(@Nonnull String fieldName, FieldType fieldType) {
        check(fieldName);
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, fieldType, version));
        return this;
    }

    @PrivateApi
    public void addField(FieldDefinitionImpl fieldDefinition) {
        if (index != fieldDefinition.getIndex()) {
            throw new IllegalArgumentException("Invalid field index");
        }
        check(fieldDefinition.getName());
        index++;
        fieldDefinitions.add(fieldDefinition);
    }

    /**
     * @return creates and returns a new ClassDefinition
     */
    @Nonnull
    public ClassDefinition build() {
        done = true;
        final ClassDefinitionImpl cd = new ClassDefinitionImpl(factoryId, classId, version);
        for (FieldDefinitionImpl fd : fieldDefinitions) {
            cd.addFieldDef(fd);
        }
        return cd;
    }

    private void check(@Nonnull String fieldName) {
        if (!addedFieldNames.add(fieldName)) {
            throw new HazelcastSerializationException("Field with field name : " + fieldName + " already exists");
        }
        if (done) {
            throw new HazelcastSerializationException("ClassDefinition is already built for " + classId);
        }
    }

    public int getFactoryId() {
        return factoryId;
    }

    public int getClassId() {
        return classId;
    }

    public int getVersion() {
        return version;
    }
}
