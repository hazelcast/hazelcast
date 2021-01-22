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
    public ClassDefinitionBuilder addIntField(@Nonnull String fieldName) {
        check(fieldName);
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.INT, version));
        return this;
    }

    /**
     * @param fieldName name of the field that will be added to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     */
    public ClassDefinitionBuilder addLongField(@Nonnull String fieldName) {
        check(fieldName);
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.LONG, version));
        return this;
    }

    /**
     * @param fieldName name of the field that will be added to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     */
    public ClassDefinitionBuilder addUTFField(@Nonnull String fieldName) {
        check(fieldName);
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.UTF, version));
        return this;
    }

    /**
     * @param fieldName name of the field that will be added to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     */
    public ClassDefinitionBuilder addBooleanField(@Nonnull String fieldName) {
        check(fieldName);
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.BOOLEAN, version));
        return this;
    }

    /**
     * @param fieldName name of the field that will be added to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     */
    public ClassDefinitionBuilder addByteField(@Nonnull String fieldName) {
        check(fieldName);
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.BYTE, version));
        return this;
    }

    /**
     * @param fieldName name of the field that will be added to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     */
    public ClassDefinitionBuilder addBooleanArrayField(@Nonnull String fieldName) {
        check(fieldName);
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.BOOLEAN_ARRAY, version));
        return this;
    }

    /**
     * @param fieldName name of the field that will be added to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     */
    public ClassDefinitionBuilder addCharField(@Nonnull String fieldName) {
        check(fieldName);
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.CHAR, version));
        return this;
    }

    /**
     * @param fieldName name of the field that will be added to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     */
    public ClassDefinitionBuilder addDoubleField(@Nonnull String fieldName) {
        check(fieldName);
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.DOUBLE, version));
        return this;
    }

    /**
     * @param fieldName name of the field that will be added to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     */
    public ClassDefinitionBuilder addFloatField(@Nonnull String fieldName) {
        check(fieldName);
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.FLOAT, version));
        return this;
    }

    /**
     * @param fieldName name of the field that will be added to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     */
    public ClassDefinitionBuilder addShortField(@Nonnull String fieldName) {
        check(fieldName);
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.SHORT, version));
        return this;
    }

    /**
     * Adds a decimal which is arbitrary precision and scale floating-point number to the class definition
     *
     * @param fieldName name of the field that will be added to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     */
    public ClassDefinitionBuilder addDecimalField(@Nonnull String fieldName) {
        check(fieldName);
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.DECIMAL, version));
        return this;
    }

    /**
     * Adds a time field consisting of hour, minute, seconds and nanos parts to the class definition
     *
     * @param fieldName name of the field that will be added to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     */
    public ClassDefinitionBuilder addTimeField(@Nonnull String fieldName) {
        check(fieldName);
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.TIME, version));
        return this;
    }

    /**
     * Adds a date field consisting of year , month of the year and day of the month to the class definition
     *
     * @param fieldName name of the field that will be added to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     */
    public ClassDefinitionBuilder addDateField(@Nonnull String fieldName) {
        check(fieldName);
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.DATE, version));
        return this;
    }

    /**
     * Adds a timestamp field consisting of
     * year , month of the year and day of the month, hour, minute, seconds, nanos parts to the class definition
     *
     * @param fieldName name of the field that will be added to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     */
    public ClassDefinitionBuilder addTimestampField(@Nonnull String fieldName) {
        check(fieldName);
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.TIMESTAMP, version));
        return this;
    }

    /**
     * Adds a timestamp with timezone field consisting of
     * year , month of the year and day of the month, offset seconds , hour, minute, seconds, nanos parts
     * to the class definition
     *
     * @param fieldName name of the field that will be added to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     */
    public ClassDefinitionBuilder addTimestampWithTimezoneField(@Nonnull String fieldName) {
        check(fieldName);
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.TIMESTAMP_WITH_TIMEZONE, version));
        return this;
    }

    /**
     * @param fieldName name of the field that will be added to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     */
    public ClassDefinitionBuilder addByteArrayField(@Nonnull String fieldName) {
        check(fieldName);
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.BYTE_ARRAY, version));
        return this;
    }

    /**
     * @param fieldName name of the field that will be added to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     */
    public ClassDefinitionBuilder addCharArrayField(@Nonnull String fieldName) {
        check(fieldName);
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.CHAR_ARRAY, version));
        return this;
    }

    /**
     * @param fieldName name of the field that will be added to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     */
    public ClassDefinitionBuilder addIntArrayField(@Nonnull String fieldName) {
        check(fieldName);
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.INT_ARRAY, version));
        return this;
    }

    /**
     * @param fieldName name of the field that will be added to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     */
    public ClassDefinitionBuilder addLongArrayField(@Nonnull String fieldName) {
        check(fieldName);
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.LONG_ARRAY, version));
        return this;
    }

    /**
     * @param fieldName name of the field that will be added to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     */
    public ClassDefinitionBuilder addDoubleArrayField(@Nonnull String fieldName) {
        check(fieldName);
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.DOUBLE_ARRAY, version));
        return this;
    }

    /**
     * @param fieldName name of the field that will be added to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     */
    public ClassDefinitionBuilder addFloatArrayField(@Nonnull String fieldName) {
        check(fieldName);
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.FLOAT_ARRAY, version));
        return this;
    }

    /**
     * @param fieldName name of the field that will be added to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     */
    public ClassDefinitionBuilder addShortArrayField(@Nonnull String fieldName) {
        check(fieldName);
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.SHORT_ARRAY, version));
        return this;
    }

    /**
     * @param fieldName name of the field that will be added to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     */
    public ClassDefinitionBuilder addUTFArrayField(@Nonnull String fieldName) {
        check(fieldName);
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.UTF_ARRAY, version));
        return this;
    }

    /**
     * @param fieldName name of the field that will be added to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     */
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
    public ClassDefinitionBuilder addDecimalArrayField(@Nonnull String fieldName) {
        check(fieldName);
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.DECIMAL_ARRAY, version));
        return this;
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
    public ClassDefinitionBuilder addTimeArrayField(@Nonnull String fieldName) {
        check(fieldName);
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.TIME_ARRAY, version));
        return this;
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
    public ClassDefinitionBuilder addDateArrayField(@Nonnull String fieldName) {
        check(fieldName);
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.DATE_ARRAY, version));
        return this;
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
    public ClassDefinitionBuilder addTimestampArrayField(@Nonnull String fieldName) {
        check(fieldName);
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.TIMESTAMP_ARRAY, version));
        return this;
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
    public ClassDefinitionBuilder addTimestampWithTimezoneArrayField(@Nonnull String fieldName) {
        check(fieldName);
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.TIMESTAMP_WITH_TIMEZONE_ARRAY, version));
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
