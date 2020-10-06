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
    public ClassDefinitionBuilder addIntField(String fieldName) {
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
    public ClassDefinitionBuilder addLongField(String fieldName) {
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
    public ClassDefinitionBuilder addUTFField(String fieldName) {
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
    public ClassDefinitionBuilder addBooleanField(String fieldName) {
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
    public ClassDefinitionBuilder addByteField(String fieldName) {
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
    public ClassDefinitionBuilder addBooleanArrayField(String fieldName) {
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
    public ClassDefinitionBuilder addCharField(String fieldName) {
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
    public ClassDefinitionBuilder addDoubleField(String fieldName) {
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
    public ClassDefinitionBuilder addFloatField(String fieldName) {
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
    public ClassDefinitionBuilder addShortField(String fieldName) {
        check(fieldName);
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.SHORT, version));
        return this;
    }

    /**
     * @param fieldName name of the field that will be added to this class definition
     * @return itself for chaining
     * @throws HazelcastSerializationException if a field with same name already exists or
     *                                         if this method is called after {@link ClassDefinitionBuilder#build()}
     */
    public ClassDefinitionBuilder addByteArrayField(String fieldName) {
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
    public ClassDefinitionBuilder addCharArrayField(String fieldName) {
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
    public ClassDefinitionBuilder addIntArrayField(String fieldName) {
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
    public ClassDefinitionBuilder addLongArrayField(String fieldName) {
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
    public ClassDefinitionBuilder addDoubleArrayField(String fieldName) {
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
    public ClassDefinitionBuilder addFloatArrayField(String fieldName) {
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
    public ClassDefinitionBuilder addShortArrayField(String fieldName) {
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
    public ClassDefinitionBuilder addUTFArrayField(String fieldName) {
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
    public ClassDefinitionBuilder addPortableField(String fieldName, ClassDefinition def) {
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
    public ClassDefinitionBuilder addPortableArrayField(String fieldName, ClassDefinition classDefinition) {
        if (classDefinition.getClassId() == 0) {
            throw new IllegalArgumentException("Portable class ID cannot be zero!");
        }
        check(fieldName);
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.PORTABLE_ARRAY,
                classDefinition.getFactoryId(), classDefinition.getClassId(), classDefinition.getVersion()));
        return this;
    }

    @PrivateApi
    public ClassDefinitionBuilder addField(FieldDefinitionImpl fieldDefinition) {
        if (index != fieldDefinition.getIndex()) {
            throw new IllegalArgumentException("Invalid field index");
        }
        check(fieldDefinition.getName());
        index++;
        fieldDefinitions.add(fieldDefinition);
        return this;
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

    private void check(String fieldName) {
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
