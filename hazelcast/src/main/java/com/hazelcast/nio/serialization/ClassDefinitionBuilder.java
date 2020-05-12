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

import java.util.ArrayList;
import java.util.List;

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

    public ClassDefinitionBuilder addIntField(String fieldName) {
        check();
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.INT, version));
        return this;
    }

    public ClassDefinitionBuilder addLongField(String fieldName) {
        check();
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.LONG, version));
        return this;
    }

    public ClassDefinitionBuilder addUTFField(String fieldName) {
        check();
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.UTF, version));
        return this;
    }

    public ClassDefinitionBuilder addBooleanField(String fieldName) {
        check();
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.BOOLEAN, version));
        return this;
    }

    public ClassDefinitionBuilder addByteField(String fieldName) {
        check();
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.BYTE, version));
        return this;
    }

    public ClassDefinitionBuilder addBooleanArrayField(String fieldName) {
        check();
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.BOOLEAN_ARRAY, version));
        return this;
    }

    public ClassDefinitionBuilder addCharField(String fieldName) {
        check();
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.CHAR, version));
        return this;
    }

    public ClassDefinitionBuilder addDoubleField(String fieldName) {
        check();
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.DOUBLE, version));
        return this;
    }

    public ClassDefinitionBuilder addFloatField(String fieldName) {
        check();
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.FLOAT, version));
        return this;
    }

    public ClassDefinitionBuilder addShortField(String fieldName) {
        check();
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.SHORT, version));
        return this;
    }

    public ClassDefinitionBuilder addByteArrayField(String fieldName) {
        check();
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.BYTE_ARRAY, version));
        return this;
    }

    public ClassDefinitionBuilder addCharArrayField(String fieldName) {
        check();
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.CHAR_ARRAY, version));
        return this;
    }

    public ClassDefinitionBuilder addIntArrayField(String fieldName) {
        check();
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.INT_ARRAY, version));
        return this;
    }

    public ClassDefinitionBuilder addLongArrayField(String fieldName) {
        check();
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.LONG_ARRAY, version));
        return this;
    }

    public ClassDefinitionBuilder addDoubleArrayField(String fieldName) {
        check();
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.DOUBLE_ARRAY, version));
        return this;
    }

    public ClassDefinitionBuilder addFloatArrayField(String fieldName) {
        check();
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.FLOAT_ARRAY, version));
        return this;
    }

    public ClassDefinitionBuilder addShortArrayField(String fieldName) {
        check();
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.SHORT_ARRAY, version));
        return this;
    }

    public ClassDefinitionBuilder addUTFArrayField(String fieldName) {
        check();
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.UTF_ARRAY, version));
        return this;
    }

    public ClassDefinitionBuilder addPortableField(String fieldName, ClassDefinition def) {
        check();
        if (def.getClassId() == 0) {
            throw new IllegalArgumentException("Portable class ID cannot be zero!");
        }
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName,
                FieldType.PORTABLE, def.getFactoryId(), def.getClassId(), def.getVersion()));
        return this;
    }

    public ClassDefinitionBuilder addPortableArrayField(String fieldName, ClassDefinition def) {
        check();
        if (def.getClassId() == 0) {
            throw new IllegalArgumentException("Portable class ID cannot be zero!");
        }
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName,
                FieldType.PORTABLE_ARRAY, def.getFactoryId(), def.getClassId(), def.getVersion()));
        return this;
    }

    public ClassDefinitionBuilder addField(FieldDefinitionImpl fieldDefinition) {
        check();
        if (index != fieldDefinition.getIndex()) {
            throw new IllegalArgumentException("Invalid field index");
        }
        index++;
        fieldDefinitions.add(fieldDefinition);
        return this;
    }

    public ClassDefinition build() {
        done = true;
        final ClassDefinitionImpl cd = new ClassDefinitionImpl(factoryId, classId, version);
        for (FieldDefinitionImpl fd : fieldDefinitions) {
            cd.addFieldDef(fd);
        }
        return cd;
    }

    private void check() {
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
