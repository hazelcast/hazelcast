/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import java.util.ArrayList;
import java.util.List;

/**
 * ClassDefinitionBuilder is used to build and register ClassDefinitions manually.
 *
 * @see com.hazelcast.nio.serialization.ClassDefinition
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

    public ClassDefinitionBuilder(int factoryId, int classId) {
        this.factoryId = factoryId;
        this.classId = classId;
        this.version = -1;
    }

    public ClassDefinitionBuilder(int factoryId, int classId, int version) {
        this.factoryId = factoryId;
        this.classId = classId;
        this.version = version;
    }

    public ClassDefinitionBuilder addIntField(String fieldName) {
        check();
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.INT));
        return this;
    }

    public ClassDefinitionBuilder addLongField(String fieldName) {
        check();
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.LONG));
        return this;
    }

    public ClassDefinitionBuilder addUTFField(String fieldName) {
        check();
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.UTF));
        return this;
    }

    public ClassDefinitionBuilder addBooleanField(String fieldName) {
        check();
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.BOOLEAN));
        return this;
    }

    public ClassDefinitionBuilder addByteField(String fieldName) {
        check();
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.BYTE));
        return this;
    }

    public ClassDefinitionBuilder addCharField(String fieldName) {
        check();
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.CHAR));
        return this;
    }

    public ClassDefinitionBuilder addDoubleField(String fieldName) {
        check();
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.DOUBLE));
        return this;
    }

    public ClassDefinitionBuilder addFloatField(String fieldName) {
        check();
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.FLOAT));
        return this;
    }

    public ClassDefinitionBuilder addShortField(String fieldName) {
        check();
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.SHORT));
        return this;
    }

    public ClassDefinitionBuilder addByteArrayField(String fieldName) {
        check();
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.BYTE_ARRAY));
        return this;
    }

    public ClassDefinitionBuilder addCharArrayField(String fieldName) {
        check();
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.CHAR_ARRAY));
        return this;
    }

    public ClassDefinitionBuilder addIntArrayField(String fieldName) {
        check();
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.INT_ARRAY));
        return this;
    }

    public ClassDefinitionBuilder addLongArrayField(String fieldName) {
        check();
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.LONG_ARRAY));
        return this;
    }

    public ClassDefinitionBuilder addDoubleArrayField(String fieldName) {
        check();
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.DOUBLE_ARRAY));
        return this;
    }

    public ClassDefinitionBuilder addFloatArrayField(String fieldName) {
        check();
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.FLOAT_ARRAY));
        return this;
    }

    public ClassDefinitionBuilder addShortArrayField(String fieldName) {
        check();
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName, FieldType.SHORT_ARRAY));
        return this;
    }

    public ClassDefinitionBuilder addPortableField(String fieldName, ClassDefinition def) {
        check();
        if (def.getClassId() == 0) {
            throw new IllegalArgumentException("Portable class id cannot be zero!");
        }
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName,
                FieldType.PORTABLE, def.getFactoryId(), def.getClassId()));
        return this;
    }

    public ClassDefinitionBuilder addPortableArrayField(String fieldName, ClassDefinition def) {
        check();
        if (def.getClassId() == 0) {
            throw new IllegalArgumentException("Portable class id cannot be zero!");
        }
        fieldDefinitions.add(new FieldDefinitionImpl(index++, fieldName,
                FieldType.PORTABLE_ARRAY, def.getFactoryId(), def.getClassId()));
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
}
