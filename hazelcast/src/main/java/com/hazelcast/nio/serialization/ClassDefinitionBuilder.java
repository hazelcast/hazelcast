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

/**
 * @mdogan 2/6/13
 */
public final class ClassDefinitionBuilder {

    private final ClassDefinitionImpl cd = new ClassDefinitionImpl();
    private int index = 0;
    private boolean done = false;

    public ClassDefinitionBuilder(int classId) {
        cd.classId = classId;
    }

    ClassDefinitionBuilder(int classId, int version) {
        cd.classId = classId;
        cd.version = version;
    }

    public ClassDefinitionBuilder addIntField(String fieldName) {
        check();
        cd.add(new FieldDefinitionImpl(index++, fieldName, FieldType.INT));
        return this;
    }

    public ClassDefinitionBuilder addLongField(String fieldName) {
        check();
        cd.add(new FieldDefinitionImpl(index++, fieldName, FieldType.LONG));
        return this;
    }

    public ClassDefinitionBuilder addUTFField(String fieldName) {
        check();
        cd.add(new FieldDefinitionImpl(index++, fieldName, FieldType.UTF));
        return this;
    }

    public ClassDefinitionBuilder addBooleanField(String fieldName) {
        check();
        cd.add(new FieldDefinitionImpl(index++, fieldName, FieldType.BOOLEAN));
        return this;
    }

    public ClassDefinitionBuilder addByteField(String fieldName) {
        check();
        cd.add(new FieldDefinitionImpl(index++, fieldName, FieldType.BYTE));
        return this;
    }

    public ClassDefinitionBuilder addCharField(String fieldName) {
        check();
        cd.add(new FieldDefinitionImpl(index++, fieldName, FieldType.CHAR));
        return this;
    }

    public ClassDefinitionBuilder addDoubleField(String fieldName) {
        check();
        cd.add(new FieldDefinitionImpl(index++, fieldName, FieldType.DOUBLE));
        return this;
    }

    public ClassDefinitionBuilder addFloatField(String fieldName) {
        check();
        cd.add(new FieldDefinitionImpl(index++, fieldName, FieldType.FLOAT));
        return this;
    }

    public ClassDefinitionBuilder addShortField(String fieldName) {
        check();
        cd.add(new FieldDefinitionImpl(index++, fieldName, FieldType.SHORT));
        return this;
    }

    public ClassDefinitionBuilder addByteArrayField(String fieldName) {
        check();
        cd.add(new FieldDefinitionImpl(index++, fieldName, FieldType.BYTE_ARRAY));
        return this;
    }

    public ClassDefinitionBuilder addCharArrayField(String fieldName) {
        check();
        cd.add(new FieldDefinitionImpl(index++, fieldName, FieldType.CHAR_ARRAY));
        return this;
    }

    public ClassDefinitionBuilder addIntArrayField(String fieldName) {
        check();
        cd.add(new FieldDefinitionImpl(index++, fieldName, FieldType.INT_ARRAY));
        return this;
    }

    public ClassDefinitionBuilder addLongArrayField(String fieldName) {
        check();
        cd.add(new FieldDefinitionImpl(index++, fieldName, FieldType.LONG_ARRAY));
        return this;
    }

    public ClassDefinitionBuilder addDoubleArrayField(String fieldName) {
        check();
        cd.add(new FieldDefinitionImpl(index++, fieldName, FieldType.DOUBLE_ARRAY));
        return this;
    }

    public ClassDefinitionBuilder addFloatArrayField(String fieldName) {
        check();
        cd.add(new FieldDefinitionImpl(index++, fieldName, FieldType.FLOAT_ARRAY));
        return this;
    }

    public ClassDefinitionBuilder addShortArrayField(String fieldName) {
        check();
        cd.add(new FieldDefinitionImpl(index++, fieldName, FieldType.SHORT_ARRAY));
        return this;
    }

    public ClassDefinitionBuilder addPortableField(String fieldName, int classId) {
        check();
        if (classId == Data.NO_CLASS_ID) {
            throw new IllegalArgumentException("Portable class id cannot be zero!");
        }
        cd.add(new FieldDefinitionImpl(index++, fieldName, FieldType.PORTABLE, classId));
        return this;
    }

    public ClassDefinitionBuilder addPortableArrayField(String fieldName, int classId) {
        check();
        if (classId == Data.NO_CLASS_ID) {
            throw new IllegalArgumentException("Portable class id cannot be zero!");
        }
        cd.add(new FieldDefinitionImpl(index++, fieldName, FieldType.PORTABLE_ARRAY, classId));
        return this;
    }

    public ClassDefinition build() {
        done = true;
        return cd;
    }

    private void check() {
        if (done) {
            throw new HazelcastSerializationException("ClassDefinition is already built for " + cd.getClassId());
        }
    }

    ClassDefinitionImpl getCd() {
        return cd;
    }
}
