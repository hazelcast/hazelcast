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
class ClassDefinitionBuilderImpl implements ClassDefinitionBuilder {

    private final SerializationContext context;
    private final ClassDefinitionBuilderImpl parent;
    private final ClassDefinitionImpl cd = new ClassDefinitionImpl();
    private int index = 0;
    private boolean done = false;

    ClassDefinitionBuilderImpl(SerializationContext context, int classId) {
        this(context, classId, null);
    }

    private ClassDefinitionBuilderImpl(SerializationContext context, int classId, ClassDefinitionBuilderImpl parent) {
        this.context = context;
        this.parent = parent;
        cd.classId = classId;
        cd.version = context.getVersion();
    }

    public void addIntField(String fieldName) {
        check();
        cd.add(new FieldDefinitionImpl(index++, fieldName, FieldDefinition.TYPE_INT));
    }

    public void addLongField(String fieldName) {
        check();
        cd.add(new FieldDefinitionImpl(index++, fieldName, FieldDefinition.TYPE_LONG));
    }

    public void addUTFField(String fieldName) {
        check();
        cd.add(new FieldDefinitionImpl(index++, fieldName, FieldDefinition.TYPE_UTF));
    }

    public void addBooleanField(String fieldName) {
        check();
        cd.add(new FieldDefinitionImpl(index++, fieldName, FieldDefinition.TYPE_BOOLEAN));
    }

    public void addByteField(String fieldName) {
        check();
        cd.add(new FieldDefinitionImpl(index++, fieldName, FieldDefinition.TYPE_BYTE));
    }

    public void addCharField(String fieldName) {
        check();
        cd.add(new FieldDefinitionImpl(index++, fieldName, FieldDefinition.TYPE_CHAR));
    }

    public void addDoubleField(String fieldName) {
        check();
        cd.add(new FieldDefinitionImpl(index++, fieldName, FieldDefinition.TYPE_DOUBLE));
    }

    public void addFloatField(String fieldName) {
        check();
        cd.add(new FieldDefinitionImpl(index++, fieldName, FieldDefinition.TYPE_FLOAT));
    }

    public void addShortField(String fieldName) {
        check();
        cd.add(new FieldDefinitionImpl(index++, fieldName, FieldDefinition.TYPE_SHORT));
    }

    public void addByteArrayField(String fieldName) {
        check();
        cd.add(new FieldDefinitionImpl(index++, fieldName, FieldDefinition.TYPE_BYTE_ARRAY));
    }

    public void addCharArrayField(String fieldName) {
        check();
        cd.add(new FieldDefinitionImpl(index++, fieldName, FieldDefinition.TYPE_CHAR_ARRAY));
    }

    public void addIntArrayField(String fieldName) {
        check();
        cd.add(new FieldDefinitionImpl(index++, fieldName, FieldDefinition.TYPE_INT_ARRAY));
    }

    public void addLongArrayField(String fieldName) {
        check();
        cd.add(new FieldDefinitionImpl(index++, fieldName, FieldDefinition.TYPE_LONG_ARRAY));
    }

    public void addDoubleArrayField(String fieldName) {
        check();
        cd.add(new FieldDefinitionImpl(index++, fieldName, FieldDefinition.TYPE_DOUBLE_ARRAY));
    }

    public void addFloatArrayField(String fieldName) {
        check();
        cd.add(new FieldDefinitionImpl(index++, fieldName, FieldDefinition.TYPE_FLOAT_ARRAY));
    }

    public void addShortArrayField(String fieldName) {
        check();
        cd.add(new FieldDefinitionImpl(index++, fieldName, FieldDefinition.TYPE_SHORT_ARRAY));
    }

    public ClassDefinitionBuilder createPortableFieldBuilder(String fieldName, int classId) {
        check();
        cd.add(new FieldDefinitionImpl(index++, fieldName, FieldDefinition.TYPE_PORTABLE, classId));
        return new ClassDefinitionBuilderImpl(context, classId, this);
    }

    public ClassDefinitionBuilder createPortableArrayFieldBuilder(String fieldName, int classId) {
        check();
        cd.add(new FieldDefinitionImpl(index++, fieldName, FieldDefinition.TYPE_PORTABLE_ARRAY, classId));
        return new ClassDefinitionBuilderImpl(context, classId, this);
    }

    public void buildAndRegister() {
        done = true;
        final ClassDefinition classDef = context.registerClassDefinition(cd);
        if (parent != null) {
            parent.addNested((ClassDefinitionImpl) classDef);
        }
    }

    private void check() {
        if (done) {
            throw new HazelcastSerializationException("ClassDefinition is already built for " + cd.getClassId());
        }
    }

    void addNested(ClassDefinitionImpl nestedCd) {
        cd.add(nestedCd);
    }

    ClassDefinitionImpl getCd() {
        return cd;
    }
}
