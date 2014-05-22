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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.Set;

public final class BinaryClassDefinitionProxy extends BinaryClassDefinition implements ClassDefinition {

    public BinaryClassDefinitionProxy(int factoryId, int classId, int version, byte[] binary) {
        this.classId = classId;
        this.version = version;
        this.factoryId = factoryId;
        setBinary(binary);
    }

    public ClassDefinition toReal(PortableContext context) throws IOException {
        final ClassDefinition cd = context.lookup(factoryId, classId, version);
        return cd != null ? cd : context.createClassDefinition(factoryId, getBinary());
    }

    public FieldDefinition get(String name) {
        throw new UnsupportedOperationException();
    }

    public FieldDefinition get(int fieldIndex) {
        throw new UnsupportedOperationException();
    }

    public boolean hasField(String fieldName) {
        throw new UnsupportedOperationException();
    }

    public Set<String> getFieldNames() {
        throw new UnsupportedOperationException();
    }

    public FieldType getFieldType(String fieldName) {
        throw new UnsupportedOperationException();
    }

    public int getFieldClassId(String fieldName) {
        throw new UnsupportedOperationException();
    }

    public int getFieldVersion(String fieldName) {
        throw new UnsupportedOperationException();
    }

    public int getFieldCount() {
        throw new UnsupportedOperationException();
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

    public void readData(ObjectDataInput in) throws IOException {
        throw new UnsupportedOperationException();
    }
}
