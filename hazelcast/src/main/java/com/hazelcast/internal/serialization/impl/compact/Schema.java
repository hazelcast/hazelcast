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

import com.hazelcast.internal.serialization.impl.compact.schema.SchemaDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

import static com.hazelcast.internal.serialization.impl.FieldOperations.fieldOperations;
import static com.hazelcast.internal.serialization.impl.FieldTypeBasedOperations.VARIABLE_SIZE;

/**
 * Represents the schema of a class.
 * Consists of field definitions and the class name.
 */
public class Schema implements IdentifiedDataSerializable {

    private String typeName;
    private TreeMap<String, FieldDescriptor> fieldDefinitionMap;
    private int numberVarSizeFields;
    private int fixedSizeFieldsLength;
    private transient long schemaId;

    public Schema() {
    }

    public Schema(String typeName, TreeMap<String, FieldDescriptor> fieldDefinitionMap) {
        this.typeName = typeName;
        this.fieldDefinitionMap = fieldDefinitionMap;
        init();
    }

    private void init() {
        List<FieldDescriptor> fixedSizeFields = new ArrayList<>();
        List<FieldDescriptor> booleanFields = new ArrayList<>();
        List<FieldDescriptor> variableSizeFields = new ArrayList<>();

        for (FieldDescriptor descriptor : fieldDefinitionMap.values()) {
            FieldType fieldType = descriptor.getType();
            if (fieldOperations(fieldType).typeSizeInBytes() == VARIABLE_SIZE) {
                variableSizeFields.add(descriptor);
            } else {
                if (FieldType.BOOLEAN.equals(fieldType)) {
                    booleanFields.add(descriptor);
                } else {
                    fixedSizeFields.add(descriptor);
                }
            }
        }

        fixedSizeFields.sort(Comparator.comparingInt(
                d -> fieldOperations(((FieldDescriptor) d).getType()).typeSizeInBytes()).reversed()
        );

        int offset = 0;
        for (FieldDescriptor descriptor : fixedSizeFields) {
            descriptor.setOffset(offset);
            offset += fieldOperations(descriptor.getType()).typeSizeInBytes();
        }

        int bitOffset = 0;
        for (FieldDescriptor descriptor : booleanFields) {
            descriptor.setOffset(offset);
            descriptor.setBitOffset((byte) (bitOffset % Byte.SIZE));
            bitOffset++;
            if (bitOffset % Byte.SIZE == 0) {
                offset += 1;
            }
        }
        if (bitOffset % Byte.SIZE != 0) {
            offset++;
        }

        fixedSizeFieldsLength = offset;

        int index = 0;
        for (FieldDescriptor descriptor : variableSizeFields) {
            descriptor.setIndex(index++);
        }

        numberVarSizeFields = index;
        schemaId = RabinFingerprint.fingerprint64(this);
    }

    /**
     * The class name provided when building a schema
     * In Java, when it is not configured explicitly, this falls back to
     * fully qualified class name.
     *
     * @return name of the class
     */
    public String getTypeName() {
        return typeName;
    }

    public Collection<FieldDescriptor> getFields() {
        return fieldDefinitionMap.values();
    }

    public Set<String> getFieldNames() {
        return fieldDefinitionMap.keySet();
    }

    public int getNumberOfVariableSizeFields() {
        return numberVarSizeFields;
    }

    public int getFixedSizeFieldsLength() {
        return fixedSizeFieldsLength;
    }

    public int getFieldCount() {
        return fieldDefinitionMap.size();
    }

    public FieldDescriptor getField(String fieldName) {
        return fieldDefinitionMap.get(fieldName);
    }

    public boolean hasField(String fieldName) {
        return fieldDefinitionMap.containsKey(fieldName);
    }

    public long getSchemaId() {
        return schemaId;
    }

    @Override
    public String toString() {
        return "Schema {"
                + " className = " + typeName
                + " numberOfComplexFields = " + numberVarSizeFields
                + " primitivesLength = " + fixedSizeFieldsLength
                + ", map = " + fieldDefinitionMap
                + '}';
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(typeName);
        out.writeInt(fieldDefinitionMap.size());
        Collection<FieldDescriptor> fields = fieldDefinitionMap.values();
        for (FieldDescriptor descriptor : fields) {
            out.writeString(descriptor.getFieldName());
            out.writeByte(descriptor.getType().getId());
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        typeName = in.readString();
        int fieldDefinitionsSize = in.readInt();
        fieldDefinitionMap = new TreeMap<>(Comparator.naturalOrder());
        for (int i = 0; i < fieldDefinitionsSize; i++) {
            String fieldName = in.readString();
            byte type = in.readByte();
            FieldType fieldType = FieldType.get(type);
            FieldDescriptor descriptor = new FieldDescriptor(fieldName, fieldType);
            fieldDefinitionMap.put(fieldName, descriptor);
        }
        init();
    }

    @Override
    public int getFactoryId() {
        return SchemaDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SchemaDataSerializerHook.SCHEMA;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Schema schema = (Schema) o;
        return numberVarSizeFields == schema.numberVarSizeFields
                && fixedSizeFieldsLength == schema.fixedSizeFieldsLength
                && schemaId == schema.schemaId
                && Objects.equals(typeName, schema.typeName)
                && Objects.equals(fieldDefinitionMap, schema.fieldDefinitionMap);
    }

    @Override
    public int hashCode() {
        return (int) schemaId;
    }
}
