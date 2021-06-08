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
import java.util.Collection;
import java.util.Comparator;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/**
 * Represents the schema of a class.
 * Consists of field definitions and the class name.
 */
public class Schema implements IdentifiedDataSerializable {

    private String typeName;
    private TreeMap<String, FieldDescriptor> fieldDefinitionMap;
    private int numberOfComplexFields;
    private int primitivesLength;
    private transient long schemaId;
    private boolean isSchemaIdSet;

    public Schema() {
    }

    public Schema(String typeName, TreeMap<String, FieldDescriptor> fieldDefinitionMap) {
        this.typeName = typeName;
        this.fieldDefinitionMap = fieldDefinitionMap;
        init();
    }

    private void init() {
        primitivesLength = 0;
        numberOfComplexFields = 0;
        int numberOfBooleans = 0;
        for (FieldDescriptor descriptor : fieldDefinitionMap.values()) {
            FieldType fieldType = descriptor.getType();
            if (FieldType.BOOLEAN.equals(fieldType)) {
                if (numberOfBooleans % Byte.SIZE == 0) {
                    primitivesLength++;
                }
                numberOfBooleans++;
            } else if (fieldType.hasDefiniteSize()) {
                primitivesLength += fieldType.getTypeSize();
            } else {
                numberOfComplexFields++;
            }
        }
    }

    /**
     * The class name provided when building a schema
     * In java, when it is not configured explicitly, this falls back to full class name including the path.
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

    public int getNumberOfVariableLengthFields() {
        return numberOfComplexFields;
    }

    public int getPrimitivesLength() {
        return primitivesLength;
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
        assert isSchemaIdSet;
        return schemaId;
    }

    public void setSchemaId(long schemaId) {
        this.isSchemaIdSet = true;
        this.schemaId = schemaId;
    }

    public boolean isSchemaIdSet() {
        return isSchemaIdSet;
    }

    @Override
    public String toString() {
        return "Schema {"
                + " className = " + typeName
                + " numberOfComplexFields = " + numberOfComplexFields
                + " primitivesLength = " + primitivesLength
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
            if (FieldType.BOOLEAN.equals(descriptor.getType())) {
                out.writeInt(descriptor.getOffset());
                out.writeByte(descriptor.getBitOffset());
            } else if (descriptor.getType().hasDefiniteSize()) {
                out.writeInt(descriptor.getOffset());
            } else {
                out.writeInt(descriptor.getIndex());
            }
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
            if (FieldType.BOOLEAN.equals(fieldType)) {
                descriptor.setOffset(in.readInt());
                descriptor.setBitOffset(in.readByte());
            } else if (fieldType.hasDefiniteSize()) {
                descriptor.setOffset(in.readInt());
            } else {
                descriptor.setIndex(in.readInt());
            }
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
        return numberOfComplexFields == schema.numberOfComplexFields
                && primitivesLength == schema.primitivesLength
                && schemaId == schema.schemaId
                && Objects.equals(typeName, schema.typeName)
                && Objects.equals(fieldDefinitionMap, schema.fieldDefinitionMap);
    }

    @Override
    public int hashCode() {
        return (int) schemaId;
    }
}
