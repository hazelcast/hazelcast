/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.serialization.FieldKind;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.hazelcast.internal.serialization.impl.FieldKindBasedOperations.VARIABLE_SIZE;
import static com.hazelcast.internal.serialization.impl.FieldOperations.fieldOperations;

/**
 * Represents the schema of a class.
 * Consists of field definitions and the class name.
 */
public class Schema implements IdentifiedDataSerializable {

    private String typeName;
    private Map<String, FieldDescriptor> fieldsMap;
    private List<FieldDescriptor> fields;
    private int numberVarSizeFields;
    private int fixedSizeFieldsLength;
    private transient long schemaId;

    public Schema() {
    }

    public Schema(String typeName, List<FieldDescriptor> fields) {
        this.typeName = typeName;
        this.fields = fields;
        init();
    }

    private void init() {
        // Construct the fields map for field lookups
        Map<String, FieldDescriptor> fieldsMap = new HashMap<>(fields.size());
        for (FieldDescriptor field : fields) {
            fieldsMap.put(field.getFieldName(), field);
        }
        this.fieldsMap = fieldsMap;

        // Sort the fields by the field name so that the field offsets/indexes
        // can be set correctly.
        fields.sort(Comparator.comparing(FieldDescriptor::getFieldName));

        List<FieldDescriptor> fixedSizeFields = new ArrayList<>();
        List<FieldDescriptor> booleanFields = new ArrayList<>();
        List<FieldDescriptor> variableSizeFields = new ArrayList<>();

        for (FieldDescriptor descriptor : fields) {
            FieldKind fieldKind = descriptor.getKind();
            if (fieldOperations(fieldKind).kindSizeInBytes() == VARIABLE_SIZE) {
                variableSizeFields.add(descriptor);
            } else {
                if (FieldKind.BOOLEAN == fieldKind) {
                    booleanFields.add(descriptor);
                } else {
                    fixedSizeFields.add(descriptor);
                }
            }
        }

        // Fixed size fields should be in descending order of size in bytes.
        // For ties, the alphabetical order(ascending) of the field name will
        // be used. Since, `fields` is sorted at this point, and the `sort`
        // method is stable, only sorting by the size in bytes is enough for
        // this invariant to hold.
        fixedSizeFields.sort(
                Comparator.comparingInt(
                        d -> fieldOperations(((FieldDescriptor) d).getKind()).kindSizeInBytes()
                ).reversed()
        );

        int offset = 0;
        for (FieldDescriptor descriptor : fixedSizeFields) {
            descriptor.setOffset(offset);
            offset += fieldOperations(descriptor.getKind()).kindSizeInBytes();
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

        // Variable size fields should be in ascending alphabetical ordering
        // of the field names
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
        return fields;
    }

    public Set<String> getFieldNames() {
        return fieldsMap.keySet();
    }

    public int getNumberOfVariableSizeFields() {
        return numberVarSizeFields;
    }

    public int getFixedSizeFieldsLength() {
        return fixedSizeFieldsLength;
    }

    public int getFieldCount() {
        return fieldsMap.size();
    }

    public FieldDescriptor getField(String fieldName) {
        return fieldsMap.get(fieldName);
    }

    public boolean hasField(String fieldName) {
        return fieldsMap.containsKey(fieldName);
    }

    public long getSchemaId() {
        return schemaId;
    }

    @Override
    public String toString() {
        return "Schema {"
                + " className = " + typeName
                + ", numberOfComplexFields = " + numberVarSizeFields
                + ", primitivesLength = " + fixedSizeFieldsLength
                + ", map = " + fieldsMap
                + '}';
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(typeName);
        out.writeInt(fields.size());
        for (FieldDescriptor descriptor : fields) {
            out.writeString(descriptor.getFieldName());
            out.writeInt(descriptor.getKind().getId());
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        typeName = in.readString();
        int fieldCount = in.readInt();
        fields = new ArrayList<>(fieldCount);
        for (int i = 0; i < fieldCount; i++) {
            String name = in.readString();
            FieldKind kind = FieldKind.get(in.readInt());
            FieldDescriptor descriptor = new FieldDescriptor(name, kind);
            fields.add(descriptor);
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
                && Objects.equals(fields, schema.fields)
                && Objects.equals(fieldsMap, schema.fieldsMap);
    }

    @Override
    public int hashCode() {
        return (int) schemaId;
    }
}
