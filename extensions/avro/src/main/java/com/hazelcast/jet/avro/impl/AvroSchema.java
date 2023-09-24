/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.avro.impl;

import com.hazelcast.internal.serialization.impl.compact.FieldDescriptor;
import com.hazelcast.jet.avro.AvroSerializerHooks;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import org.apache.avro.Schema;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toSet;
import static org.apache.avro.SchemaNormalization.fingerprint64;
import static org.apache.avro.SchemaNormalization.toParsingForm;

/**
 * Schema ID is the 64-bit Rabin fingerprint of the canonical JSON representation of the schema.
 * As explained in the <a href="https://avro.apache.org/docs/1.11.1/specification/#schema-fingerprints">
 * Avro specification</a>, such fingerprints are safe to be used as a key in schema caches of up
 * to a million entries (for such a cache, the chance of a collision is 3x10<sup>-8</sup>).
 */
public class AvroSchema extends com.hazelcast.internal.serialization.impl.compact.Schema {
    private Schema schema;
    private String schemaJson;

    public AvroSchema() { }

    public AvroSchema(Schema schema) {
        this.schema = schema;
        init(toParsingForm(schema));
    }

    @SuppressWarnings("unused") // used by CustomTypeFactory via reflection
    public AvroSchema(String schemaJson) {
        init(schemaJson);
    }

    private void init(String schemaJson) {
        if (schema == null) {
            schema = new Schema.Parser().parse(schemaJson);
        }
        this.schemaJson = schemaJson;
        typeName = getClass().getName() + "\0" + schemaJson;
        schemaId = fingerprint64(schemaJson.getBytes(StandardCharsets.UTF_8));
    }

    public Schema getSchema() {
        return schema;
    }

    @Override
    public Collection<FieldDescriptor> getFields() {
        // We return an empty list in order not to break SchemaCodec.
        // Schema fields are included in the type name.
        return emptyList();
    }

    @Override
    public Set<String> getFieldNames() {
        return schema.getFields().stream().map(Schema.Field::name).collect(toSet());
    }

    @Override
    public boolean hasField(String fieldName) {
        return schema.getField(fieldName) != null;
    }

    @Override
    public int getFieldCount() {
        return schema.getFields().size();
    }

    @Override
    public String toString() {
        return "AvroSchema " + schemaJson;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(schemaJson);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        init(in.readString());
    }

    @Override
    public int getFactoryId() {
        return AvroSerializerHooks.F_ID;
    }

    @Override
    public int getClassId() {
        return AvroSerializerHooks.SCHEMA;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return schema.equals(((AvroSchema) o).schema);
    }

    @Override
    public int hashCode() {
        return schema.hashCode();
    }

    @Override
    public FieldDescriptor getField(String fieldName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getNumberOfVariableSizeFields() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getFixedSizeFieldsLength() {
        throw new UnsupportedOperationException();
    }
}
