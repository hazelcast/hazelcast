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

package com.hazelcast.jet.sql.impl.connector.file;

import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataAvroResolver.Schemas.AVRO_TO_SQL;
import static com.hazelcast.sql.impl.type.QueryDataType.OBJECT;
import static org.apache.avro.Schema.Type.NULL;

public final class AvroResolver {
    private AvroResolver() { }

    // CREATE MAPPING <name> TYPE File OPTIONS ('format'='avro', ...)
    // TABLE(AVRO_FILE(...))
    static List<MappingField> resolveFields(Schema schema) {
        Map<String, MappingField> fields = new LinkedHashMap<>();
        for (Schema.Field schemaField : schema.getFields()) {
            String name = schemaField.name();
            // SQL types are nullable by default and NOT NULL is currently unsupported.
            Schema.Type schemaFieldType = unwrapNullableType(schemaField.schema()).getType();
            QueryDataType type = AVRO_TO_SQL.getOrDefault(schemaFieldType, OBJECT);

            MappingField field = new MappingField(name, type);
            fields.putIfAbsent(field.name(), field);
        }
        return new ArrayList<>(fields.values());
    }

    /**
     * For nullable types, i.e. {@code [aType, null]}, returns {@code aType}.
     * Otherwise, returns the specified type as-is.
     */
    public static Schema unwrapNullableType(Schema schema) {
        if (schema.isUnion()) {
            List<Schema> unionSchemas = schema.getTypes();
            if (unionSchemas.size() == 2) {
                if (unionSchemas.get(0).getType() == NULL) {
                    return unionSchemas.get(1);
                } else if (unionSchemas.get(1).getType() == NULL) {
                    return unionSchemas.get(0);
                }
            }
        }
        return schema;
    }
}
