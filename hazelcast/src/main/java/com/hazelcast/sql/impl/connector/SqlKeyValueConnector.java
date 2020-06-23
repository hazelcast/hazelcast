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

package com.hazelcast.sql.impl.connector;

import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.ExternalTable.ExternalField;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableField;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class SqlKeyValueConnector implements SqlConnector {

    public static final String TO_SERIALIZATION_KEY_FORMAT = "serialization.key.format";
    public static final String TO_SERIALIZATION_VALUE_FORMAT = "serialization.value.format";

    /**
     * A key in the table options (TO).
     * <p>
     * Specifies the key class in the entry. Can be omitted if "__key" is one
     * of the columns.
     */
    public static final String TO_KEY_CLASS = "keyClass";

    /**
     * A key in the table options (TO).
     * <p>
     * Specifies the value class in the entry. Can be omitted if "this" is one
     * of the columns.
     */
    public static final String TO_VALUE_CLASS = "valueClass";

    public static final String TO_KEY_FACTORY_ID = "keyFactoryId";
    public static final String TO_KEY_CLASS_ID = "keyClassId";
    public static final String TO_KEY_CLASS_VERSION = "keyClassVersion";

    public static final String TO_VALUE_FACTORY_ID = "valueFactoryId";
    public static final String TO_VALUE_CLASS_ID = "valueClassId";
    public static final String TO_VALUE_CLASS_VERSION = "valueClassVersion";

    protected static List<TableField> mergeFields(
            List<ExternalField> externalFields,
            Map<String, QueryPath> keyFields,
            Map<String, QueryPath> valueFields
    ) {
        List<TableField> fields = new ArrayList<>(externalFields.size());

        for (ExternalField externalField : externalFields) {
            String fieldName = externalField.name();

            QueryPath queryPath;
            if (keyFields.containsKey(fieldName)) {
                queryPath = keyFields.get(fieldName);
            } else if (valueFields.containsKey(fieldName)) {
                queryPath = valueFields.get(fieldName);
            } else {
                // allow nulls for non existing fields
                queryPath = QueryPath.create(fieldName);
            }

            TableField field = new MapTableField(
                    externalField.name(),
                    externalField.type(),
                    false,
                    queryPath
            );

            fields.add(field);
        }

        return fields;
    }
}
