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

import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.schema.ExternalTable.ExternalField;
import com.hazelcast.sql.impl.schema.TableField;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public abstract class SqlKeyValueConnector implements SqlConnector {

    public static final String TO_SERIALIZATION_KEY_FORMAT = "serialization.key.format";
    public static final String TO_SERIALIZATION_VALUE_FORMAT = "serialization.value.format";

    /**
     * A key in the table options (TO).
     * <p>
     * Specifies the key class in the entry. Can be omitted if "__key" is one
     * of the columns.
     */
    public static final String TO_KEY_CLASS = "serialization.key.pojo.class";

    /**
     * A key in the table options (TO).
     * <p>
     * Specifies the value class in the entry. Can be omitted if "this" is one
     * of the columns.
     */
    public static final String TO_VALUE_CLASS = "serialization.value.pojo.class";

    public static final String TO_KEY_FACTORY_ID = "serialization.key.portable.factoryId";
    public static final String TO_KEY_CLASS_ID = "serialization.key.portable.classId";
    public static final String TO_KEY_CLASS_VERSION = "serialization.key.portable.classVersion";

    public static final String TO_VALUE_FACTORY_ID = "serialization.value.portable.factoryId";
    public static final String TO_VALUE_CLASS_ID = "serialization.value.portable.classId";
    public static final String TO_VALUE_CLASS_VERSION = "serialization.value.portable.classVersion";

    // TODO: deduplicate with AbstractMapTableResolver
    protected static List<TableField> mergeFields(
            List<ExternalField> externalFields,
            Map<String, TableField> keyFields,
            Map<String, TableField> valueFields
    ) {
        LinkedHashMap<String, TableField> fields = new LinkedHashMap<>(keyFields);

        // value fields do not override key fields.
        for (Entry<String, TableField> valueFieldEntry : valueFields.entrySet()) {
            fields.putIfAbsent(valueFieldEntry.getKey(), valueFieldEntry.getValue());
        }

        // all declared fields should be mapped to neither key nor value
        List<String> unmappedFields = externalFields.stream()
                                                    .filter(externalField -> fields.get(externalField.name()) == null)
                                                    .map(ExternalField::name)
                                                    .collect(Collectors.toList());
        if (!unmappedFields.isEmpty()) {
            throw QueryException.error("Unmapped fields: " + unmappedFields);
        }

        return new ArrayList<>(fields.values());
    }
}
