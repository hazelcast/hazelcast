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

package com.hazelcast.jet.sql.impl.connector.file;

import com.fasterxml.jackson.jr.stree.JrsBoolean;
import com.fasterxml.jackson.jr.stree.JrsObject;
import com.fasterxml.jackson.jr.stree.JrsValue;
import com.hazelcast.jet.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

final class JsonResolver {

    private JsonResolver() {
    }

    static List<MappingField> resolveFields(JrsObject object) {
        Map<String, MappingField> fields = new LinkedHashMap<>();
        Iterator<Entry<String, JrsValue>> iterator = object.fields();
        while (iterator.hasNext()) {
            Entry<String, JrsValue> entry = iterator.next();

            String name = entry.getKey();
            QueryDataType type = resolveType(entry.getValue());

            MappingField field = new MappingField(name, type);
            fields.putIfAbsent(field.name(), field);
        }
        return new ArrayList<>(fields.values());
    }

    private static QueryDataType resolveType(JrsValue value) {
        if (value == null || value.isNull()) {
            return QueryDataType.OBJECT;
        } else if (value instanceof JrsBoolean) {
            return QueryDataType.BOOLEAN;
        } else if (value.isNumber()) {
            return QueryDataType.DOUBLE;
        } else if (value.isValueNode()) {
            return QueryDataType.VARCHAR;
        } else {
            return QueryDataType.OBJECT;
        }
    }
}
