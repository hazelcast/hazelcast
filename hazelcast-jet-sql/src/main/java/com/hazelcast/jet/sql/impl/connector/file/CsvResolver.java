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

package com.hazelcast.jet.sql.impl.connector.file;

import com.hazelcast.jet.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

final class CsvResolver {

    private CsvResolver() {
    }

    static List<MappingField> resolveFields(Set<String> header) {
        Map<String, MappingField> fields = new LinkedHashMap<>();
        for (String name : header) {
            MappingField field = new MappingField(name, QueryDataType.VARCHAR);
            fields.putIfAbsent(field.name(), field);
        }
        return new ArrayList<>(fields.values());
    }
}
