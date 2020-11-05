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

package com.hazelcast.jet.sql.impl.schema;

import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public class MappingDefinition {

    private final Table table;
    private final String type;
    private final Map<String, String> options;

    public MappingDefinition(
            Table table,
            String type,
            Map<String, String> options
    ) {
        this.table = table;
        this.type = type;
        this.options = options;
    }

    public String schema() {
        return table.getSchemaName();
    }

    public String name() {
        return table.getSqlName();
    }

    public String type() {
        return type;
    }

    public List<TableField> fields() {
        return table.getFields();
    }

    public String options() {
        return options.entrySet().stream()
                      .sorted(Entry.comparingByKey())
                      .map(entry -> entry.getKey() + "=" + entry.getValue())
                      .collect(Collectors.joining(", ", "{", "}"));
    }
}
