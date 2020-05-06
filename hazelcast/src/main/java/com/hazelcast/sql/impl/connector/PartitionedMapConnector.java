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

import com.hazelcast.sql.impl.extract.GenericQueryTargetDescriptor;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.TableSchema.Field;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTable;

import java.util.List;
import java.util.Map;

import static com.hazelcast.sql.impl.schema.map.PartitionedMapTable.DISTRIBUTION_FIELD_ORDINAL_NONE;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

// TODO: do we want to keep it? maps are auto discovered...
public class PartitionedMapConnector implements SqlConnector {

    @Override
    public Table createTable(String schemaName,
                             String name,
                             List<Field> fields,
                             Map<String, String> options) {
        return new PartitionedMapTable(schemaName, name, toMapFields(fields), new ConstantTableStatistics(0),
                new GenericQueryTargetDescriptor(), new GenericQueryTargetDescriptor(), emptyList(), DISTRIBUTION_FIELD_ORDINAL_NONE); // TODO: ???
    }

    private static List<TableField> toMapFields(List<Field> fields) {
        return fields.stream()
                     .map(field -> new MapTableField(field.name(), field.type(), QueryPath.create(field.name()))) // TODO: ???
                     .collect(toList());
    }
}
