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

package com.hazelcast.sql.impl.calcite;

import com.hazelcast.sql.impl.extract.GenericQueryTargetDescriptor;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.TableStatistics;
import com.hazelcast.sql.impl.schema.map.AbstractMapTable;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Fake map table for testing purposes.
 */
public class TestMapTable extends AbstractMapTable {
    private TestMapTable(String schemaName, String name, List<TableField> fields, TableStatistics statistics) {
        super(
            schemaName,
            name,
            fields,
            statistics,
            GenericQueryTargetDescriptor.INSTANCE,
            GenericQueryTargetDescriptor.INSTANCE, Collections.emptyMap())
        ;
    }

    public static TestMapTable create(String schemaName, String name, TableField... fields) {
        return new TestMapTable(schemaName, name, Arrays.asList(fields), new ConstantTableStatistics(100));
    }

    public static TableField field(String name) {
        return field(name, QueryDataType.INT);
    }

    public static TableField field(String name, QueryDataType type) {
        return new Field(name, type);
    }

    private static class Field extends TableField {
        private Field(String name, QueryDataType type) {
            super(name, type);
        }
    }
}
