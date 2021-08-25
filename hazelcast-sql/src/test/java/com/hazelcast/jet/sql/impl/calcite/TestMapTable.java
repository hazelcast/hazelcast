/*
 * Copyright 2021 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.calcite;

import com.hazelcast.sql.impl.extract.GenericQueryTargetDescriptor;
import com.hazelcast.sql.impl.optimizer.PlanObjectKey;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.TableStatistics;
import com.hazelcast.sql.impl.schema.map.AbstractMapTable;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.Arrays;
import java.util.List;

/**
 * Fake map table for testing purposes.
 */
public class TestMapTable extends AbstractMapTable {
    private TestMapTable(String schemaName, String name, List<TableField> fields, TableStatistics statistics) {
        super(
                schemaName,
                name,
                name,
                fields,
                statistics,
                GenericQueryTargetDescriptor.DEFAULT,
                GenericQueryTargetDescriptor.DEFAULT,
                null,
                null
        );
    }

    @Override
    public PlanObjectKey getObjectKey() {
        if (!isValid()) {
            return null;
        }

        return new ObjectKey(getSchemaName(), getSqlName(), getFields());
    }

    public static TestMapTable create(String schemaName, String name, TableField... fields) {
        return new TestMapTable(schemaName, name, Arrays.asList(fields), new ConstantTableStatistics(100));
    }

    public static TableField field(String name) {
        return field(name, false);
    }

    public static TableField field(String name, boolean hidden) {
        return field(name, QueryDataType.INT, hidden);
    }

    public static TableField field(String name, QueryDataType type) {
        return field(name, type, false);
    }

    public static TableField field(String name, QueryDataType type, boolean hidden) {
        return new Field(name, type, hidden);
    }

    private static class Field extends TableField {
        private Field(String name, QueryDataType type, boolean hidden) {
            super(name, type, hidden);
        }
    }

    private static class ObjectKey implements PlanObjectKey {

        private final String schemaName;
        private final String name;
        private final List<TableField> fields;

        private ObjectKey(String schemaName, String name, List<TableField> fields) {
            this.schemaName = schemaName;
            this.name = name;
            this.fields = fields;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            ObjectKey objectId = (ObjectKey) o;

            if (!schemaName.equals(objectId.schemaName)) {
                return false;
            }

            if (!name.equals(objectId.name)) {
                return false;
            }

            return fields.equals(objectId.fields);
        }

        @Override
        public int hashCode() {
            int result = schemaName.hashCode();
            result = 31 * result + name.hashCode();
            result = 31 * result + fields.hashCode();
            return result;
        }
    }
}
