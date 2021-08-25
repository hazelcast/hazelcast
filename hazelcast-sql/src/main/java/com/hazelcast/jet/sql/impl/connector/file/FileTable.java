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

package com.hazelcast.jet.sql.impl.connector.file;

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.schema.JetTable;
import com.hazelcast.sql.impl.extract.QueryTarget;
import com.hazelcast.sql.impl.optimizer.PlanObjectKey;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.List;
import java.util.Objects;

abstract class FileTable extends JetTable {

    protected final ProcessorMetaSupplierProvider processorMetaSupplierProvider;
    private final SupplierEx<QueryTarget> queryTargetSupplier;

    protected FileTable(
            SqlConnector sqlConnector,
            String schemaName,
            String name,
            List<TableField> fields,
            ProcessorMetaSupplierProvider processorMetaSupplierProvider,
            SupplierEx<QueryTarget> queryTargetSupplier
    ) {
        super(sqlConnector, fields, schemaName, name, new ConstantTableStatistics(0));

        this.processorMetaSupplierProvider = processorMetaSupplierProvider;
        this.queryTargetSupplier = queryTargetSupplier;
    }

    ProcessorMetaSupplier processorMetaSupplier() {
        return processorMetaSupplierProvider.get();
    }

    SupplierEx<QueryTarget> queryTargetSupplier() {
        return queryTargetSupplier;
    }

    String[] paths() {
        return getFields().stream().map(field -> ((FileTableField) field).getPath()).toArray(String[]::new);
    }

    QueryDataType[] types() {
        return getFields().stream().map(TableField::getType).toArray(QueryDataType[]::new);
    }

    /**
     * A table with deterministic schema.
     * Plans containing it can be cached.
     */
    static class SpecificFileTable extends FileTable {

        SpecificFileTable(
                SqlConnector sqlConnector,
                String schemaName,
                String name,
                List<TableField> fields,
                ProcessorMetaSupplierProvider processorMetaSupplierProvider,
                SupplierEx<QueryTarget> queryTargetSupplier
        ) {
            super(sqlConnector, schemaName, name, fields, processorMetaSupplierProvider, queryTargetSupplier);
        }

        @Override
        public PlanObjectKey getObjectKey() {
            return new FilePlanObjectKey(getSchemaName(), getSqlName(), getFields(), processorMetaSupplierProvider);
        }
    }

    /**
     * A table with non-deterministic schema (i.e. result of a file table function).
     * Plans containing it must/can not be cached.
     */
    static class DynamicFileTable extends FileTable {

        DynamicFileTable(
                SqlConnector sqlConnector,
                String schemaName,
                String name,
                List<TableField> fields,
                ProcessorMetaSupplierProvider processorMetaSupplierProvider,
                SupplierEx<QueryTarget> queryTargetSupplier
        ) {
            super(sqlConnector, schemaName, name, fields, processorMetaSupplierProvider, queryTargetSupplier);
        }

        @Override
        public PlanObjectKey getObjectKey() {
            return PlanObjectKey.NON_CACHEABLE_OBJECT_KEY;
        }
    }

    static final class FilePlanObjectKey implements PlanObjectKey {

        private final String schemaName;
        private final String name;
        private final List<TableField> fields;
        private final ProcessorMetaSupplierProvider processorMetaSupplierProvider;

        FilePlanObjectKey(
                String schemaName,
                String name,
                List<TableField> fields,
                ProcessorMetaSupplierProvider processorMetaSupplierProvider
        ) {
            this.schemaName = schemaName;
            this.name = name;
            this.fields = fields;
            this.processorMetaSupplierProvider = processorMetaSupplierProvider;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FilePlanObjectKey that = (FilePlanObjectKey) o;
            return Objects.equals(schemaName, that.schemaName)
                    && Objects.equals(name, that.name)
                    && Objects.equals(fields, that.fields)
                    && Objects.equals(processorMetaSupplierProvider, that.processorMetaSupplierProvider);
        }

        @Override
        public int hashCode() {
            return Objects.hash(schemaName, name, fields, processorMetaSupplierProvider);
        }
    }
}
