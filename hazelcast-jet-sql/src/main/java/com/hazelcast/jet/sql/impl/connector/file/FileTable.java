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

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.schema.JetTable;
import com.hazelcast.sql.impl.extract.QueryTarget;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.List;
import java.util.function.Supplier;

class FileTable extends JetTable {

    private final Supplier<ProcessorMetaSupplier> processorMetaSupplierProvider;
    private final SupplierEx<QueryTarget> queryTargetSupplier;

    FileTable(
            SqlConnector sqlConnector,
            String schemaName,
            String name,
            List<TableField> fields,
            Supplier<ProcessorMetaSupplier> processorMetaSupplierProvider,
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
}
