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

package com.hazelcast.jet.sql.impl.connector.infoschema;

import com.hazelcast.jet.sql.impl.schema.JetTable;
import com.hazelcast.sql.impl.optimizer.PlanObjectKey;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.TableStatistics;

import java.util.List;

/**
 * A table in the {@code information_schema}.
 */
public abstract class InfoSchemaTable extends JetTable {

    private final String catalog;

    public InfoSchemaTable(
            List<TableField> fields,
            String catalog,
            String schemaName,
            String name,
            TableStatistics statistics
    ) {
        super(InfoSchemaConnector.INSTANCE, fields, schemaName, name, statistics);

        this.catalog = catalog;
    }

    String catalog() {
        return catalog;
    }

    protected abstract List<Object[]> rows();

    @Override
    public final PlanObjectKey getObjectKey() {
        // schema tables are always available and their field list does not change
        return null;
    }
}
