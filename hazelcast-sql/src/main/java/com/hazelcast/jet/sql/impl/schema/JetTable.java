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

package com.hazelcast.jet.sql.impl.schema;

import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.sql.impl.QueryUtils;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.TableStatistics;

import javax.annotation.Nonnull;
import java.util.List;

import static java.util.Arrays.asList;

public abstract class JetTable extends Table {

    private final SqlConnector sqlConnector;

    public JetTable(
            @Nonnull SqlConnector sqlConnector,
            @Nonnull List<TableField> fields,
            @Nonnull String schemaName,
            @Nonnull String name,
            @Nonnull TableStatistics statistics
    ) {
        super(schemaName, name, fields, statistics);
        this.sqlConnector = sqlConnector;
    }

    public final List<String> getQualifiedName() {
        return asList(QueryUtils.CATALOG, getSchemaName(), getSqlName());
    }

    public final SqlConnector getSqlConnector() {
        return sqlConnector;
    }

    @Override
    public String toString() {
        return getSqlConnector().typeName() + "[" + getSchemaName() + "." + getSqlName() + ']';
    }
}
