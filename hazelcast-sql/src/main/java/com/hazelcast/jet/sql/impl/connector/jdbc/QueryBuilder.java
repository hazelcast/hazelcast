/*
 * Copyright 2023 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.connector.jdbc;

import com.hazelcast.internal.util.StringUtil;
import org.apache.calcite.sql.SqlDialect;

public final class QueryBuilder {

    private QueryBuilder() {
    }

    /**
     * Used by SQL builders
     *
     * @param jdbcTable specifies the table to be used
     * @return If schema name of the JdbcTable is specified returns quoted schema name + "."
     * otherwise returns empty string
     */
    public static String quoteSchemaName(JdbcTable jdbcTable) {
        String quotedSchemaName;
        String externalSchemaName = jdbcTable.getExternalSchemaName();
        if (!StringUtil.isNullOrEmpty(externalSchemaName)) {
            SqlDialect sqlDialect = jdbcTable.sqlDialect();
            quotedSchemaName = sqlDialect.quoteIdentifier(externalSchemaName) + ".";
        } else {
            quotedSchemaName = "";
        }
        return quotedSchemaName;
    }
}
