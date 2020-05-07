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

package com.hazelcast.sql.impl.calcite.parse;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;

/**
 * Parsing result.
 */
public class QueryParseResult {

    private final String sql;
    private final SqlNode node;
    private final RelDataType parameterRowType;

    public QueryParseResult(String sql, SqlNode node, RelDataType parameterRowType) {
        this.sql = sql;
        this.node = node;
        this.parameterRowType = parameterRowType;
    }

    public boolean isDdl() {
        return node.getKind().belongsTo(SqlKind.DDL);
    }

    public String getSql() {
        return sql;
    }

    public SqlNode getNode() {
        return node;
    }

    public RelDataType getParameterRowType() {
        return parameterRowType;
    }
}
