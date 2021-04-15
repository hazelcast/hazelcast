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

package com.hazelcast.sql.impl.calcite.parse;

import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.calcite.SqlBackend;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;

/**
 * Parsing result.
 */
public class QueryParseResult {

    private final SqlNode node;
    private final QueryParameterMetadata parameterMetadata;
    private final SqlValidator validator;
    private final SqlBackend sqlBackend;
    private final boolean isInfiniteRows;

    public QueryParseResult(
            SqlNode node,
            QueryParameterMetadata parameterMetadata,
            SqlValidator validator,
            SqlBackend sqlBackend,
            boolean isInfiniteRows
    ) {
        this.node = node;
        this.parameterMetadata = parameterMetadata;
        this.validator = validator;
        this.sqlBackend = sqlBackend;
        this.isInfiniteRows = isInfiniteRows;
    }

    public SqlNode getNode() {
        return node;
    }

    public QueryParameterMetadata getParameterMetadata() {
        return parameterMetadata;
    }

    public SqlValidator getValidator() {
        return validator;
    }

    public SqlBackend getSqlBackend() {
        return sqlBackend;
    }

    public boolean isInfiniteRows() {
        return isInfiniteRows;
    }
}
