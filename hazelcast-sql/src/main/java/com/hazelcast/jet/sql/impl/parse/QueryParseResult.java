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

package com.hazelcast.jet.sql.impl.parse;

import com.hazelcast.sql.impl.QueryParameterMetadata;
import org.apache.calcite.sql.SqlNode;

/**
 * Parsing result.
 */
public class QueryParseResult {

    private final SqlNode node;
    private final QueryParameterMetadata parameterMetadata;

    public QueryParseResult(
            SqlNode node,
            QueryParameterMetadata parameterMetadata
    ) {
        this.node = node;
        this.parameterMetadata = parameterMetadata;
    }

    public SqlNode getNode() {
        return node;
    }

    public QueryParameterMetadata getParameterMetadata() {
        return parameterMetadata;
    }
}
