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

package com.hazelcast.sql.impl.optimizer;

import com.hazelcast.sql.impl.schema.SqlCatalog;
import com.hazelcast.sql.impl.security.SqlSecurityContext;

import java.util.List;

/**
 * Encapsulates the optimization task.
 */
public final class OptimizationTask {
    /** The query. */
    private final String sql;

    /** Query parameter values. */
    private final List<Object> arguments;

    /** The scopes for object lookup in addition to the default ones. */
    private final List<List<String>> searchPaths;

    /** The resolved schema. */
    private final SqlCatalog schema;

    /** SQL-specific security context. */
    private final SqlSecurityContext securityContext;

    public OptimizationTask(
            String sql,
            List<Object> arguments,
            List<List<String>> searchPaths,
            SqlCatalog schema,
            SqlSecurityContext securityContext) {
        this.sql = sql;
        this.arguments = arguments;
        this.searchPaths = searchPaths;
        this.schema = schema;
        this.securityContext = securityContext;
    }

    public String getSql() {
        return sql;
    }

    public List<Object> getArguments() {
        return arguments;
    }

    public List<List<String>> getSearchPaths() {
        return searchPaths;
    }

    public SqlCatalog getSchema() {
        return schema;
    }

    public SqlSecurityContext getSecurityContext() {
        return securityContext;
    }
}
