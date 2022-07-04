/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.optimizer;

import com.hazelcast.sql.impl.schema.SqlCatalog;

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

    public OptimizationTask(String sql, List<Object> arguments, List<List<String>> searchPaths, SqlCatalog schema) {
        this.sql = sql;
        this.arguments = arguments;
        this.searchPaths = searchPaths;
        this.schema = schema;
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
}
