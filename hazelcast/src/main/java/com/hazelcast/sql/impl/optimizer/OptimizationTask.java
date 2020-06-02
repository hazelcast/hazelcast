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

package com.hazelcast.sql.impl.optimizer;

import java.util.ArrayList;
import java.util.List;

/**
 * Encapsulates the optimization task.
 */
public final class OptimizationTask {
    /** The query. */
    private final String sql;

    /** The scopes for object lookup in addition to the default ones. */
    private final List<List<String>> searchPaths;

    private OptimizationTask(String sql, List<List<String>> searchPaths) {
        this.sql = sql;
        this.searchPaths = searchPaths;
    }

    public String getSql() {
        return sql;
    }

    public List<List<String>> getSearchPaths() {
        return searchPaths;
    }

    public static class Builder {

        private final String sql;
        private List<List<String>> searchPaths;

        public Builder(String sql) {
            this.sql = sql;
        }

        public Builder addSchemaPath(List<String> searchPath) {
            if (searchPaths == null) {
                searchPaths = new ArrayList<>(1);
            }

            searchPaths.add(searchPath);

            return this;
        }

        public OptimizationTask build() {
            return new OptimizationTask(sql, searchPaths);
        }
    }
}
