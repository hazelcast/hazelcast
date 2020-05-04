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

package com.hazelcast.sql.impl.schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class SchemaUtils {
    public static final String CATALOG = "hazelcast";

    /** Name of the implicit schema containing partitioned maps. */
    public static final String SCHEMA_NAME_PARTITIONED = "partitioned";

    /** Name of the implicit schema containing replicated maps. */
    public static final String SCHEMA_NAME_REPLICATED = "replicated";

    private SchemaUtils() {
        // No-op.
    }

    public static List<TableField> mergeMapFields(Map<String, TableField> keyFields, Map<String, TableField> valueFields) {
        LinkedHashMap<String, TableField> res = new LinkedHashMap<>(keyFields);

        res.putAll(valueFields);

        return new ArrayList<>(res.values());
    }

    /**
     * Prepares schema paths that will be used for search.
     *
     * @param currentSearchPaths Additional schema paths to be considered.
     * @return Schema paths to be used.
     */
    public static List<List<String>> prepareSearchPaths(
        List<List<String>> currentSearchPaths,
        List<TableResolver> tableResolvers
    ) {
        // Current search paths have the highest priority.
        List<List<String>> res = new ArrayList<>();

        if (currentSearchPaths != null) {
            res.addAll(currentSearchPaths);
        }

        // Then add paths from table resolvers.
        if (tableResolvers != null) {
            for (TableResolver tableResolver : tableResolvers) {
                List<List<String>> tableResolverSearchPaths = tableResolver.getDefaultSearchPaths();

                if (tableResolverSearchPaths != null) {
                    res.addAll(tableResolverSearchPaths);
                }
            }
        }

        // Add catalog scope.
        res.add(Collections.singletonList(com.hazelcast.sql.impl.schema.SchemaUtils.CATALOG));

        // Add top-level scope.
        res.add(Collections.emptyList());

        return res;
    }
}
