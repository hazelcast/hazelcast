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

import com.hazelcast.core.DistributedObject;

/**
 * Schema resolver which attempts to get data from
 */
public class ChainedSqlSchemaResolver implements SqlSchemaResolver {
    /** Available resolvers. */
    private final SqlSchemaResolver[] resolvers;

    public ChainedSqlSchemaResolver(SqlSchemaResolver... resolvers) {
        this.resolvers = resolvers;
    }

    @Override
    public SqlTableSchema resolve(DistributedObject object) {
        for (SqlSchemaResolver resolver : resolvers) {
            SqlTableSchema tableSchema = resolver.resolve(object);

            if (tableSchema != null) {
                return tableSchema;
            }
        }

        return null;
    }
}
