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

package com.hazelcast.sql;

import javax.annotation.Nonnull;

/**
 * Service to execute SQL queries.
 */
public interface SqlService {
    /**
     * Convenient method to execute a query with the given parameters.
     * <p>
     * Converts passed SQL string and parameters into an {@link SqlQuery} object and invokes {@link #query(SqlQuery)}.
     *
     * @see SqlQuery
     * @see #query(SqlQuery)
     * @param sql SQL.
     * @param params Parameters.
     * @return Result.
     * @throws NullPointerException If sql is null
     * @throws IllegalArgumentException If sql is empty
     * @throws SqlException In case of execution error
     */
    @Nonnull
    default SqlResult query(@Nonnull String sql, Object... params) {
        SqlQuery query = new SqlQuery(sql);

        if (params != null) {
            for (Object param : params) {
                query.addParameter(param);
            }
        }

        return query(query);
    }

    /**
     * Executes query.
     *
     * @param query Query.
     * @return Result.
     * @throws NullPointerException If query is null
     * @throws SqlException In case of execution error
     */
    @Nonnull
    SqlResult query(@Nonnull SqlQuery query);
}
