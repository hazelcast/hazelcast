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

/**
 * Service to execute SQL queries.
 */
public interface SqlService {
    /** Unique service name. */
    String SERVICE_NAME = "hz:impl:sqlService";

    /**
     * Execute query.
     *
     * @param sql SQL.
     * @param params Parameters.
     * @return Cursor.
     */
    default SqlResult query(String sql, Object... params) {
        SqlQuery query = new SqlQuery(sql);

        if (params != null) {
            for (Object param : params) {
                query.addParameter(param);
            }
        }

        return query(query);
    }

    /**
     * Execute query.
     *
     * @param query Query.
     * @return Cursor.
     */
    SqlResult query(SqlQuery query);
}
