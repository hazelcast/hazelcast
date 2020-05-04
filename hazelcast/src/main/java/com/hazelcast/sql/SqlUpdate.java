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
 * Definition of the SQL update.
 */
public class SqlUpdate {

    /**
     * SQL update.
     */
    private String sql;

    public SqlUpdate() {
        // No-op.
    }

    public SqlUpdate(String sql) {
        setSql(sql);
    }

    public String getSql() {
        return sql;
    }

    public SqlUpdate setSql(String sql) {
        if (sql == null || sql.length() == 0) {
            throw new IllegalArgumentException("SQL cannot be null or empty.");
        }

        this.sql = sql;

        return this;
    }

    public SqlUpdate copy() {
        return new SqlUpdate(sql);
    }
}
