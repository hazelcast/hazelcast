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

package com.hazelcast.sql.support;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.impl.SqlTestSupport;

import java.util.ArrayList;
import java.util.List;

/**
 * Common infrastructure for SQL tests.
 */
public class CalciteSqlTestSupport extends SqlTestSupport {

    protected SqlResult executeQuery(HazelcastInstance target, String sql) {
        return target.getSql().query(sql);
    }

    protected List<SqlRow> getQueryRows(SqlResult result) {
        List<SqlRow> rows = new ArrayList<>();

        for (SqlRow row : result) {
            rows.add(row);
        }

        return rows;
    }

    protected List<SqlRow> getQueryRows(HazelcastInstance target, String sql) {
        try (SqlResult result = executeQuery(target, sql)) {
            return getQueryRows(result);
        } catch (Exception e) {
            throw new RuntimeException("Failed to execute query and get result set rows: " + sql, e);
        }
    }
}
