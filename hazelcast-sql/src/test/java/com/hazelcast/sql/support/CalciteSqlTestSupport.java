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
import com.hazelcast.sql.SqlCursor;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.impl.SqlCursorImpl;
import com.hazelcast.sql.impl.SqlServiceImpl;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.impl.optimizer.OptimizationTask;
import com.hazelcast.sql.impl.optimizer.SqlPlan;

import java.util.ArrayList;
import java.util.List;

/**
 * Common infrastructure for SQL tests.
 */
public class CalciteSqlTestSupport extends SqlTestSupport {
    @SuppressWarnings("unchecked")
    protected <T extends SqlPlan> T getPlan(HazelcastInstance target, String sql) {
        SqlServiceImpl sqlService = (SqlServiceImpl) target.getSqlService();

        return (T) sqlService.getOptimizer().prepare(new OptimizationTask.Builder(sql).build());
    }

    protected SqlCursor executeQuery(HazelcastInstance target, String sql) {
        return target.getSqlService().query(sql);
    }

    protected SqlCursorImpl executeQueryEx(HazelcastInstance target, String sql) {
        return (SqlCursorImpl) executeQuery(target, sql);
    }

    protected List<SqlRow> getQueryRows(SqlCursor cursor) {
        List<SqlRow> rows = new ArrayList<>();

        for (SqlRow row : cursor) {
            rows.add(row);
        }

        return rows;
    }

    protected List<SqlRow> getQueryRows(HazelcastInstance target, String sql) {
        try (SqlCursor cursor = executeQuery(target, sql)) {
            return getQueryRows(cursor);
        } catch (Exception e) {
            throw new RuntimeException("Failed to execute query and get result set rows: " + sql, e);
        }
    }
}
