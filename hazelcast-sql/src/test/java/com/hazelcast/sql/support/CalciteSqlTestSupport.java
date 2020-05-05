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
import com.hazelcast.sql.impl.parser.DqlStatement;
import com.hazelcast.sql.impl.parser.SqlParseTask;
import com.hazelcast.sql.impl.plan.Plan;
import com.hazelcast.sql.impl.schema.Catalog;
import com.hazelcast.sql.impl.schema.Table;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Common infrastructure for SQL tests.
 */
public class CalciteSqlTestSupport extends SqlTestSupport {

    protected Plan getPlan(HazelcastInstance target, String sql) {
        SqlServiceImpl sqlService = (SqlServiceImpl) target.getSqlService();
        DqlStatement operation = (DqlStatement) sqlService.getParser().parse(
                new SqlParseTask.Builder(sql, new Catalog(null) {
                    @Override
                    public Collection<Table> getTables() {
                        return Collections.emptyList();
                    }
                }).build()
        );
        return operation.getPlan();
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

    protected void executeUpdate(HazelcastInstance target, String sql) {
        target.getSqlService().update(sql);
    }
}
