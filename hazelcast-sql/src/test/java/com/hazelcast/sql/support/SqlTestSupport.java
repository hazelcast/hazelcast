/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.sql.impl.QueryFragment;
import com.hazelcast.sql.impl.SqlCursorImpl;
import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExtractorExpression;
import com.hazelcast.sql.impl.expression.KeyValueExtractorExpression;
import com.hazelcast.sql.impl.physical.PhysicalNodeVisitor;
import com.hazelcast.sql.support.plan.PhysicalPlanChecker;
import com.hazelcast.test.HazelcastTestSupport;
import org.apache.commons.math3.analysis.function.Exp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Common infrastructure for SQL tests.
 */
public class SqlTestSupport extends HazelcastTestSupport {
    protected SqlCursorImpl executeQuery(HazelcastInstance target, String sql) {
        SqlCursor cursor = target.getSqlService().query(sql);

        return (SqlCursorImpl) cursor;
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

    protected static void assertPlan(QueryFragment fragment, PhysicalPlanChecker checker) {
        checker.checkPlan(fragment.getNode());
    }

    protected Expression keyValueExtractorExpression(String path) {
        return new KeyValueExtractorExpression(path);
    }

    protected Expression columnExpression(int idx) {
        return new ColumnExpression(idx);
    }

    protected List<Expression> expressions(Expression... expressions) {
        return new ArrayList<>(Arrays.asList(expressions));
    }
}
