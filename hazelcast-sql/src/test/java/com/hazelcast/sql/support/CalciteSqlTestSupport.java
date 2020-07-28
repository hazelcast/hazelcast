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
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.impl.QueryUtils;
import com.hazelcast.sql.impl.SqlResultImpl;
import com.hazelcast.sql.impl.SqlServiceImpl;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.impl.optimizer.OptimizationTask;
import com.hazelcast.sql.impl.optimizer.SqlPlan;
import com.hazelcast.sql.impl.schema.SqlCatalog;
import com.hazelcast.sql.impl.schema.TableResolver;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTableResolver;
import com.hazelcast.sql.impl.schema.map.ReplicatedMapTableResolver;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.Collections.emptyList;

/**
 * Common infrastructure for SQL tests.
 */
public class CalciteSqlTestSupport extends SqlTestSupport {
    @SuppressWarnings("unchecked")
    protected <T extends SqlPlan> T getPlan(HazelcastInstance target, String sql) {
        NodeEngine nodeEngine = nodeEngine(target);

        List<TableResolver> tableResolvers = Arrays.asList(
            new PartitionedMapTableResolver(nodeEngine),
            new ReplicatedMapTableResolver(nodeEngine)
        );

        List<List<String>> searchPaths = QueryUtils.prepareSearchPaths(emptyList(), tableResolvers);
        SqlCatalog schema = new SqlCatalog(tableResolvers);

        SqlServiceImpl sqlService = (SqlServiceImpl) target.getSql();

        return (T) sqlService.getOptimizer().prepare(new OptimizationTask(sql, searchPaths, schema));
    }

    protected SqlResult executeQuery(HazelcastInstance target, String sql) {
        return target.getSql().query(sql);
    }

    protected SqlResultImpl executeQueryEx(HazelcastInstance target, String sql) {
        return (SqlResultImpl) executeQuery(target, sql);
    }

    protected List<SqlRow> getQueryRows(SqlResult cursor) {
        List<SqlRow> rows = new ArrayList<>();

        for (SqlRow row : cursor) {
            rows.add(row);
        }

        return rows;
    }

    protected List<SqlRow> getQueryRows(HazelcastInstance target, String sql) {
        try (SqlResult cursor = executeQuery(target, sql)) {
            return getQueryRows(cursor);
        } catch (Exception e) {
            throw new RuntimeException("Failed to execute query and get result set rows: " + sql, e);
        }
    }
}
