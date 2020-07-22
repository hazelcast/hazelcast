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

package com.hazelcast.sql.impl;

import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.impl.optimizer.SqlPlan;

import java.util.List;
import java.util.function.BiFunction;

/**
 * A service to optimize a RelNode and execute the SqlPlan implemented by Jet.
 */
public interface JetSqlBackend {

    String SERVICE_NAME = "hz:impl:jetSqlService";

    /**
     * @return Actual returned type is org.apache.calcite.sql.SqlOperatorTable
     */
    Object operatorTable();

    /**
     * @return Actual returned type is:<br>
     * BiFunction&lt;<br>
     * org.apache.calcite.prepare.Prepare.CatalogReader,<br>
     * org.apache.calcite.sql.SqlNode,<br>
     * org.apache.calcite.sql.SqlNode<br>
     * &gt;
     */
    BiFunction<Object, Object, Object> validator();

    /**
     * @param context  Actual type is com.hazelcast.sql.impl.calcite.OptimizerContext
     * @param inputRel Actual type is org.apache.calcite.rel.RelNode
     */
    SqlPlan optimizeAndCreatePlan(Object context, Object inputRel, List<String> rootColumnNames);

    /**
     * Execute the SqlPlan.
     */
    SqlResult execute(SqlPlan plan, List<Object> params, long timeout, int pageSize);
}
