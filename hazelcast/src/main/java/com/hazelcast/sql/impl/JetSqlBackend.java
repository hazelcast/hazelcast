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

import com.hazelcast.sql.SqlCursor;
import com.hazelcast.sql.impl.optimizer.SqlPlan;

import java.util.List;

/**
 * A service to optimize a RelNode and execute the SqlPlan implemented by Jet.
 */
public interface JetSqlBackend {

    String SERVICE_NAME = "hz:impl:jetSqlService";

    /**
     *
     * @param opTab Actual type is org.apache.calcite.sql.SqlOperatorTable
     * @param catalogReader Actual type is org.apache.calcite.sql.validate.SqlValidatorCatalogReader
     * @param typeFactory Actual type is org.apache.calcite.rel.type.RelDataTypeFactory
     * @param conformance Actual type is org.apache.calcite.sql.validate.SqlConformance
     * @return Actual returned type is com.hazelcast.sql.impl.calcite.validate.HazelcastSqlValidator
     */
    Object createValidator(Object opTab, Object catalogReader, Object typeFactory, Object conformance);

    /**
     * @param context Actual type is com.hazelcast.sql.impl.calcite.OptimizerContext
     * @param inputRel Actual type is org.apache.calcite.rel.RelNode
     */
    SqlPlan optimizeAndCreatePlan(Object context, Object inputRel);

    /**
     * Execute the SqlPlan.
     */
    SqlCursor execute(SqlPlan plan, List<Object> params, long timeout, int pageSize);
}
