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
import java.util.Set;

/**
 * A service to optimize a RelNode and execute the SqlPlan implemented by Jet.
 */
public interface JetSqlBackend {

    String SERVICE_NAME = "hz:impl:jetSqlService";

    Object tableResolver();

    Object createParserFactory();

    Object createValidator(Object catalogReader, Object typeFactory, Object conformance);

    Object createUnsupportedOperationVisitor(Object catalogReader);

    Object createConverter(
        Object viewExpander,
        Object validator,
        Object catalogReader,
        Object cluster,
        Object convertletTable,
        Object config
    );

    Object createPlan(Object parseResult, Object context);

    /**
     * Execute the SqlPlan.
     */
    SqlResult execute(SqlPlan plan, List<Object> params, long timeout, int pageSize);
}
