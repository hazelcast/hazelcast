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

package com.hazelcast.sql.impl.parser;

import com.hazelcast.sql.SqlCursor;
import com.hazelcast.sql.impl.SqlCursorImpl;
import com.hazelcast.sql.impl.SqlInternalService;
import com.hazelcast.sql.impl.plan.Plan;
import com.hazelcast.sql.impl.state.QueryState;

import java.util.List;

/**
 * DQL statement.
 */
public class DqlStatement implements Statement {

    private final Plan plan;

    public DqlStatement(Plan plan) {
        this.plan = plan;
    }

    public SqlCursor explain(SqlInternalService internalService) {
        QueryState state = internalService.executeExplain(plan);
        return new SqlCursorImpl(state);
    }

    public SqlCursor execute(SqlInternalService internalService, List<Object> params, long timeout, int pageSize) {
        QueryState state = internalService.execute(plan, params, timeout, pageSize);
        return new SqlCursorImpl(state);
    }

    public Plan getPlan() {
        return plan;
    }
}
