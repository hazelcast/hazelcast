/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.optimizer;

import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.security.SqlSecurityContext;

import java.util.List;

/**
 * Abstraction over execution plan that allows for specialization for an execution backend.
 */
public abstract class SqlPlan {

    /** Key of the plan. */
    private final PlanKey planKey;

    /** Time when the plan was used for the last time. */
    private volatile long planLastUsed;

    protected SqlPlan(PlanKey planKey) {
        this.planKey = planKey;
    }

    public PlanKey getPlanKey() {
        return planKey;
    }

    public void onPlanUsed() {
        planLastUsed = System.currentTimeMillis();
    }

    public long getPlanLastUsed() {
        return planLastUsed;
    }

    /**
     * @return {@code true} if the plan is eligible for caching, {@code false} otherwise
     */
    public abstract boolean isCacheable();

    /**
     * @return {@code true} if the plan is valid, {@code false} otherwise
     */
    public abstract boolean isPlanValid(PlanCheckContext context);

    /**
     * Check whether the user has enough permissions to execute this plan.
     *
     * @param context security context
     */
    public abstract void checkPermissions(SqlSecurityContext context);

    /**
     * @return {@code true} if the query produces rows, {@code false} otherwise
     */
    public abstract boolean producesRows();

    public abstract SqlResult execute(QueryId queryId, List<Object> arguments, long timeout);
}
