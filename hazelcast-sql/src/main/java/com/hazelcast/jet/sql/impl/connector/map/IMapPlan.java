/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.jet.sql.impl.JetPlan;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.optimizer.PlanCheckContext;
import com.hazelcast.sql.impl.optimizer.PlanKey;
import com.hazelcast.sql.impl.optimizer.PlanObjectKey;
import com.hazelcast.sql.impl.security.SqlSecurityContext;

import java.util.Collections;
import java.util.List;
import java.util.Set;

abstract class IMapPlan extends JetPlan {

    protected IMapPlan(PlanKey planKey) {
        super(planKey);
    }

    static class IMapDeletePlan extends IMapPlan {
        private final String mapName;
        private final Set<PlanObjectKey> objectKeys;
        private final IMapPlanExecutor planExecutor;
        private final Expression<?> condition;

        IMapDeletePlan(
                PlanKey planKey,
                PlanObjectKey objectKey,
                IMapPlanExecutor planExecutor,
                String mapName,
                Expression<?> condition
        ) {
            super(planKey);

            this.objectKeys = Collections.singleton(objectKey);
            this.planExecutor = planExecutor;
            this.mapName = mapName;
            this.condition = condition;
        }

        String mapName() {
            return mapName;
        }

        Expression<?> condition() {
            return condition;
        }

        @Override
        public boolean isCacheable() {
            return true;
        }

        @Override
        public boolean isPlanValid(PlanCheckContext context) {
            return context.isValid(objectKeys);
        }

        @Override
        public void checkPermissions(SqlSecurityContext context) {
            context.checkPermission(new MapPermission(mapName, ActionConstants.ACTION_PUT));
        }

        @Override
        public boolean producesRows() {
            return false;
        }

        @Override
        public SqlResult execute(QueryId queryId, List<Object> arguments, long timeout) {
            return planExecutor.execute(this, arguments, timeout);
        }
    }
}
