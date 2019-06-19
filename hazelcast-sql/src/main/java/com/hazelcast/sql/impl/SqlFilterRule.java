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

package com.hazelcast.sql.impl;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Filter;

public class SqlFilterRule extends RelOptRule {

    public static final RelOptRule INSTANCE = new SqlFilterRule();

    private SqlFilterRule() {
        super(operand(Filter.class, operand(SqlQuery.class, none())));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Filter filter = call.rel(0);
        SqlQuery query = call.rel(1);

        SqlQuery combinedQuery = query.tryToCombineWith(filter);
        if (combinedQuery != null) {
            call.transformTo(combinedQuery);
        }
    }

}
