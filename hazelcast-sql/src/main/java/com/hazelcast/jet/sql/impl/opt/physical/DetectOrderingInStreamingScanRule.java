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

package com.hazelcast.jet.sql.impl.opt.physical;

import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.sql.impl.QueryException;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Sort;

import static com.hazelcast.jet.sql.impl.opt.Conventions.LOGICAL;

public final class DetectOrderingInStreamingScanRule extends RelOptRule {
    static final RelOptRule INSTANCE = new DetectOrderingInStreamingScanRule();

    private DetectOrderingInStreamingScanRule() {
        super(
                operandJ(
                        Sort.class,
                        LOGICAL,
                        sort -> !isEmptyCollation(sort) && OptUtils.isUnbounded(sort.getInput()),
                        none()
                ),
                DetectOrderingInStreamingScanRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        System.err.println("MATCHED");
        throw QueryException.error("Sorting is not supported for a streaming query");
    }

    private static boolean isEmptyCollation(Sort sort) {
        return sort.collation.getFieldCollations().size() == 0;
    }
}
