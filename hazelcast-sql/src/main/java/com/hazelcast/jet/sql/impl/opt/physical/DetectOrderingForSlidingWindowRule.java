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
import com.hazelcast.jet.sql.impl.opt.logical.AggregateLogicalRel;
import com.hazelcast.jet.sql.impl.opt.logical.FullScanLogicalRel;
import com.hazelcast.jet.sql.impl.opt.logical.ProjectLogicalRel;
import com.hazelcast.jet.sql.impl.opt.logical.SlidingWindowLogicalRel;
import com.hazelcast.sql.impl.QueryException;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;

import static com.hazelcast.jet.sql.impl.opt.Conventions.LOGICAL;


/**
 * Detect pattern
 * <p>
 * - Aggregate
 * <p>
 * -- SlidingWindow
 * <p>
 * --- FullScan [stream = ture, ordered = false]
 * <p>
 * to throw exception with a reason
 * `Grouping/aggregations over non-windowed, non-ordered streaming source not supported`
 */
public final class DetectOrderingForSlidingWindowRule extends RelOptRule {
    static final RelOptRule INSTANCE = new DetectOrderingForSlidingWindowRule();

    private DetectOrderingForSlidingWindowRule() {
        super(
                operandJ(
                        AggregateLogicalRel.class,
                        LOGICAL,
                        rel -> OptUtils.isUnbounded(rel) && detectOrderingPattern(rel.getInput()),
                        none()
                ),
                DetectOrderingForSlidingWindowRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        System.err.println("MATCHED");
        throw QueryException.error("Grouping/aggregations over non-windowed, non-ordered streaming source not supported");
    }

    private static boolean detectOrderingPattern(RelNode rel) {
        if (rel instanceof ProjectLogicalRel) {
            rel = ((ProjectLogicalRel) rel).getInput();
        } else if (rel instanceof RelSubset) {
            RelSubset relSubset = (RelSubset) rel;
            RelNode best = relSubset.getBest();
            if (best instanceof ProjectLogicalRel) {
                rel = ((ProjectLogicalRel) best).getInput();
            }
        }

        SlidingWindowLogicalRel windowRel = null;

        if (rel instanceof SlidingWindowLogicalRel) {
            windowRel = (SlidingWindowLogicalRel) rel;
        }

        if (rel instanceof RelSubset) {
            RelSubset relSubset = (RelSubset) rel;
            RelNode best = relSubset.getBest();
            if (best instanceof SlidingWindowLogicalRel) {
                windowRel = ((SlidingWindowLogicalRel) best);
            }
        } else {
            return false;
        }

        if (windowRel == null) {
            return false;
        }

        if (windowRel.getInput() instanceof FullScanLogicalRel) {
            FullScanLogicalRel scan = (FullScanLogicalRel) windowRel.getInput();
            return scan.eventTimePolicyProvider() == null;
        }

        if (windowRel.getInput() instanceof RelSubset) {
            RelSubset relSubset = (RelSubset) windowRel.getInput();
            RelNode best = relSubset.getBest();
            if (best instanceof FullScanLogicalRel) {
                FullScanLogicalRel scan = (FullScanLogicalRel) best;
                return scan.eventTimePolicyProvider() == null;
            }
        }
        // no more lookups.
        return false;
    }
}
