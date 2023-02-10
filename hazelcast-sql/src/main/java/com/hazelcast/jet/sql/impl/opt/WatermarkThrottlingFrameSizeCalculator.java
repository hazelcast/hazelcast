/*
 * Copyright 2023 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.opt;

import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.sql.impl.opt.physical.PhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.SlidingWindowAggregatePhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.StreamToStreamJoinPhysicalRel;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;

import javax.annotation.Nullable;

public final class WatermarkThrottlingFrameSizeCalculator {
    static final long S2S_JOIN_MAX_THROTTLING_INTERVAL = 100L;
    static final long PRECISION_DIVIDER = 10L;

    private WatermarkThrottlingFrameSizeCalculator() {
    }

    public static long calculate(PhysicalRel rel, ExpressionEvalContext evalContext) {
        GcdCalculatorVisitor visitor = new GcdCalculatorVisitor(evalContext);
        visitor.go(rel);

        if (visitor.gcd == 0) {
            // there's no window aggr in the rel, return the value for joins, which is already capped at some reasonable value
            return visitor.maximumIntervalForJoins;
        }
        // if there's window aggr, cap it with the maximumIntervalForJoins
        return Math.min(visitor.gcd, visitor.maximumIntervalForJoins);
    }

    private static class GcdCalculatorVisitor extends RelVisitor {
        private long gcd;
        private long maximumIntervalForJoins = S2S_JOIN_MAX_THROTTLING_INTERVAL;
        private ExpressionEvalContext eec;

        GcdCalculatorVisitor(ExpressionEvalContext evalContext) {
            this.eec = evalContext;
        }

        @Override
        public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
            visit0(node);
        }

        private void visit0(RelNode node) {
            if (node instanceof SlidingWindowAggregatePhysicalRel) {
                SlidingWindowAggregatePhysicalRel slidingWindow = (SlidingWindowAggregatePhysicalRel) node;
                long windowSize = slidingWindow.windowPolicyProvider().apply(eec).frameSize();
                gcd = gcd > 0L ? Util.gcd(gcd, windowSize) : windowSize;
            } else if (node instanceof StreamToStreamJoinPhysicalRel) {
                StreamToStreamJoinPhysicalRel s2sJoin = (StreamToStreamJoinPhysicalRel) node;
                // For stream-to-stream join we cannot precisely define the throttling frame size, because the
                // closing edge of records doesn't have a regular rhythm, as windows do. Therefore, we use
                // a hard-coded value that's a trade-off between latency and the amount of watermarks.
                // The default maximum throttling size is 100, but we reduce it to one tenth of the minimum
                // join bounds spread, if that's less.
                long suggestedInterval = s2sJoin.minimumSpread() / PRECISION_DIVIDER;
                // clamp it to between 1..100
                suggestedInterval = Math.max(Math.min(suggestedInterval, S2S_JOIN_MAX_THROTTLING_INTERVAL), 1);
                maximumIntervalForJoins = Math.min(maximumIntervalForJoins, suggestedInterval);
            }

            for (RelNode child : node.getInputs()) {
                visit0(child);
            }
        }
    }
}
