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

package com.hazelcast.jet.sql.impl.opt;

import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.sql.impl.opt.physical.PhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.SlidingWindowAggregatePhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.StreamToStreamJoinPhysicalRel;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;

import javax.annotation.Nullable;

public class WindowSizeGcdCalculator {
    static final long DEFAULT_THROTTLING_FRAME_SIZE = 100L;
    static final long MINIMUM_WATERMARK_INTERVAL = 1L;

    private final GcdCalculatorVisitor visitor;

    public WindowSizeGcdCalculator(ExpressionEvalContext eec) {
        this.visitor = new GcdCalculatorVisitor(eec);
    }

    public void calculate(PhysicalRel rel) {
        visitor.go(rel);
    }

    public long get() {
        return visitor.gcd > 0 ? visitor.gcd : DEFAULT_THROTTLING_FRAME_SIZE;
    }

    private static class GcdCalculatorVisitor extends RelVisitor {
        private final ExpressionEvalContext eec;
        private long gcd;

        GcdCalculatorVisitor(ExpressionEvalContext eec) {
            this.eec = eec;
        }

        @Override
        public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
            visit0(node);
        }

        private void visit0(RelNode node) {
            if (node instanceof SlidingWindowAggregatePhysicalRel) {
                SlidingWindowAggregatePhysicalRel slidingWindow = (SlidingWindowAggregatePhysicalRel) node;
                long windowSize = slidingWindow.windowPolicyProvider().apply(eec).windowSize();
                gcd = gcd > 0L ? Util.gcd(gcd, windowSize) : windowSize;
            } else if (node instanceof StreamToStreamJoinPhysicalRel) {
                StreamToStreamJoinPhysicalRel s2sJoin = (StreamToStreamJoinPhysicalRel) node;
                long windowSize = s2sJoin.minWindowSize();
                if (windowSize == 0L) {
                    windowSize = MINIMUM_WATERMARK_INTERVAL;
                }
                // we don't want to overflow input events buffers, that’s why gcd is limited with default frame size.
                gcd = gcd > 0L ? Util.gcd(gcd, windowSize) : Util.gcd(DEFAULT_THROTTLING_FRAME_SIZE, windowSize);
            }

            for (RelNode child : node.getInputs()) {
                visit0(child);
            }
        }
    }
}
