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
import com.hazelcast.jet.sql.impl.opt.physical.FullScanPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.PhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.ShouldNotExecuteRel;
import com.hazelcast.jet.sql.impl.opt.physical.SlidingWindowAggregatePhysicalRel;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Union;

import javax.annotation.Nullable;

public class WindowSizeGCDCalculator {
    static final long DEFAULT_THROTTLING_FRAME_SIZE = 10L;

    private final GCDCalculatorVisitor visitor;
    private final SlidingWindowDetector swDetector;

    public WindowSizeGCDCalculator(ExpressionEvalContext eec) {
        this.visitor = new GCDCalculatorVisitor(eec);
        this.swDetector = new SlidingWindowDetector();
    }

    public void calculate(PhysicalRel rel) {
        if (shouldRun(rel)) {
            visitor.go(rel);
        }
    }

    public long get() {
        return visitor.gcd > 0 ? visitor.gcd : DEFAULT_THROTTLING_FRAME_SIZE;
    }

    private boolean shouldRun(PhysicalRel rel) {
        swDetector.go(rel);
        return swDetector.found;
    }

    private static class GCDCalculatorVisitor extends RelVisitor {
        private final ExpressionEvalContext eec;
        private long gcd;

        GCDCalculatorVisitor(ExpressionEvalContext eec) {
            this.eec = eec;
        }

        @Override
        public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
            visit0(node, null);
        }

        private void visit0(RelNode node, @Nullable RelNode sw) {
            if (node instanceof ShouldNotExecuteRel) {
                gcd = 0L;
                return;
            }

            if (node instanceof BiRel || node instanceof Union) {
                int i = 0;
                for (RelNode child : node.getInputs()) {
                    visit0(child, sw);
                }
            } else if (node instanceof SlidingWindowAggregatePhysicalRel) {
                SlidingWindowAggregatePhysicalRel slidingWindow = (SlidingWindowAggregatePhysicalRel) node;
                long windowSize = slidingWindow.windowPolicyProvider().apply(eec).windowSize();
                gcd = gcd >= 0L ? Util.gcd(gcd, windowSize) : windowSize;

                visit0(slidingWindow.getInput(), slidingWindow);
            } else if (node instanceof SlidingWindow) {
                SlidingWindow slidingWindow = (SlidingWindow) node;
                long windowSize = slidingWindow.windowPolicyProvider().apply(eec).windowSize();
                gcd = gcd >= 0L ? Util.gcd(gcd, windowSize) : windowSize;

                visit0(slidingWindow.getInput(), slidingWindow);
            } else if (node instanceof FullScanPhysicalRel) {
                return;
            } else {
                visit0(node.getInput(0), sw);
            }
        }
    }

    private static class SlidingWindowDetector extends RelVisitor {
        boolean found;

        @Override
        public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
            if (node instanceof SlidingWindow || node instanceof SlidingWindowAggregatePhysicalRel) {
                found = true;
                return;
            }
            super.visit(node, ordinal, parent);
        }
    }
}
