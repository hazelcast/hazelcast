package com.hazelcast.jet.sql.impl.opt;

import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.sql.impl.opt.physical.FullScanPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.PhysicalRel;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Union;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

public class WindowGCDCalculator {
    private final SlidingWindowDetector visitor;

    public WindowGCDCalculator(ExpressionEvalContext eec) {
        this.visitor = new SlidingWindowDetector(eec);
    }

    public void calculate(PhysicalRel rel) {
        visitor.go(rel);
    }

    public Long get(FullScanPhysicalRel scan) {
        return visitor.gcdMap.get(scan);
    }

    private static class SlidingWindowDetector extends RelVisitor {
        private final ExpressionEvalContext eec;
        private final Map<SlidingWindow, Long> swGcdMap = new HashMap<>();

        final Map<FullScanPhysicalRel, Long> gcdMap = new HashMap<>();

        public SlidingWindowDetector(ExpressionEvalContext eec) {
            this.eec = eec;
        }

        @Override
        public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
            visit0(node, null);
        }

        private void visit0(RelNode node, @Nullable SlidingWindow sw) {
            if (node == null) {
                return;
            }

            if (node instanceof BiRel || node instanceof Union) {
                for (RelNode child : node.getInputs()) {
                    visit0(child, sw);
                }
            } else if (node instanceof SlidingWindow) {
                SlidingWindow slidingWindow = (SlidingWindow) node;
                long windowSize = slidingWindow.windowPolicyProvider().apply(eec).windowSize();
                swGcdMap.put(slidingWindow, sw == null ? windowSize : Util.gcd(swGcdMap.get(sw), windowSize));
                visit0(slidingWindow.getInput(), sw == null ? slidingWindow : sw);
            } else if (node instanceof FullScanPhysicalRel) {
                gcdMap.put((FullScanPhysicalRel) node, swGcdMap.get(sw));
            } else {
                visit0(node.getInput(0), sw);
            }
        }
    }
}
