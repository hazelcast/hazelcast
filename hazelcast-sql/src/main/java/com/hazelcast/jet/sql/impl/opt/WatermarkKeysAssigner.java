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

import com.hazelcast.jet.sql.impl.opt.metadata.HazelcastRelMetadataQuery;
import com.hazelcast.jet.sql.impl.opt.physical.CalcPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.FullScanPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.PhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.StreamToStreamJoinPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.UnionPhysicalRel;
import com.hazelcast.sql.impl.QueryException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Traverse optimized physical relational tree
 * and manually assign watermark keys.
 */
public class WatermarkKeysAssigner {
    private final PhysicalRel root;
    private final BottomUpWatermarkKeyAssignerVisitor visitor;
    private final byte[] keyCounter = {0};

    public WatermarkKeysAssigner(PhysicalRel root) {
        this.root = root;
        this.visitor = new BottomUpWatermarkKeyAssignerVisitor(root);
    }

    public void assignWatermarkKeys() {
        visitor.go(root);
    }

    public Map<RexInputRef, Byte> getWatermarkedFieldsKey(RelNode node) {
        return visitor.getRefToWmKeyMapping().get(node);
    }

    /**
     * Main watermark key assigner used to propagate keys
     * from bottom to top over whole rel tree.
     */
    private class BottomUpWatermarkKeyAssignerVisitor extends RelVisitor {
        private final RexBuilder rb;
        private final HazelcastRelMetadataQuery relMetadataQuery;
        private final Map<RelNode, Map<RexInputRef, Byte>> refToWmKeyMapping = new HashMap<>();

        BottomUpWatermarkKeyAssignerVisitor(PhysicalRel root) {
            this.rb = root.getCluster().getRexBuilder();
            this.relMetadataQuery = OptUtils.metadataQuery(root);
        }

        public Map<RelNode, Map<RexInputRef, Byte>> getRefToWmKeyMapping() {
            return refToWmKeyMapping;
        }

        @Override
        public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
            // front wave of recursion

            // FullScan rel is always a leaf node in rel tree.
            if (node instanceof FullScanPhysicalRel) {
                FullScanPhysicalRel scan = (FullScanPhysicalRel) node;
                int idx = scan.watermarkedColumnIndex();
                if (idx >= 0) {
                    RelDataType type = scan.getRowType().getFieldList().get(idx).getType();
                    scan.setWatermarkKey(WatermarkKeysAssigner.this.keyCounter[0]);
                    refToWmKeyMapping.put(scan, Collections.singletonMap(rb.makeInputRef(type, idx), keyCounter[0]++));
                }
                return;
            }

            super.visit(node, ordinal, parent);

            // back wave of recursion
            if (node instanceof CalcPhysicalRel) {
                CalcPhysicalRel calc = (CalcPhysicalRel) node;
                List<RexNode> projects = calc.getProgram().expandList(calc.getProgram().getProjectList());
                Map<RexInputRef, Byte> refByteMap = refToWmKeyMapping.get(calc.getInput());
                if (refByteMap == null) {
                    return;
                }

                Map<RexInputRef, Byte> calcRefByteMap = new HashMap<>();
                for (RexNode rexNode : projects) {
                    if (rexNode instanceof RexInputRef) {
                        RexInputRef ref = (RexInputRef) rexNode;
                        if (refByteMap.containsKey(ref)) {
                            calcRefByteMap.put(ref, refByteMap.get(ref));
                        }
                    }
                }

                refToWmKeyMapping.put(calc, calcRefByteMap);
            } else if (node instanceof UnionPhysicalRel) {
                UnionPhysicalRel union = (UnionPhysicalRel) node;
                Set<RexInputRef> used = new HashSet<>();
                for (RelNode input : union.getInputs()) {
                    used.addAll(refToWmKeyMapping.getOrDefault(input, Collections.emptyMap()).keySet());
                }

                // in that case we cannot use any watermarks.
                if (used.size() != 1) {
                    return;
                }

                int idx = 0;
                Map<RexInputRef, Byte> refByteMap = refToWmKeyMapping.get(union.getInput(idx++));
                for (int i = idx; i < union.getInputs().size(); ++i) {
                    refToWmKeyMapping.put(union.getInput(i), refByteMap);
                }

                RelVisitor topDownWmKeysAssigner = new TopDownUnionWatermarkKeyAssignerVisitor(
                        union,
                        refToWmKeyMapping
                );

                topDownWmKeysAssigner.go(union);
            } else if (node instanceof StreamToStreamJoinPhysicalRel) {
                StreamToStreamJoinPhysicalRel join = (StreamToStreamJoinPhysicalRel) node;
                Map<RexInputRef, Byte> leftRefByteMap = refToWmKeyMapping.get(join.getLeft());
                if (leftRefByteMap == null) {
                    throw QueryException.error("Left input of stream-to-stream JOIN doesn't contain watermarks");
                }

                Map<RexInputRef, Byte> rightRefByteMap = refToWmKeyMapping.get(join.getRight());
                if (rightRefByteMap == null) {
                    throw QueryException.error("Right input of stream-to-stream JOIN doesn't contain watermarks");
                }

                Map<RexInputRef, RexInputRef> jointToLeftInputMapping = join.jointRowToLeftInputMapping();
                Map<RexInputRef, RexInputRef> jointToRightInputMapping = join.jointRowToRightInputMapping();

                Map<RexInputRef, Byte> jointRefByteMap = new HashMap<>();
                for (Map.Entry<RexInputRef, RexInputRef> entry : jointToLeftInputMapping.entrySet()) {
                    if (leftRefByteMap.get(entry.getValue()) != null) {
                        jointRefByteMap.put(entry.getKey(), leftRefByteMap.get(entry.getValue()));
                    }
                }

                for (Map.Entry<RexInputRef, RexInputRef> entry : jointToRightInputMapping.entrySet()) {
                    if (rightRefByteMap.get(entry.getValue()) != null) {
                        jointRefByteMap.put(entry.getKey(), rightRefByteMap.get(entry.getValue()));
                    }
                }

                assert leftRefByteMap.size() + rightRefByteMap.size() == jointRefByteMap.size();
                refToWmKeyMapping.put(join, jointRefByteMap);
            } else if (node instanceof Join) {
                // Hash Join and Nested Loop Join just forward watermarks from left input.
                Join join = (Join) node;
                Map<RexInputRef, Byte> refByteMap = refToWmKeyMapping.get(join.getLeft());
                refToWmKeyMapping.put(node, refByteMap);
            } else {
                // anything else -- just forward without any changes.
                if (!node.getInputs().isEmpty()) {
                    Map<RexInputRef, Byte> refByteMap = refToWmKeyMapping.get(node.getInputs().iterator().next());
                    refToWmKeyMapping.put(node, refByteMap);
                }
            }
        }

    }

    /**
     * Helper watermark key assigner used to propagate keys
     * from current node to the leafs of the rel tree
     * if {@link Union} rel was found by {@link BottomUpWatermarkKeyAssignerVisitor}.
     */
    private class TopDownUnionWatermarkKeyAssignerVisitor extends RelVisitor {
        private UnionPhysicalRel root;
        private Map<RelNode, Map<RexInputRef, Byte>> refToWmKeyMapping;
        private RexBuilder rexBuilder;

        TopDownUnionWatermarkKeyAssignerVisitor(
                UnionPhysicalRel root,
                Map<RelNode, Map<RexInputRef, Byte>> refToWmKeyMapping) {
            this.root = root;
            this.refToWmKeyMapping = refToWmKeyMapping;
            this.rexBuilder = root.getCluster().getRexBuilder();
        }

        @Override
        public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
            if (refToWmKeyMapping.containsKey(node)) {
                super.visit(node, ordinal, parent);
                return;
            }

            if (node instanceof FullScanPhysicalRel) {
                visit((FullScanPhysicalRel) node, ordinal, parent);
            } else if (node instanceof StreamToStreamJoinPhysicalRel) {
                visit((StreamToStreamJoinPhysicalRel) node, ordinal, parent);
            } else if (node instanceof SlidingWindow) {
                visit((SlidingWindow) node, ordinal, parent);
            }

            super.visit(node, ordinal, parent);
        }

        /**
         * Assign watermark keys to source relation.
         * Each source may have <b>only</b> watermark key assigned.
         */
        private void visit(FullScanPhysicalRel scan, int ordinal, @Nullable RelNode parent) {
            int idx = scan.watermarkedColumnIndex();

            byte wmKey = 0;
            Map<RexInputRef, Byte> refByteMap = refToWmKeyMapping.get(parent);
            RexInputRef scanWmRef;
            if (refByteMap.isEmpty()) {
                // Sometimes watermarked fields may be removed by Project or Calc relations.
                // In such case, we assign a watermark key which would not
                // be used in any joins, but stream still is abele to generate watermarks.
                wmKey = keyCounter[0]++;
                scanWmRef = rexBuilder.makeInputRef(scan.getRowType().getFieldList().get(idx).getType(), wmKey);
            } else {
                RexInputRef ref = refByteMap.keySet().iterator().next();
                assert ref != null : "Parent node doesn't have watermark key";
                wmKey = refByteMap.get(ref);
                scanWmRef = ref;
            }

            refToWmKeyMapping.put(scan, Collections.singletonMap(scanWmRef, wmKey));
            scan.setWatermarkKey(wmKey);
        }

        /**
         * Removes {@code window_start} and {@code window_end}
         * watermarked fields added to {@link SlidingWindow} relation.
         */
        private void visit(SlidingWindow sw, int ordinal, @Nullable RelNode parent) {
            if (refToWmKeyMapping.containsKey(sw)) {
                return;
            }
            Map<RexInputRef, Byte> refByteMap = refToWmKeyMapping.get(parent);
            Map<RexInputRef, Byte> reducedRefByteMap = new HashMap<>();
            assert refByteMap != null;

            for (Map.Entry<RexInputRef, Byte> entry : refByteMap.entrySet()) {
                int idx = entry.getKey().getIndex();
                if (idx != sw.windowStartIndex() && idx != sw.windowEndIndex()) {
                    reducedRefByteMap.put(rexBuilder.makeInputRef(entry.getKey().getType(), idx), entry.getValue());
                }
            }

            refToWmKeyMapping.put(sw, reducedRefByteMap);
        }

        /**
         * Propagate watermark keys to JOIN children relations.
         */
        private void visit(StreamToStreamJoinPhysicalRel join, int ordinal, @Nullable RelNode parent) {
            Map<RexInputRef, Byte> refByteMap = refToWmKeyMapping.getOrDefault(join, refToWmKeyMapping.get(parent));

            Map<RexInputRef, Byte> leftInputRefByteMap = new HashMap<>();
            Map<RexInputRef, Byte> rightInputRefByteMap = new HashMap<>();
            Map<RexInputRef, RexInputRef> jointToLeftMapping = join.jointRowToLeftInputMapping();
            Map<RexInputRef, RexInputRef> jointToRightMapping = join.jointRowToRightInputMapping();

            for (Map.Entry<RexInputRef, Byte> entry : refByteMap.entrySet()) {
                if (jointToLeftMapping.containsKey(entry.getKey())) {
                    leftInputRefByteMap.put(jointToLeftMapping.get(entry.getKey()), entry.getValue());
                } else if (jointToRightMapping.containsKey(entry.getKey())) {
                    rightInputRefByteMap.put(jointToRightMapping.get(entry.getKey()), entry.getValue());
                }
            }

            refToWmKeyMapping.put(join.getLeft(), leftInputRefByteMap);
            refToWmKeyMapping.put(join.getRight(), rightInputRefByteMap);
        }
    }
}