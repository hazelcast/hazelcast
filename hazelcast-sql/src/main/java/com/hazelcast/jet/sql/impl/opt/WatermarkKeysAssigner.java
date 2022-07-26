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

import com.hazelcast.jet.sql.impl.opt.physical.CalcPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.FullScanPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.PhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.UnionPhysicalRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Union;
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
 * Traverse a relational tree and assign watermark keys.
 */
public class WatermarkKeysAssigner {
    private final PhysicalRel root;
    private final BottomUpWatermarkKeyAssignerVisitor visitor;
    // Note: at the moment, no need to separate watermark keys without stream-to-stream join introduced.
    private final byte keyCounter = 0;
//    private final byte[] keyCounter = {0};

    public WatermarkKeysAssigner(PhysicalRel root) {
        this.root = root;
        this.visitor = new BottomUpWatermarkKeyAssignerVisitor();
    }

    public void assignWatermarkKeys() {
        visitor.go(root);
    }

    public Map<Integer, Byte> getWatermarkedFieldsKey(RelNode node) {
        return visitor.getRelToWmKeyMapping().get(node);
    }

    /**
     * Main watermark key assigner used to propagate keys
     * from bottom to top over whole rel tree.
     */
    private class BottomUpWatermarkKeyAssignerVisitor extends RelVisitor {
        private final Map<RelNode, Map<Integer, Byte>> relToWmKeyMapping = new HashMap<>();

        BottomUpWatermarkKeyAssignerVisitor() {
        }

        public Map<RelNode, Map<Integer, Byte>> getRelToWmKeyMapping() {
            return relToWmKeyMapping;
        }

        @Override
        public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
            // front wave of recursion

            if (node instanceof FullScanPhysicalRel) {
                assert node.getInputs().isEmpty() : "FullScan is not a leaf";
                FullScanPhysicalRel scan = (FullScanPhysicalRel) node;
                int idx = scan.watermarkedColumnIndex();
                if (idx >= 0) {
                    scan.setWatermarkKey(WatermarkKeysAssigner.this.keyCounter);
                    relToWmKeyMapping.put(scan, Collections.singletonMap(idx, keyCounter));
                }
                return;
            }

            super.visit(node, ordinal, parent);

            // back wave of recursion
            if (node instanceof CalcPhysicalRel) {
                CalcPhysicalRel calc = (CalcPhysicalRel) node;
                List<RexNode> projects = calc.getProgram().expandList(calc.getProgram().getProjectList());
                Map<Integer, Byte> refByteMap = relToWmKeyMapping.get(calc.getInput());
                if (refByteMap == null) {
                    return;
                }

                Map<Integer, Byte> calcRefByteMap = new HashMap<>();
                for (RexNode rexNode : projects) {
                    if (rexNode instanceof RexInputRef) {
                        int idx = ((RexInputRef) rexNode).getIndex();
                        calcRefByteMap.put(idx, refByteMap.get(idx));
                    }
                }

                relToWmKeyMapping.put(calc, calcRefByteMap);
            } else if (node instanceof UnionPhysicalRel) {
                UnionPhysicalRel union = (UnionPhysicalRel) node;
                Set<Integer> usedColumns = new HashSet<>();
                for (RelNode input : union.getInputs()) {
                    usedColumns.addAll(relToWmKeyMapping.getOrDefault(input, Collections.emptyMap()).keySet());
                }

                // in that case we cannot use any watermarks.
                if (usedColumns.size() != 1) {
                    return;
                }

                int idx = 0;
                Map<Integer, Byte> refByteMap = relToWmKeyMapping.get(union.getInput(idx++));
                for (int i = idx; i < union.getInputs().size(); ++i) {
                    relToWmKeyMapping.put(union.getInput(i), refByteMap);
                }

                RelVisitor topDownWmKeysAssigner = new TopDownUnionWatermarkKeyAssignerVisitor(
                        union,
                        relToWmKeyMapping
                );

                topDownWmKeysAssigner.go(union);
//            } else if (node instanceof StreamToStreamJoinPhysicalRel) {
//                StreamToStreamJoinPhysicalRel join = (StreamToStreamJoinPhysicalRel) node;
//                Map<RexInputRef, Byte> leftRefByteMap = refToWmKeyMapping.get(join.getLeft());
//                if (leftRefByteMap == null) {
//                    throw QueryException.error("Left input of stream-to-stream JOIN doesn't contain watermarks");
//                }
//
//                Map<RexInputRef, Byte> rightRefByteMap = refToWmKeyMapping.get(join.getRight());
//                if (rightRefByteMap == null) {
//                    throw QueryException.error("Right input of stream-to-stream JOIN doesn't contain watermarks");
//                }
//
//                Map<RexInputRef, RexInputRef> jointToLeftInputMapping = join.jointRowToLeftInputMapping();
//                Map<RexInputRef, RexInputRef> jointToRightInputMapping = join.jointRowToRightInputMapping();
//
//                Map<RexInputRef, Byte> jointRefByteMap = new HashMap<>();
//                for (Map.Entry<RexInputRef, RexInputRef> entry : jointToLeftInputMapping.entrySet()) {
//                    if (leftRefByteMap.get(entry.getValue()) != null) {
//                        jointRefByteMap.put(entry.getKey(), leftRefByteMap.get(entry.getValue()));
//                    }
//                }
//
//                for (Map.Entry<RexInputRef, RexInputRef> entry : jointToRightInputMapping.entrySet()) {
//                    if (rightRefByteMap.get(entry.getValue()) != null) {
//                        jointRefByteMap.put(entry.getKey(), rightRefByteMap.get(entry.getValue()));
//                    }
//                }
//
//                assert leftRefByteMap.size() + rightRefByteMap.size() == jointRefByteMap.size();
//                refToWmKeyMapping.put(join, jointRefByteMap);
            } else if (node instanceof Join) {
                // Hash Join and Nested Loop Join just forward watermarks from left input.
                Join join = (Join) node;
                Map<Integer, Byte> refByteMap = relToWmKeyMapping.get(join.getLeft());
                relToWmKeyMapping.put(node, refByteMap);
            } else {
                // anything else -- just forward without any changes.
                if (!node.getInputs().isEmpty()) {
                    Map<Integer, Byte> refByteMap = relToWmKeyMapping.get(node.getInputs().iterator().next());
                    relToWmKeyMapping.put(node, refByteMap);
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
        private final Map<RelNode, Map<Integer, Byte>> refToWmKeyMapping;

        TopDownUnionWatermarkKeyAssignerVisitor(
                UnionPhysicalRel root,
                Map<RelNode, Map<Integer, Byte>> refToWmKeyMapping) {
            this.refToWmKeyMapping = refToWmKeyMapping;
        }

        @Override
        public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
            if (refToWmKeyMapping.containsKey(node)) {
                super.visit(node, ordinal, parent);
                return;
            }

            if (node instanceof FullScanPhysicalRel) {
                visit((FullScanPhysicalRel) node, parent);
//            } else if (node instanceof StreamToStreamJoinPhysicalRel) {
//                visit((StreamToStreamJoinPhysicalRel) node, ordinal, parent);
            } else if (node instanceof SlidingWindow) {
                visit((SlidingWindow) node, parent);
            }

            super.visit(node, ordinal, parent);
        }

        /**
         * Assign watermark keys to source relation.
         * Each source may have <b>only</b> watermark key assigned.
         */
        private void visit(FullScanPhysicalRel scan, @Nullable RelNode parent) {
            int idx = scan.watermarkedColumnIndex();

            byte wmKey = 0;
            Map<Integer, Byte> refByteMap = refToWmKeyMapping.get(parent);
            int scanWmIndex;
            if (refByteMap.isEmpty()) {
                // Sometimes watermarked fields may be removed by Project or Calc relations.
                // In such case, we assign a watermark key which would not
                // be used in any joins, but stream still is abele to generate watermarks.
                // TODO: replace with keyCounter[0]++
                wmKey = keyCounter;
                scanWmIndex = idx;
            } else {
                int index = refByteMap.keySet().iterator().next();
                wmKey = refByteMap.get(index);
                scanWmIndex = index;
            }

            refToWmKeyMapping.put(scan, Collections.singletonMap(scanWmIndex, wmKey));
            scan.setWatermarkKey(wmKey);
        }

        /**
         * Removes {@code window_start} and {@code window_end}
         * watermarked fields added to {@link SlidingWindow} relation.
         */
        private void visit(SlidingWindow sw, @Nullable RelNode parent) {
            if (refToWmKeyMapping.containsKey(sw)) {
                return;
            }
            Map<Integer, Byte> refByteMap = refToWmKeyMapping.get(parent);
            Map<Integer, Byte> reducedRefByteMap = new HashMap<>();
            assert refByteMap != null;

            for (Map.Entry<Integer, Byte> entry : refByteMap.entrySet()) {
                if (entry.getKey() != sw.windowStartIndex() && entry.getKey() != sw.windowEndIndex()) {
                    reducedRefByteMap.put(entry.getKey(), entry.getValue());
                }
            }

            refToWmKeyMapping.put(sw, reducedRefByteMap);
        }

        /**
         * Propagate watermark keys to JOIN children relations.
         */
//        private void visit(StreamToStreamJoinPhysicalRel join, int ordinal, @Nullable RelNode parent) {
//            Map<RexInputRef, Byte> refByteMap = refToWmKeyMapping.getOrDefault(join, refToWmKeyMapping.get(parent));
//
//            Map<RexInputRef, Byte> leftInputRefByteMap = new HashMap<>();
//            Map<RexInputRef, Byte> rightInputRefByteMap = new HashMap<>();
//            Map<RexInputRef, RexInputRef> jointToLeftMapping = join.jointRowToLeftInputMapping();
//            Map<RexInputRef, RexInputRef> jointToRightMapping = join.jointRowToRightInputMapping();
//
//            for (Map.Entry<RexInputRef, Byte> entry : refByteMap.entrySet()) {
//                if (jointToLeftMapping.containsKey(entry.getKey())) {
//                    leftInputRefByteMap.put(jointToLeftMapping.get(entry.getKey()), entry.getValue());
//                } else if (jointToRightMapping.containsKey(entry.getKey())) {
//                    rightInputRefByteMap.put(jointToRightMapping.get(entry.getKey()), entry.getValue());
//                }
//            }
//
//            refToWmKeyMapping.put(join.getLeft(), leftInputRefByteMap);
//            refToWmKeyMapping.put(join.getRight(), rightInputRefByteMap);
//        }
    }
}
