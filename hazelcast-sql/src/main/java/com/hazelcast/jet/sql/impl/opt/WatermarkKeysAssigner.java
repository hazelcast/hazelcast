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

    private static class BottomUpWatermarkKeyAssignerVisitor extends RelVisitor {
        private final RexBuilder rb;
        private final HazelcastRelMetadataQuery relMetadataQuery;
        private final byte[] keyCounter = {0};
        private final Map<RelNode, Map<RexInputRef, Byte>> refToWmKeyMapping = new HashMap<>();

        BottomUpWatermarkKeyAssignerVisitor(PhysicalRel root) {
            this.rb = root.getCluster().getRexBuilder();
            this.relMetadataQuery = OptUtils.metadataQuery(root);
//            this.leafRels = collectLeafNodes(root);
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
                    scan.setWatermarkKey(keyCounter[0]);
                    refToWmKeyMapping.put(scan, Collections.singletonMap(rb.makeInputRef(type, idx), keyCounter[0]++));
                }
                return;
            }

            super.visit(node, ordinal, parent);

            // back wave of recursion
            if (node instanceof CalcPhysicalRel) {
                CalcPhysicalRel calc = (CalcPhysicalRel) node;
                List<RexNode> projects = calc.getProgram().expandList(calc.getProgram().getProjectList());
                System.err.println();
            } else if (node instanceof UnionPhysicalRel) {
                UnionPhysicalRel union = (UnionPhysicalRel) node;
                Set<RexInputRef> used = new HashSet<>();
                for (RelNode input : union.getInputs()) {
                    used.addAll(refToWmKeyMapping.get(input).keySet());
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
                // spread equal watermark keys on whole Union branch.
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
            } else {
                // anything else -- just forward without any changes.
                if (!node.getInputs().isEmpty()) {
                    Map<RexInputRef, Byte> refByteMap = refToWmKeyMapping.get(node.getInputs().iterator().next());
                    refToWmKeyMapping.put(node, refByteMap);
                }
            }
        }

        private Set<FullScanPhysicalRel> collectLeafNodes(PhysicalRel root) {
            final Set<FullScanPhysicalRel> leafNodes = new HashSet<>();
            RelVisitor visitor = new RelVisitor() {
                @Override
                public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
                    if (node instanceof FullScanPhysicalRel) {
                        FullScanPhysicalRel scan = (FullScanPhysicalRel) node;
                        int wmColIndex = scan.watermarkedColumnIndex();
                        if (wmColIndex >= 0) {
                            leafNodes.add(scan);
                        }
                    }
                    super.visit(node, ordinal, parent);
                }
            };
            visitor.go(root);
            return leafNodes;
        }

        // TODO: to implement Union watermark keys back-propagation.
        private void spreadWatermarkKeyForUnion(UnionPhysicalRel root) {
            RelVisitor visitor = new RelVisitor() {
                @Override
                public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
                    super.visit(node, ordinal, parent);
                }
            };
            visitor.go(root);
        }
    }
}