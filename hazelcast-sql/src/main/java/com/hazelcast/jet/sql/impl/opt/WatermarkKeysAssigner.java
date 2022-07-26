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
import com.hazelcast.jet.sql.impl.opt.utils.MutableByte;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyMap;

/**
 * Traverse a relational tree and assign watermark keys.
 */
public class WatermarkKeysAssigner {
    private final PhysicalRel root;
    private final BottomUpWatermarkKeyAssignerVisitor visitor;
    // Note: at the moment, no need to separate watermark keys without stream-to-stream join introduction.
    private final byte keyCounter = 0;
//    private final byte[] keyCounter = {0};

    public WatermarkKeysAssigner(PhysicalRel root) {
        this.root = root;
        this.visitor = new BottomUpWatermarkKeyAssignerVisitor();
    }

    public void assignWatermarkKeys() {
        visitor.go(root);
    }

    public Map<Integer, MutableByte> getWatermarkedFieldsKey(RelNode node) {
        return visitor.getRelToWmKeyMapping().get(node);
    }

    /**
     * Main watermark key assigner used to propagate keys
     * from bottom to top over whole rel tree.
     */
    private class BottomUpWatermarkKeyAssignerVisitor extends RelVisitor {
        private final Map<RelNode, Map<Integer, MutableByte>> relToWmKeyMapping = new HashMap<>();

        BottomUpWatermarkKeyAssignerVisitor() {
        }

        public Map<RelNode, Map<Integer, MutableByte>> getRelToWmKeyMapping() {
            return relToWmKeyMapping;
        }

        @Override
        public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
            // front wave of recursion

            if (node instanceof FullScanPhysicalRel) {
                assert node.getInputs().isEmpty() : "FullScan not a leaf";
                FullScanPhysicalRel scan = (FullScanPhysicalRel) node;
                int idx = scan.watermarkedColumnIndex();
                if (idx >= 0) {
                    scan.setWatermarkKey(WatermarkKeysAssigner.this.keyCounter);
                    relToWmKeyMapping.put(scan, Collections.singletonMap(idx, new MutableByte(keyCounter)));
                }
                return;
            }

            super.visit(node, ordinal, parent);

            // back wave of recursion
            if (node instanceof CalcPhysicalRel) {
                CalcPhysicalRel calc = (CalcPhysicalRel) node;
                List<RexNode> projects = calc.getProgram().expandList(calc.getProgram().getProjectList());
                Map<Integer, MutableByte> refByteMap = relToWmKeyMapping.get(calc.getInput());
                if (refByteMap == null) {
                    return;
                }

                int projectIndex = 0;
                Map<Integer, MutableByte> calcRefByteMap = new HashMap<>();
                for (RexNode rexNode : projects) {
                    if (rexNode instanceof RexInputRef) {
                        int idx = ((RexInputRef) rexNode).getIndex();
                        MutableByte wmKey = refByteMap.get(idx);
                        if (wmKey != null) {
                            calcRefByteMap.put(projectIndex++, wmKey);
                        }
                    }
                }

                relToWmKeyMapping.put(calc, calcRefByteMap);
            } else if (node instanceof UnionPhysicalRel) {
                // Note: here, we want to find intersection of all input watermarked fields,
                //  and assign same keys for intersected items.
                // TODO: test it.
                UnionPhysicalRel union = (UnionPhysicalRel) node;
                assert !union.getInputs().isEmpty();

                // Collect intersection of watermarked fields from all union inputs.
                Iterator<RelNode> it = union.getInputs().iterator();
                Set<Integer> commonWmIdx = new HashSet<>(relToWmKeyMapping.getOrDefault(it.next(), emptyMap()).keySet());
                while (it.hasNext()) {
                    commonWmIdx.retainAll(relToWmKeyMapping.getOrDefault(it.next(), emptyMap()).keySet());
                }

                // Get a reference keyed wm map from first input
                Map<Integer, MutableByte> byteMap = relToWmKeyMapping.getOrDefault(union.getInputs().iterator().next(), emptyMap());
                if (byteMap.isEmpty()) {
                    return;
                }

                // Assign a new byte value for all referenced bytes
                it = union.getInputs().iterator();
                while (it.hasNext()) {
                    for (Integer idx: commonWmIdx) {
                        relToWmKeyMapping.get(it.next()).get(idx).setValue(byteMap.get(idx).getValue());
                    }
                }
//            } else if (node instanceof StreamToStreamJoinPhysicalRel) {
//                StreamToStreamJoinPhysicalRel join = (StreamToStreamJoinPhysicalRel) node;
//                Map<RexInputRef, MutableByte> leftRefByteMap = refToWmKeyMapping.get(join.getLeft());
//                if (leftRefByteMap == null) {
//                    throw QueryException.error("Left input of stream-to-stream JOIN doesn't contain watermarks");
//                }
//
//                Map<RexInputRef, MutableByte> rightRefByteMap = refToWmKeyMapping.get(join.getRight());
//                if (rightRefByteMap == null) {
//                    throw QueryException.error("Right input of stream-to-stream JOIN doesn't contain watermarks");
//                }
//
//                Map<RexInputRef, RexInputRef> jointToLeftInputMapping = join.jointRowToLeftInputMapping();
//                Map<RexInputRef, RexInputRef> jointToRightInputMapping = join.jointRowToRightInputMapping();
//
//                Map<RexInputRef, MutableByte> jointRefByteMap = new HashMap<>();
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
                // TODO the above not true for s2s join - see TODO in HazelcastRelMdWatermarkedFields
                Join join = (Join) node;
                Map<Integer, MutableByte> refByteMap = relToWmKeyMapping.get(join.getLeft());
                relToWmKeyMapping.put(node, refByteMap);
            } else {
                // anything else -- just forward without any changes.
                if (!node.getInputs().isEmpty()) {
                    Map<Integer, MutableByte> refByteMap = relToWmKeyMapping.get(node.getInputs().iterator().next());
                    relToWmKeyMapping.put(node, refByteMap);
                }
            }

            assert relToWmKeyMapping.getOrDefault(node, emptyMap()).keySet().equals(
                    OptUtils.metadataQuery(node).extractWatermarkedFields(node).getFieldIndexes())
                    : "mismatch between WM fields in metadata query and in WmKeyAssigner";
        }
    }
}
