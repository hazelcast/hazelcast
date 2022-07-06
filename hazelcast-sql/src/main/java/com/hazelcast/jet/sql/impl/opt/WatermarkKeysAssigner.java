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
import com.hazelcast.jet.sql.impl.opt.metadata.WatermarkedFields;
import com.hazelcast.jet.sql.impl.opt.physical.FullScanPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.PhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.StreamToStreamJoinPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.UnionPhysicalRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Traverse optimized physical relational tree
 * and manually assign watermark keys.
 */
public class WatermarkKeysAssigner {
    private final PhysicalRel root;
    private final WatermarkKeyAssignerVisitor visitor;

    public WatermarkKeysAssigner(PhysicalRel root) {
        this.root = root;
        this.visitor = new WatermarkKeyAssignerVisitor(root);
    }

    public void assignWatermarkKeys() {
        WatermarkKeyAssignerVisitor visitor = new WatermarkKeyAssignerVisitor(root);
        visitor.go(root);
    }

    public Map<RexInputRef, Byte> getWatermarkedFieldsKey(RelNode node) {
        return visitor.getRefToWmKeyMapping().get(node);
    }

    private static class WatermarkKeyAssignerVisitor extends RelVisitor {
        private final RexBuilder rexBuilder;
        private final HazelcastRelMetadataQuery relMetadataQuery;
        private final byte[] keyCounter = {0};
        private final Map<RelNode, Map<RexInputRef, Byte>> refToWmKeyMapping = new HashMap<>();
        private final Set<StreamToStreamJoinPhysicalRel> visited = new HashSet<>();
        private final Set<Byte> rootWatermarkKeysSet = new HashSet<>();

        WatermarkKeyAssignerVisitor(PhysicalRel rootRel) {
            this.rexBuilder = rootRel.getCluster().getRexBuilder();
            this.relMetadataQuery = OptUtils.metadataQuery(rootRel);
            WatermarkedFields rootWatermarkedFields = relMetadataQuery.extractWatermarkedFields(rootRel);

            byte[] idx = {0};
            Map<RexInputRef, Byte> refByteMap = new HashMap<>();
            for (RexInputRef ref : rootWatermarkedFields.getPropertiesByIndex().values()) {
                rootWatermarkKeysSet.add(idx[0]);
                refByteMap.put(ref, idx[0]++);
            }
            refToWmKeyMapping.put(rootRel, refByteMap);
        }

        public Map<RelNode, Map<RexInputRef, Byte>> getRefToWmKeyMapping() {
            return refToWmKeyMapping;
        }

        @Override
        public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
            // Most probably, query would not contain any watermarked fields.
            // In that case, just skip watermarks assigment phase.
            if (rootWatermarkKeysSet.isEmpty()) {
                return;
            }

            if (node instanceof FullScanPhysicalRel) {
                visit((FullScanPhysicalRel) node, ordinal, parent);
            } else if (node instanceof StreamToStreamJoinPhysicalRel) {
                visit((StreamToStreamJoinPhysicalRel) node, ordinal, parent);
            } else if (node instanceof SlidingWindow) {
                visit((SlidingWindow) node, ordinal, parent);
            } else if (node instanceof UnionPhysicalRel) {
                visit((UnionPhysicalRel) node, ordinal, parent);
            }

            // anything else -- just forward without any changes.
            if (!refToWmKeyMapping.containsKey(node)) {
                refToWmKeyMapping.put(node, refToWmKeyMapping.get(parent));
            }
            super.visit(node, ordinal, parent);
        }

        /**
         * Assign watermark keys to source relation.
         * Each source may have <b>only</b> watermark key assigned.
         */
        private void visit(FullScanPhysicalRel scan, int ordinal, @Nullable RelNode parent) {
            int idx = scan.watermarkedColumnIndex();
            RelDataType watermarkedFieldType = scan.getRowType().getFieldList().get(idx).getType();
            RexInputRef watermarkedRexRef = rexBuilder.makeInputRef(watermarkedFieldType, idx);

            byte wmKey = 0;
            if (rootWatermarkKeysSet.size() > 1) {
                Map<RexInputRef, Byte> refByteMap;
                if (refToWmKeyMapping.containsKey(scan)) {
                    refByteMap = refToWmKeyMapping.get(scan);
                } else {
                    refByteMap = refToWmKeyMapping.get(parent);
                }
                if (refByteMap.isEmpty()) {
                    // Sometimes watermarked fields may beremoved by Project or Calc relations.
                    // In such case, we assign a watermark key which would not
                    // be used in any joins, but stream still is abele to generate watermarks.
                    wmKey = keyCounter[0]++;
                } else {
                    RexInputRef ref = refByteMap.keySet().iterator().next();
                    assert ref != null : "Parent node doesn't have watermark key";
                    if (watermarkedRexRef.equals(ref)) {
                        wmKey = refByteMap.get(ref);
                    }
                }
                scan.setWatermarkKey(wmKey);
            }

            refToWmKeyMapping.put(scan, Collections.singletonMap(watermarkedRexRef, wmKey));
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
            WatermarkedFields joinedWmFields = relMetadataQuery.extractWatermarkedFields(join);
            Map<RexInputRef, Byte> refByteMap = refToWmKeyMapping.get(parent);
            // Case when S2S Join rel is a root level
            if (refByteMap == null) {
                refByteMap = refToWmKeyMapping.get(join);
            }

            Map<RexInputRef, Byte> leftInputRefByteMap = new HashMap<>();
            for (Map.Entry<Integer, Integer> entry : join.leftInputToJointRowMapping().entrySet()) {
                tryAssignKey(join.getLeft(), joinedWmFields, entry, refByteMap, leftInputRefByteMap);
            }

            Map<RexInputRef, Byte> rightInputRefByteMap = new HashMap<>();
            for (Map.Entry<Integer, Integer> entry : join.rightInputToJointRowMapping().entrySet()) {
                tryAssignKey(join.getRight(), joinedWmFields, entry, refByteMap, rightInputRefByteMap);
            }

            refToWmKeyMapping.put(join.getLeft(), leftInputRefByteMap);
            refToWmKeyMapping.put(join.getRight(), rightInputRefByteMap);
        }

        /**
         *
         */
        private void visit(UnionPhysicalRel union, int ordinal, @Nullable RelNode parent) {
            refToWmKeyMapping.put(union, refToWmKeyMapping.get(parent));
        }

        private void tryAssignKey(
                RelNode child,
                WatermarkedFields watermarkedFields,
                Entry<Integer, Integer> entry,
                Map<RexInputRef, Byte> refByteMap,
                Map<RexInputRef, Byte> inputRefByteMap) {
            if (watermarkedFields.getPropertiesByIndex().containsKey(entry.getValue())) {
                RexInputRef ref = watermarkedFields.getPropertiesByIndex().get(entry.getValue());
                assert ref != null;
                Byte wmKey = refByteMap.get(ref);
                if (wmKey == null) {
                    return;
                }

                RexInputRef currRef = rexBuilder.makeInputRef(ref.getType(), entry.getKey());
                inputRefByteMap.put(currRef, wmKey);
            }
        }
    }
}