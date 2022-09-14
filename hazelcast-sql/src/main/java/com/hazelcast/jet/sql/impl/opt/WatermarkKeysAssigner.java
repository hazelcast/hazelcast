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

import com.hazelcast.internal.util.MutableByte;
import com.hazelcast.jet.sql.impl.opt.metadata.WatermarkedFields;
import com.hazelcast.jet.sql.impl.opt.physical.CalcPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.DropLateItemsPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.FullScanPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.PhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.SlidingWindowAggregatePhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.StreamToStreamJoinPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.UnionPhysicalRel;
import com.hazelcast.sql.impl.QueryException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonMap;

/**
 * Traverse a relational tree and assign watermark keys. It assumes that {@link
 * FullScan} instances already have keys assigned, and assigns the watermark
 * keys to other rels in the tree. It can also assign a different key to
 * different scans, e.g. in case they are both an input to a UNION operator.
 * Therefore, after this, the {@link FullScanPhysicalRel#getWatermarkKey()}
 * shouldn't be used, because it contains the value assigned before this class
 * assigned different key.
 */
public class WatermarkKeysAssigner {
    private final PhysicalRel root;
    private final KeyAssignerVisitor visitor;

    public WatermarkKeysAssigner(PhysicalRel root) {
        this.root = root;
        this.visitor = new KeyAssignerVisitor();
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
    private static class KeyAssignerVisitor extends RelVisitor {
        private final Map<RelNode, Map<Integer, MutableByte>> relToWmKeyMapping = new HashMap<>();

        KeyAssignerVisitor() {
        }

        public Map<RelNode, Map<Integer, MutableByte>> getRelToWmKeyMapping() {
            return relToWmKeyMapping;
        }

        @Override
        public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
            visit0(node, ordinal, parent);

            WatermarkedFields wmFields = OptUtils.metadataQuery(node).extractWatermarkedFields(node);
            if (!Objects.equals(
                    wmFields == null ? emptySet() : wmFields.getFieldIndexes(),
                    relToWmKeyMapping.getOrDefault(node, emptyMap()).keySet())) {
                throw new RuntimeException("mismatch between WM fields in metadata query and in WmKeyAssigner");
            }
        }

        private void visit0(RelNode node, int ordinal, @Nullable RelNode parent) {
            // start with recursion to children
            super.visit(node, ordinal, parent);

            if (node instanceof FullScanPhysicalRel) {
                assert node.getInputs().isEmpty() : "FullScan not a leaf";
                FullScanPhysicalRel scan = (FullScanPhysicalRel) node;
                if (scan.watermarkedColumnIndex() < 0) {
                    return;
                }
                Byte keyToAssign = scan.getWatermarkKey();
                assert keyToAssign != null;
                MutableByte key = new MutableByte(keyToAssign);
                Map<Integer, MutableByte> res = singletonMap(scan.watermarkedColumnIndex(), key);
                Map<Integer, MutableByte> oldValue = relToWmKeyMapping.put(scan, res);
                assert oldValue == null : "The same scan used twice in the execution plan";
                return;
            }

            // don't add anything if no traversed FullScan is watermarked.
            if (relToWmKeyMapping.isEmpty()) {
                return;
            }

            if (node instanceof CalcPhysicalRel) {
                CalcPhysicalRel calc = (CalcPhysicalRel) node;
                List<RexNode> projects = calc.getProgram().expandList(calc.getProgram().getProjectList());
                Map<Integer, MutableByte> refByteMap = relToWmKeyMapping.get(calc.getInput());
                if (refByteMap == null) {
                    return;
                }

                Map<Integer, MutableByte> calcRefByteMap = new HashMap<>();
                for (int projectIndex = 0; projectIndex < projects.size(); ++projectIndex) {
                    if (projects.get(projectIndex) instanceof RexInputRef) {
                        RexInputRef ref = (RexInputRef) projects.get(projectIndex);
                        int idx = ref.getIndex();
                        MutableByte wmKey = refByteMap.get(idx);
                        if (wmKey != null) {
                            calcRefByteMap.put(projectIndex, wmKey);
                        }
                    }
                }

                relToWmKeyMapping.put(calc, calcRefByteMap);

            } else if (node instanceof UnionPhysicalRel) {
                UnionPhysicalRel union = (UnionPhysicalRel) node;
                assert !union.getInputs().isEmpty();

                Map<Integer, MutableByte> intersection =
                        new HashMap<>(relToWmKeyMapping.getOrDefault(union.getInput(0), emptyMap()));
                for (int inputIndex = 0; inputIndex < union.getInputs().size(); inputIndex++) {
                    Map<Integer, MutableByte> inputWmKeys =
                            relToWmKeyMapping.getOrDefault(union.getInput(inputIndex), emptyMap());
                    for (Iterator<Entry<Integer, MutableByte>> intersectionIt = intersection.entrySet().iterator();
                         intersectionIt.hasNext(); ) {
                        Entry<Integer, MutableByte> intersectionEntry = intersectionIt.next();
                        MutableByte inputWmKey = inputWmKeys.get(intersectionEntry.getKey());
                        if (inputWmKey == null) {
                            intersectionIt.remove();
                        } else {
                            inputWmKey.setValue(intersectionEntry.getValue().getValue());
                        }
                    }
                }

                relToWmKeyMapping.put(union, intersection);
            } else if (node instanceof StreamToStreamJoinPhysicalRel) {
                StreamToStreamJoinPhysicalRel join = (StreamToStreamJoinPhysicalRel) node;
                Map<Integer, MutableByte> leftWmKeyMapping = relToWmKeyMapping.get(join.getLeft());
                if (leftWmKeyMapping == null) {
                    throw QueryException.error("Left input of stream-to-stream JOIN doesn't contain watermarks");
                }

                Map<Integer, MutableByte> rightWmKeyMapping = relToWmKeyMapping.get(join.getRight());
                if (rightWmKeyMapping == null) {
                    throw QueryException.error("Right input of stream-to-stream JOIN doesn't contain watermarks");
                }

                Map<Integer, MutableByte> joinedRefByteMap = new HashMap<>();
                int leftFieldCount = join.getLeft().getRowType().getFieldCount();
                for (Entry<Integer, MutableByte> en : leftWmKeyMapping.entrySet()) {
                    joinedRefByteMap.put(en.getKey(), en.getValue());
                }
                for (Entry<Integer, MutableByte> en : rightWmKeyMapping.entrySet()) {
                    joinedRefByteMap.put(en.getKey() + leftFieldCount, en.getValue());
                }

                assert leftWmKeyMapping.size() + rightWmKeyMapping.size() == joinedRefByteMap.size();
                relToWmKeyMapping.put(join, joinedRefByteMap);
            } else if (node instanceof Join) {
                // Hash Join and Nested Loop Join just forward watermarks from left input.
                Join join = (Join) node;
                Map<Integer, MutableByte> refByteMap = relToWmKeyMapping.get(join.getLeft());
                relToWmKeyMapping.put(node, refByteMap);

            } else if (node instanceof SlidingWindowAggregatePhysicalRel) {
                SlidingWindowAggregatePhysicalRel swAgg = (SlidingWindowAggregatePhysicalRel) node;
                Map<Integer, MutableByte> inputWmKeys = relToWmKeyMapping.get(swAgg.getInput());
                if (inputWmKeys == null) {
                    return;
                }
                MutableByte inputTimestampKey = inputWmKeys.get(swAgg.timestampFieldIndex());
                if (inputTimestampKey == null) {
                    return;
                }
                WatermarkedFields watermarkedFields = swAgg.watermarkedFields();
                Map<Integer, MutableByte> refByteMap = new HashMap<>();
                for (Integer fieldIndex : watermarkedFields.getFieldIndexes()) {
                    refByteMap.put(fieldIndex, inputTimestampKey);
                }

                relToWmKeyMapping.put(swAgg, refByteMap);

            } else if (node instanceof SlidingWindow) {
                SlidingWindow sw = (SlidingWindow) node;
                relToWmKeyMapping.put(sw, relToWmKeyMapping.get(sw.getInput()));

            } else if (node instanceof Aggregate) {
                Aggregate agg = (Aggregate) node;
                WatermarkedFields inputWmFields = OptUtils.metadataQuery(agg).extractWatermarkedFields(agg.getInput());
                if (inputWmFields == null || agg.getGroupSets().size() != 1) {
                    // not implemented
                    return;
                }

                Map<Integer, MutableByte> inputByteMap = relToWmKeyMapping.get(agg.getInput());
                Map<Integer, MutableByte> byteMap = new HashMap<>();
                Iterator<Integer> groupedIndexes = agg.getGroupSets().get(0).iterator();
                // we forward only grouped fields.
                for (int outputIndex = 0; groupedIndexes.hasNext(); outputIndex++) {
                    int groupedBy = groupedIndexes.next();
                    if (inputWmFields.getFieldIndexes().contains(groupedBy)) {
                        byteMap.put(groupedBy, inputByteMap.get(outputIndex));
                    }
                }
                relToWmKeyMapping.put(agg, byteMap);

            } else if (node instanceof DropLateItemsPhysicalRel) {
                relToWmKeyMapping.put(node, relToWmKeyMapping.get(node.getInput(0)));

            } else {
                // watermark is not preserved for other rels -- break the chain.
                return;
            }
        }
    }
}
