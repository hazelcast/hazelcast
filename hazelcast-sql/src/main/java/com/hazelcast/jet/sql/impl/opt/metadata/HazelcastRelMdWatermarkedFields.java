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

package com.hazelcast.jet.sql.impl.opt.metadata;

import com.google.common.collect.ImmutableSet;
import com.hazelcast.jet.sql.impl.opt.FullScan;
import com.hazelcast.jet.sql.impl.opt.SlidingWindow;
import com.hazelcast.jet.sql.impl.opt.logical.DropLateItemsLogicalRel;
import com.hazelcast.jet.sql.impl.opt.logical.WatermarkLogicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.DropLateItemsPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.JoinHashPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.JoinNestedLoopPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.SlidingWindowAggregatePhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.StreamToStreamJoinPhysicalRel;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import javax.annotation.Nullable;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hazelcast.jet.sql.impl.validate.ValidationUtil.unwrapAsOperatorOperand;

public final class HazelcastRelMdWatermarkedFields
        implements MetadataHandler<HazelcastRelMdWatermarkedFields.WatermarkedFieldsMetadata> {

    public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider.reflectiveSource(
            WatermarkedFieldsMetadata.METHOD,
            new HazelcastRelMdWatermarkedFields()
    );

    private HazelcastRelMdWatermarkedFields() {
    }

    @Override
    public MetadataDef<WatermarkedFieldsMetadata> getDef() {
        return WatermarkedFieldsMetadata.DEF;
    }

    @SuppressWarnings("unused")
    public WatermarkedFields extractWatermarkedFields(FullScan rel, RelMetadataQuery mq) {
        return watermarkedFieldByIndex(rel.watermarkedColumnIndex());
    }

    @SuppressWarnings("unused")
    public WatermarkedFields extractWatermarkedFields(WatermarkLogicalRel rel) {
        return watermarkedFieldByIndex(rel.watermarkedColumnIndex());
    }

    @Nullable
    public static WatermarkedFields watermarkedFieldByIndex(int watermarkedFieldIndex) {
        if (watermarkedFieldIndex < 0) {
            return null;
        }
        return new WatermarkedFields(ImmutableSet.of(watermarkedFieldIndex));
    }

    @SuppressWarnings("unused")
    public WatermarkedFields extractWatermarkedFields(SlidingWindow rel, RelMetadataQuery mq) {
        HazelcastRelMetadataQuery query = HazelcastRelMetadataQuery.reuseOrCreate(mq);
        // TODO also add watermark to window start and end, under a different key, but it needs
        //  to be supported by the processor. It's not really needed as when we're doing aggregation,
        //  the rule removes this rel, but would be needed for future cases
        return query.extractWatermarkedFields(rel.getInput());
    }

    @SuppressWarnings("unused")
    public WatermarkedFields extractWatermarkedFields(Calc rel, RelMetadataQuery mq) {
        HazelcastRelMetadataQuery query = HazelcastRelMetadataQuery.reuseOrCreate(mq);
        WatermarkedFields inputWmFields = query.extractWatermarkedFields(rel.getInput());
        if (inputWmFields == null) {
            return null;
        }

        Set<Integer> outputWmFields = new HashSet<>();
        List<RexNode> projectList = rel.getProgram().expandList(rel.getProgram().getProjectList());
        for (int i = 0; i < projectList.size(); i++) {
            RexNode project = projectList.get(i);
            RexNode project2 = unwrapAsOperatorOperand(project);
            // TODO [viliam] we currently handle only direct input references. We should handle also monotonic
            //  transformations of input references.
            if (project2 instanceof RexInputRef) {
                int index = ((RexInputRef) project2).getIndex();
                if (inputWmFields.getFieldIndexes().contains(index)) {
                    outputWmFields.add(i);
                }
            }
        }

        return new WatermarkedFields(outputWmFields);
    }

    @SuppressWarnings("unused")
    public WatermarkedFields extractWatermarkedFields(SlidingWindowAggregatePhysicalRel rel, RelMetadataQuery mq) {
        return rel.watermarkedFields();
    }

    @SuppressWarnings("unused")
    public WatermarkedFields extractWatermarkedFields(Aggregate rel, RelMetadataQuery mq) {
        HazelcastRelMetadataQuery query = HazelcastRelMetadataQuery.reuseOrCreate(mq);
        WatermarkedFields inputWmFields = query.extractWatermarkedFields(rel.getInput());
        if (inputWmFields == null || rel.getGroupSets().size() != 1) {
            // not implemented
            return null;
        }

        // The fields, by which the aggregation groups, and which are aggregated on input, are watermarked
        // also on the output.
        Iterator<Integer> groupedIndexes = rel.getGroupSets().get(0).iterator();
        Set<Integer> outputProperties = new HashSet<>();
        for (int outputIndex = 0; groupedIndexes.hasNext(); outputIndex++) {
            int groupedBy = groupedIndexes.next();
            if (inputWmFields.getFieldIndexes().contains(groupedBy)) {
                outputProperties.add(outputIndex);
            }
        }

        return new WatermarkedFields(outputProperties);
    }

    @SuppressWarnings("unused")
    public WatermarkedFields extractWatermarkedFields(Join rel, RelMetadataQuery mq) {
        HazelcastRelMetadataQuery query = HazelcastRelMetadataQuery.reuseOrCreate(mq);

        if (rel instanceof JoinNestedLoopPhysicalRel || rel instanceof JoinHashPhysicalRel) {
            // Nested-loop join and hash join iterate the left side and forward WM in it.
            // WM on the right side isn't forwarded.
            return query.extractWatermarkedFields(rel.getLeft());
        } else if (rel instanceof StreamToStreamJoinPhysicalRel) {
            // Stream-to-stream join forwards all watermarks from both inputs. The fields
            // of the right input are shifted by the number of fields in the left input, so we
            // merge the WM indices this way.
            WatermarkedFields leftWmFields = query.extractWatermarkedFields(rel.getLeft());
            WatermarkedFields rightWmFields = query.extractWatermarkedFields(rel.getRight());

            final int offset = rel.getLeft().getRowType().getFieldList().size();
            Set<Integer> shiftedRightProps = rightWmFields.getFieldIndexes()
                    .stream()
                    .map(idx -> idx + offset)
                    .collect(Collectors.toSet());

            return leftWmFields.union(new WatermarkedFields(shiftedRightProps));
        } else {
            throw new RuntimeException("Unknown join rel: " + rel.getClass().getName());
        }
    }

    @SuppressWarnings("unused")
    public WatermarkedFields extractWatermarkedFields(Union rel, RelMetadataQuery mq) {
        HazelcastRelMetadataQuery query = HazelcastRelMetadataQuery.reuseOrCreate(mq);
        assert !rel.getInputs().isEmpty();
        Set<Integer> wmFields = new HashSet<>();
        for (int i = 0; i < rel.getInputs().size(); i++) {
            WatermarkedFields wmFields2 = query.extractWatermarkedFields(rel.getInputs().get(i));
            if (wmFields2 == null) {
                return null;
            }
            if (i == 0) {
                wmFields.addAll(wmFields2.getFieldIndexes());
            } else {
                wmFields.retainAll(wmFields2.getFieldIndexes());
            }
        }
        return new WatermarkedFields(wmFields);
    }

    @SuppressWarnings("unused")
    public WatermarkedFields extractWatermarkedFields(DropLateItemsLogicalRel rel, RelMetadataQuery mq) {
        HazelcastRelMetadataQuery query = HazelcastRelMetadataQuery.reuseOrCreate(mq);
        return query.extractWatermarkedFields(rel.getInput());
    }

    @SuppressWarnings("unused")
    public WatermarkedFields extractWatermarkedFields(DropLateItemsPhysicalRel rel, RelMetadataQuery mq) {
        HazelcastRelMetadataQuery query = HazelcastRelMetadataQuery.reuseOrCreate(mq);
        return query.extractWatermarkedFields(rel.getInput());
    }

    // Volcano planner specific case
    @SuppressWarnings("unused")
    public WatermarkedFields extractWatermarkedFields(RelSubset subset, RelMetadataQuery mq) {
        HazelcastRelMetadataQuery query = HazelcastRelMetadataQuery.reuseOrCreate(mq);
        RelNode rel = subset.getBestOrOriginal();
        return query.extractWatermarkedFields(rel);
    }

    // HEP planner specific case
    @SuppressWarnings("unused")
    public WatermarkedFields extractWatermarkedFields(HepRelVertex vertex, RelMetadataQuery mq) {
        HazelcastRelMetadataQuery query = HazelcastRelMetadataQuery.reuseOrCreate(mq);
        RelNode rel = vertex.getCurrentRel();
        return query.extractWatermarkedFields(rel);
    }

    @SuppressWarnings("unused")
    public WatermarkedFields extractWatermarkedFields(RelNode rel, RelMetadataQuery mq) {
        return null;
    }

    public interface WatermarkedFieldsMetadata extends Metadata {
        Method METHOD = Types.lookupMethod(WatermarkedFieldsMetadata.class, "extractWatermarkedFields");

        MetadataDef<WatermarkedFieldsMetadata> DEF = MetadataDef.of(
                WatermarkedFieldsMetadata.class,
                WatermarkedFieldsMetadata.Handler.class,
                METHOD);

        @SuppressWarnings("unused")
        WatermarkedFields extractWatermarkedFields();

        interface Handler extends MetadataHandler<WatermarkedFieldsMetadata> {
            WatermarkedFields extractWatermarkedFields(RelNode rel, RelMetadataQuery mq);
        }
    }
}
