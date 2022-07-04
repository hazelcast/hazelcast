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

package com.hazelcast.jet.sql.impl.opt.metadata;

import com.google.common.collect.ImmutableMap;
import com.hazelcast.jet.sql.impl.opt.FullScan;
import com.hazelcast.jet.sql.impl.opt.SlidingWindow;
import com.hazelcast.jet.sql.impl.opt.logical.DropLateItemsLogicalRel;
import com.hazelcast.jet.sql.impl.opt.logical.WatermarkLogicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.DropLateItemsPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.SlidingWindowAggregatePhysicalRel;
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
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Util;

import javax.annotation.Nullable;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
        return watermarkedFieldByIndex(rel, rel.watermarkedColumnIndex());
    }

    @SuppressWarnings("unused")
    public WatermarkedFields extractWatermarkedFields(WatermarkLogicalRel rel) {
        return watermarkedFieldByIndex(rel, rel.watermarkedColumnIndex());
    }

    @Nullable
    public static WatermarkedFields watermarkedFieldByIndex(RelNode rel, int watermarkedFieldIndex) {
        if (watermarkedFieldIndex < 0) {
            return null;
        }
        return new WatermarkedFields(ImmutableMap.of(watermarkedFieldIndex,
                rel.getCluster().getRexBuilder().makeInputRef(rel, watermarkedFieldIndex)));
    }

    @SuppressWarnings("unused")
    public WatermarkedFields extractWatermarkedFields(SlidingWindow rel, RelMetadataQuery mq) {
        HazelcastRelMetadataQuery query = HazelcastRelMetadataQuery.reuseOrCreate(mq);
        WatermarkedFields inputWatermarkedFields = query.extractWatermarkedFields(rel.getInput());

        if (inputWatermarkedFields == null
                || !inputWatermarkedFields.getPropertiesByIndex().containsKey(rel.orderingFieldIndex())) {
            // if there's no watermarked field in the input to a window function, or if the field used to
            // calculate window bounds isn't watermarked, the window bounds aren't watermarked either
            return inputWatermarkedFields;
        }

        RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
        return inputWatermarkedFields.merge(new WatermarkedFields(ImmutableMap.of(
                rel.windowStartIndex(), rexBuilder.makeInputRef(rel, rel.windowStartIndex()),
                rel.windowEndIndex(), rexBuilder.makeInputRef(rel, rel.windowEndIndex()))));
    }

    @SuppressWarnings("unused")
    public WatermarkedFields extractWatermarkedFields(Calc rel, RelMetadataQuery mq) {
        HazelcastRelMetadataQuery query = HazelcastRelMetadataQuery.reuseOrCreate(mq);
        WatermarkedFields inputWmFields = query.extractWatermarkedFields(rel.getInput());
        if (inputWmFields == null) {
            return null;
        }

        Map<Integer, RexNode> outputWmFields = new HashMap<>();
        List<RexNode> projectList = rel.getProgram().expandList(rel.getProgram().getProjectList());
        for (int i = 0; i < projectList.size(); i++) {
            RexNode project = projectList.get(i);
            RexNode project2 = unwrapAsOperatorOperand(project);
            // TODO [viliam] we currently handle only direct input references. We should handle also monotonic
            //  transformations of input references.
            if (project2 instanceof RexInputRef) {
                int index = ((RexInputRef) project2).getIndex();
                if (inputWmFields.getPropertiesByIndex().containsKey(index)) {
                    outputWmFields.put(i, project);
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
        Map<Integer, RexNode> outputProperties = new HashMap<>();
        for (int outputIndex = 0; groupedIndexes.hasNext(); outputIndex++) {
            int groupedBy = groupedIndexes.next();
            if (inputWmFields.getPropertiesByIndex().containsKey(groupedBy)) {
                outputProperties.put(outputIndex, rel.getCluster().getRexBuilder().makeInputRef(rel, outputIndex));
            }
        }

        return new WatermarkedFields(outputProperties);
    }

    @SuppressWarnings("unused")
    public WatermarkedFields extractWatermarkedFields(Join rel, RelMetadataQuery mq) {
        HazelcastRelMetadataQuery query = HazelcastRelMetadataQuery.reuseOrCreate(mq);
        // We currently support only nested-loop join and hash join that iterate the left side and forward
        // WM in it. WM on the right side isn't forwarded.
        // TODO: When we implement stream-to-stream join, we need to revisit this.
        return query.extractWatermarkedFields(rel.getLeft());
    }

    @SuppressWarnings("unused")
    public WatermarkedFields extractWatermarkedFields(Union rel, RelMetadataQuery mq) {
        HazelcastRelMetadataQuery query = HazelcastRelMetadataQuery.reuseOrCreate(mq);
        WatermarkedFields wmFields = new WatermarkedFields(Collections.emptyMap());
        for (RelNode input : rel.getInputs()) {
            WatermarkedFields watermarkedFields = query.extractWatermarkedFields(input);
            if (!wmFields.equals(watermarkedFields)) {
                wmFields = wmFields.merge(watermarkedFields);
            }
        }
        return wmFields;
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
        RelNode rel = Util.first(subset.getBest(), subset.getOriginal());
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
