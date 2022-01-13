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
import com.hazelcast.jet.sql.impl.opt.logical.WatermarkLogicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.SlidingWindowAggregatePhysicalRel;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Util;

import java.lang.reflect.Method;
import java.util.HashMap;
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

    private static WatermarkedFields watermarkedFieldByIndex(RelNode rel, int watermarkedFieldIndex) {
        if (watermarkedFieldIndex < 0) {
            return null;
        }

        RelDataType type = rel.getRowType().getFieldList().get(watermarkedFieldIndex).getType();
        RexNode expr = rel.getCluster().getRexBuilder().makeInputRef(type, watermarkedFieldIndex);
        return new WatermarkedFields(ImmutableMap.of(watermarkedFieldIndex, expr));
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

        RelDataType rowType = rel.getRowType();
        int fieldCount = rowType.getFieldCount();
        RexBuilder rexBuilder = rel.getCluster().getRexBuilder();

        return inputWatermarkedFields.merge(new WatermarkedFields(ImmutableMap.of(
                fieldCount - 2, rexBuilder.makeInputRef(rowType.getFieldList().get(fieldCount - 2).getType(), fieldCount - 2),
                fieldCount - 1, rexBuilder.makeInputRef(rowType.getFieldList().get(fieldCount - 1).getType(), fieldCount - 1))));
    }

    @SuppressWarnings("unused")
    public WatermarkedFields extractWatermarkedFields(Project rel, RelMetadataQuery mq) {
        HazelcastRelMetadataQuery query = HazelcastRelMetadataQuery.reuseOrCreate(mq);
        WatermarkedFields inputWmFields = query.extractWatermarkedFields(rel.getInput());
        if (inputWmFields == null) {
            return null;
        }

        Map<Integer, RexNode> outputWmFields = new HashMap<>();
        for (int i = 0; i < rel.getProjects().size(); i++) {
            RexNode project = rel.getProjects().get(i);
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
    public WatermarkedFields extractWatermarkedFields(Filter rel, RelMetadataQuery mq) {
        HazelcastRelMetadataQuery query = HazelcastRelMetadataQuery.reuseOrCreate(mq);
        return query.extractWatermarkedFields(rel.getInput());
    }


    @SuppressWarnings("unused")
    public WatermarkedFields extractWatermarkedFields(SlidingWindowAggregatePhysicalRel rel, RelMetadataQuery mq) {
        return rel.watermarkedFields();
    }

    @SuppressWarnings("unused")
    public WatermarkedFields extractWatermarkedFields(Join rel, RelMetadataQuery mq) {
        HazelcastRelMetadataQuery query = HazelcastRelMetadataQuery.reuseOrCreate(mq);
        WatermarkedFields leftInputWatermarkedFields = query.extractWatermarkedFields(rel.getLeft());
        WatermarkedFields rightInputWatermarkedFields = query.extractWatermarkedFields(rel.getRight());

        if (rightInputWatermarkedFields == null) {
            return leftInputWatermarkedFields;
        }
        WatermarkedFields rightWatermarkedFields = new WatermarkedFields(rightInputWatermarkedFields.getPropertiesByIndex());

        return leftInputWatermarkedFields == null
                ? rightWatermarkedFields
                : leftInputWatermarkedFields.merge(rightWatermarkedFields);
    }
//
//    // i.e. Filter, AggregateAccumulateByKeyPhysicalRel, AggregateAccumulatePhysicalRel
//    @SuppressWarnings("unused")
//    public WatermarkedFields extractWatermarkedFields(SingleRel rel, RelMetadataQuery mq) {
//        HazelcastRelMetadataQuery query = HazelcastRelMetadataQuery.reuseOrCreate(mq);
//        return query.extractWatermarkedFields(rel.getInput());
//    }

    @SuppressWarnings("unused")
    public WatermarkedFields extractWatermarkedFields(RelSubset subset, RelMetadataQuery mq) {
        HazelcastRelMetadataQuery query = HazelcastRelMetadataQuery.reuseOrCreate(mq);
        RelNode rel = Util.first(subset.getBest(), subset.getOriginal());
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
