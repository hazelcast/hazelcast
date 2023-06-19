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

import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.opt.metadata.HazelcastRelMdPrunability.PrunabilityMetadata;
import com.hazelcast.jet.sql.impl.opt.physical.FullScanPhysicalRel;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTable;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Util;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class HazelcastRelMdPrunability
        implements MetadataHandler<PrunabilityMetadata> {

    public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider.reflectiveSource(
            PrunabilityMetadata.METHOD,
            new HazelcastRelMdPrunability()
    );

    private HazelcastRelMdPrunability() {
    }

    @Override
    public MetadataDef<PrunabilityMetadata> getDef() {
        return PrunabilityMetadata.DEF;
    }

    @SuppressWarnings("unused")
    public List<Tuple2<String, Map<String, RexNode>>> extractPrunability(FullScanPhysicalRel scan, RelMetadataQuery mq) {

        final HazelcastTable hazelcastTable = OptUtils.extractHazelcastTable(scan);
        if (!(hazelcastTable.getTarget() instanceof PartitionedMapTable)) {
            return Collections.emptyList();
        }

        final PartitionedMapTable targetTable = hazelcastTable.getTarget();
        final HashSet<String> partitioningFiledNames = new HashSet<>(targetTable.partitioningAttributes());
        final Set<String> partitioningColumns = targetTable.keyFields()
                .filter(kf -> partitioningFiledNames.contains(kf.getPath().getPath()))
                .map(TableField::getName)
                .collect(Collectors.toSet());

        final RelDataType rowType = hazelcastTable.getRowType(scan.getCluster().getTypeFactory());
        final Set<Integer> partitioningColumnIndexes = partitioningColumns.stream()
                .map(colName -> rowType.getField(colName, false, false))
                .filter(Objects::nonNull)
                .map(RelDataTypeField::getIndex)
                .collect(Collectors.toSet());
        if (partitioningColumnIndexes.size() != partitioningColumns.size()) {
            return Collections.emptyList();
        }

        final Map<Integer, Boolean> filterCompleteness = partitioningColumnIndexes.stream()
                .collect(Collectors.toMap(Function.identity(), (k) -> false));

        final RexNode filter = hazelcastTable.getFilter();
        if (!(filter instanceof RexCall)) {
            return Collections.emptyList();
        }

        final RexCall call = (RexCall) filter;
        PartitionStrategyConditionExtractor conditionExtractor = new PartitionStrategyConditionExtractor();
        return Collections.singletonList(conditionExtractor.extractCondition(targetTable.getMapName(), call));
    }
    @SuppressWarnings("unused")
    public List<Tuple2<String, Map<String, RexNode>>> extractPrunability(RelSubset subset, RelMetadataQuery mq) {
        HazelcastRelMetadataQuery query = HazelcastRelMetadataQuery.reuseOrCreate(mq);
        RelNode rel = Util.first(subset.getBest(), subset.getOriginal());
        return query.extractPrunability(rel);
    }

    @SuppressWarnings("unused")
    public List<Tuple2<String, Map<String, RexNode>>> extractPrunability(RelNode rel, RelMetadataQuery mq) {
        // For any non-mentioned rels, we assume they are prunable and forwards prunability.
        HazelcastRelMetadataQuery query = HazelcastRelMetadataQuery.reuseOrCreate(mq);
        List<Tuple2<String, Map<String, RexNode>>> prunability = new ArrayList<>();
        for (int i = 0; i < rel.getInputs().size(); i++) {
            RelNode input = rel.getInput(i);
            prunability.addAll(query.extractPrunability(input));
        }
        return prunability;
    }

    public List<Tuple2<String, Map<String, RexNode>>> extractPrunability(BiRel rel, RelMetadataQuery mq) {
        // For any bi-rel we (temporarily) are not propagating prunability.
        return Collections.emptyList();
    }

    public interface PrunabilityMetadata extends Metadata {

        Method METHOD = Types.lookupMethod(PrunabilityMetadata.class, "extractPrunability");

        MetadataDef<PrunabilityMetadata> DEF = MetadataDef.of(
                PrunabilityMetadata.class,
                Handler.class,
                METHOD
        );

        @SuppressWarnings("unused")
        List<Map<String, RexNode>> extractPrunability();

        interface Handler extends MetadataHandler<PrunabilityMetadata> {

            List<Tuple2<String, Map<String, RexNode>>> extractPrunability(RelNode rel, RelMetadataQuery mq);
        }
    }
}
