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

import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.opt.metadata.HazelcastRelMdPrunability.PrunabilityMetadata;
import com.hazelcast.jet.sql.impl.opt.physical.FullScanPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.IndexScanMapPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.prunability.PartitionStrategyConditionExtractor;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTable;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Util;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;

@SuppressWarnings("DuplicatedCode")
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
    public Map<String, List<Map<String, RexNode>>> extractPrunability(
            FullScanPhysicalRel scan,
            RelMetadataQuery mq
    ) {

        final HazelcastTable hazelcastTable = OptUtils.extractHazelcastTable(scan);
        if (!(hazelcastTable.getTarget() instanceof PartitionedMapTable)) {
            return emptyMap();
        }

        final PartitionedMapTable targetTable = hazelcastTable.getTarget();
        if (!targetTable.supportsPartitionPruning()) {
            return emptyMap();
        }

        final Set<String> partitioningColumns;
        if (targetTable.partitioningAttributes().isEmpty()) {
            partitioningColumns = Set.of(QueryPath.KEY);
        } else {
            // PartitioningColumns contains field names rather than columns names,
            // we have to convert it to column names if EXTERNAL NAME is used.
            final HashSet<String> partitioningFieldNames = new HashSet<>(targetTable.partitioningAttributes());
            partitioningColumns = targetTable.keyFields()
                    .filter(kf -> partitioningFieldNames.contains(kf.getPath().getPath()))
                    .map(TableField::getName)
                    .collect(Collectors.toSet());
        }

        final RexNode filter = hazelcastTable.getFilter();
        if (!(filter instanceof RexCall)) {
            return emptyMap();
        }

        final RexCall call = (RexCall) filter;
        final var conditionExtractor = new PartitionStrategyConditionExtractor();
        return conditionExtractor.extractCondition(targetTable, call, partitioningColumns);
    }

    @SuppressWarnings("unused")
    public Map<String, List<Map<String, RexNode>>> extractPrunability(
            IndexScanMapPhysicalRel scan,
            RelMetadataQuery mq
    ) {
        // TODO: Implement
        return emptyMap();
    }

    @SuppressWarnings("unused")
    public Map<String, List<Map<String, RexNode>>> extractPrunability(Calc calc, RelMetadataQuery mq) {
        HazelcastRelMetadataQuery query = HazelcastRelMetadataQuery.reuseOrCreate(mq);
        return query.extractPrunability(calc.getInput());
    }

    @SuppressWarnings("unused")
    public Map<String, List<Map<String, RexNode>>> extractPrunability(Aggregate agg, RelMetadataQuery mq) {
        // TODO: Implement
        return emptyMap();
    }

    // It is done to support usage of this metadata query during opt phase.
    @SuppressWarnings("unused")
    public Map<String, List<Map<String, RexNode>>> extractPrunability(RelSubset subset, RelMetadataQuery mq) {
        HazelcastRelMetadataQuery query = HazelcastRelMetadataQuery.reuseOrCreate(mq);
        RelNode rel = Util.first(subset.getBest(), subset.getOriginal());
        return query.extractPrunability(rel);
    }

    @SuppressWarnings("unused")
    public Map<String, List<Map<String, RexNode>>> extractPrunability(Join rel, RelMetadataQuery mq) {
        // For any bi-rel (Joins) we (temporarily) are not propagating prunability.
        return Collections.emptyMap();
    }

    @SuppressWarnings("unused")
    public Map<String, List<Map<String, RexNode>>> extractPrunability(Union rel, RelMetadataQuery mq) {
        // Union is prunable, if all inputs of Union is prunable.
        // It collects prunability metadata from all inputs and forwards it.
        HazelcastRelMetadataQuery query = HazelcastRelMetadataQuery.reuseOrCreate(mq);
        Map<String, List<Map<String, RexNode>>> prunability = new HashMap<>();
        for (int i = 0; i < rel.getInputs().size(); i++) {
            RelNode input = rel.getInput(i);
            var extractedPrunability = query.extractPrunability(input);
            // If we detect any non-prunable input rel, we disrupt prunability.
            if (extractedPrunability.isEmpty()) {
                return emptyMap();
            }
            for (final String tableName : extractedPrunability.keySet()) {
                var tableVariants = extractedPrunability.get(tableName);
                prunability.putIfAbsent(tableName, new ArrayList<>());
                prunability.get(tableName).addAll(tableVariants);
            }
        }
        return prunability;
    }

    @SuppressWarnings("unused")
    public Map<String, List<Map<String, RexNode>>> extractPrunability(Sort rel, RelMetadataQuery mq) {
        // Sort is prunable and forwards prunability.
        HazelcastRelMetadataQuery query = HazelcastRelMetadataQuery.reuseOrCreate(mq);
        return query.extractPrunability(rel.getInput());
    }

    @SuppressWarnings("unused")
    public Map<String, List<Map<String, RexNode>>> extractPrunability(RelNode rel, RelMetadataQuery mq) {
        // For any non-mentioned rels, we assume they are not prunable and breaks prunability.
        return emptyMap();
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

            Map<String, List<Map<String, RexNode>>> extractPrunability(RelNode rel, RelMetadataQuery mq);
        }
    }
}
