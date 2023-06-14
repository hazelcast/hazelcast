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

package com.hazelcast.jet.sql.impl.opt.physical;

import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.opt.distribution.DistributionTrait;
import com.hazelcast.jet.sql.impl.opt.distribution.DistributionTraitDef;
import com.hazelcast.jet.sql.impl.opt.distribution.DistributionType;
import com.hazelcast.jet.sql.impl.opt.logical.FullScanLogicalRel;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTable;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.hazelcast.jet.sql.impl.opt.Conventions.LOGICAL;
import static com.hazelcast.jet.sql.impl.opt.Conventions.PHYSICAL;

final class FullScanPhysicalRule extends ConverterRule {

    static final RelOptRule INSTANCE = new FullScanPhysicalRule();

    private FullScanPhysicalRule() {
        super(
                FullScanLogicalRel.class, LOGICAL, PHYSICAL,
                FullScanPhysicalRule.class.getSimpleName()
        );
    }

    @Override
    public RelNode convert(RelNode rel) {
        FullScanLogicalRel logicalScan = (FullScanLogicalRel) rel;
        final RelTraitSet traitSet = OptUtils.toPhysicalConvention(logicalScan.getTraitSet());

        return new FullScanPhysicalRel(
                logicalScan.getCluster(),
                computeDistributedTrait(logicalScan, traitSet),
                logicalScan.getTable(),
                logicalScan.lagExpression(),
                logicalScan.watermarkedColumnIndex(),
                0);
    }

    private RelTraitSet computeDistributedTrait(FullScanLogicalRel scan, RelTraitSet traitSet) {
        final DistributionTrait distributionTrait = OptUtils.getDistribution(scan);
        final DistributionTraitDef traitDef = (DistributionTraitDef) distributionTrait.getTraitDef();
        if (distributionTrait.getType().equals(DistributionType.PARTITIONED)) {
            return traitSet;
        }

        final HazelcastTable hazelcastTable = OptUtils.extractHazelcastTable(scan);
        if (!(hazelcastTable.getTarget() instanceof PartitionedMapTable)) {
            return traitSet;
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
            return traitSet;
        }

        final Map<Integer, Boolean> filterCompleteness = partitioningColumnIndexes.stream()
                .collect(Collectors.toMap(Function.identity(), (k) -> false));

        final RexNode filter = hazelcastTable.getFilter();
        if (!(filter instanceof RexCall)) {
            return traitSet;
        }
        final RexCall call = (RexCall) filter;
        traverseCondition(call, filterCompleteness, call.isA(SqlKind.OR));

        return traitSet;
    }

    private void traverseCondition(RexCall cond, Map<Integer, Boolean> completenessMap, boolean subConditionsMustConverge) {
        switch (cond.getKind()) {
            case AND:
                final Map<Integer, Boolean> andCompletenessMap = setToMap(completenessMap.keySet());
                for (final RexNode operand : cond.getOperands()) {
                    if (!(operand instanceof RexCall)) {
                        continue;
                    }
                    traverseCondition((RexCall) operand, andCompletenessMap, false);
                }
                break;
            case EQUALS:
                assert cond.getOperands().size() == 2;
                final RexInputRef inputRef = extractInputRef(cond);
                final RexNode constantExpr = extractConstantExpression(cond);
                if (inputRef == null || constantExpr == null) {
                    return;
                }
                completenessMap.put(inputRef.getIndex(), true);
                break;
            default:
                return;
        }
    }

    private static <K,V> Map<K,V> setToMap(Set<K> set) {
        Map<K, V> map = new HashMap<>();
        for (K k : set) {
            if (map.put(k, null) != null) {
                throw new IllegalStateException("Duplicate key");
            }
        }
        return map;
    }

    private RexInputRef extractInputRef(final RexCall call) {
        // only works for EQUALS
        assert call.isA(SqlKind.EQUALS);
        if (call.getOperands().get(0) instanceof RexInputRef) {
            return (RexInputRef) call.getOperands().get(0);
        }

        if (call.getOperands().get(1) instanceof RexInputRef) {
            return (RexInputRef) call.getOperands().get(1);
        }

        return null;
    }

    private RexNode extractConstantExpression(final RexCall call) {
        assert call.isA(SqlKind.EQUALS);
        if (call.getOperands().get(0) instanceof RexDynamicParam) {
            return call.getOperands().get(0);
        }

        if (call.getOperands().get(1) instanceof RexDynamicParam) {
            return call.getOperands().get(1);
        }

        if (call.getOperands().get(0) instanceof RexLiteral) {
            return call.getOperands().get(0);
        }

        if (call.getOperands().get(0) instanceof RexLiteral) {
            return call.getOperands().get(0);
        }

        return null;
    }
}
