/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.impl.calcite.physical.rule;

import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.partition.PartitioningStrategy;
import com.hazelcast.partition.strategy.DeclarativePartitioningStrategy;
import com.hazelcast.query.QueryConstants;
import com.hazelcast.sql.impl.SqlUtils;
import com.hazelcast.sql.impl.calcite.HazelcastConventions;
import com.hazelcast.sql.impl.calcite.RuleUtils;
import com.hazelcast.sql.impl.calcite.logical.rel.MapScanLogicalRel;
import com.hazelcast.sql.impl.calcite.physical.distribution.PhysicalDistributionField;
import com.hazelcast.sql.impl.calcite.physical.distribution.PhysicalDistributionTrait;
import com.hazelcast.sql.impl.calcite.physical.rel.MapScanPhysicalRel;
import com.hazelcast.sql.impl.calcite.physical.rel.PhysicalRel;
import com.hazelcast.sql.impl.calcite.physical.rel.ReplicatedMapScanPhysicalRel;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.Collections;
import java.util.List;

/**
 * Convert logical map scan to either replicated or partitioned physical scan.
 */
public final class MapScanPhysicalRule extends RelOptRule {
    public static final RelOptRule INSTANCE = new MapScanPhysicalRule();

    private MapScanPhysicalRule() {
        super(
            RuleUtils.single(MapScanLogicalRel.class, HazelcastConventions.LOGICAL),
            MapScanPhysicalRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        MapScanLogicalRel scan = call.rel(0);

        RelOptTable table = scan.getTable();

        HazelcastTable hazelcastTable = table.unwrap(HazelcastTable.class);

        PhysicalRel newScan;

        if (hazelcastTable.isReplicated()) {
            newScan = new ReplicatedMapScanPhysicalRel(
                scan.getCluster(),
                RuleUtils.toPhysicalConvention(scan.getTraitSet(), PhysicalDistributionTrait.REPLICATED),
                table,
                scan.getProjects(),
                scan.getFilter()
            );
        } else {
            PhysicalDistributionTrait distributionTrait = getDistributionTrait(hazelcastTable);

            newScan = new MapScanPhysicalRel(
                scan.getCluster(),
                RuleUtils.toPhysicalConvention(scan.getTraitSet(), distributionTrait),
                table,
                scan.getProjects(),
                scan.getFilter()
            );
        }

        call.transformTo(newScan);
    }

    /**
     * Get distribution trait for the given table.
     *
     * @param hazelcastTable Table.
     * @return Distribution trait.
     */
    private static PhysicalDistributionTrait getDistributionTrait(HazelcastTable hazelcastTable) {
        List<PhysicalDistributionField> distributionFields = getDistributionFields(hazelcastTable);

        if (distributionFields.isEmpty()) {
            return PhysicalDistributionTrait.DISTRIBUTED;
        } else {
            return PhysicalDistributionTrait.distributedPartitioned(distributionFields);
        }
    }

    /**
     * Get distribution field of the given table.
     *
     * @param hazelcastTable Table.
     * @return Distribution field wrapped into a list or an empty list if no distribution field could be determined.
     */
    private static List<PhysicalDistributionField> getDistributionFields(HazelcastTable hazelcastTable) {
        if (hazelcastTable.isReplicated()) {
            return Collections.emptyList();
        }

        MapProxyImpl map = hazelcastTable.getContainer();

        String distributionField = getDistributionFieldName(hazelcastTable);

        int index = 0;

        for (RelDataTypeField field : hazelcastTable.getFieldList()) {
            String path = map.normalizeAttributePath(field.getName());

            if (path.equals(QueryConstants.KEY_ATTRIBUTE_NAME.value())) {
                // If there is no distribution field, use the whole key.
                if (distributionField == null) {
                    return Collections.singletonList(new PhysicalDistributionField(index));
                }

                // Otherwise try to find desired field as a nested field of the key.
                for (RelDataTypeField nestedField : field.getType().getFieldList()) {
                    String nestedFieldName = nestedField.getName();

                    if (nestedField.getName().equals(distributionField)) {
                        return Collections.singletonList(new PhysicalDistributionField(index, nestedFieldName));
                    }
                }
            } else {
                // Try extracting the field from the key-based path and check if it is the distribution field.
                // E.g. "field" -> (attribute) -> "__key.distField" -> (strategy) -> "distField".
                String keyPath = SqlUtils.extractKeyPath(path);

                if (keyPath != null) {
                    if (keyPath.equals(distributionField)) {
                        return Collections.singletonList(new PhysicalDistributionField(index));
                    }
                }
            }

            index++;
        }

        return Collections.emptyList();
    }

    /**
     * Get distribution field name if possible.
     *
     * @param hazelcastTable Table.
     * @return Distribution field or {@code null} if none available.
     */
    private static String getDistributionFieldName(HazelcastTable hazelcastTable) {
        assert !hazelcastTable.isReplicated();

        MapProxyImpl map = hazelcastTable.getContainer();

        PartitioningStrategy strategy = map.getPartitionStrategy();

        if (strategy instanceof DeclarativePartitioningStrategy) {
            return ((DeclarativePartitioningStrategy) strategy).getField();
        }

        return null;
    }
}
