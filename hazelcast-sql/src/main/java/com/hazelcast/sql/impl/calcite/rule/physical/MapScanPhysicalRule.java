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

package com.hazelcast.sql.impl.calcite.rule.physical;

import com.hazelcast.query.QueryConstants;
import com.hazelcast.sql.impl.SqlUtils;
import com.hazelcast.sql.impl.calcite.HazelcastConventions;
import com.hazelcast.sql.impl.calcite.RuleUtils;
import com.hazelcast.sql.impl.calcite.rel.logical.MapScanLogicalRel;
import com.hazelcast.sql.impl.calcite.distribution.DistributionField;
import com.hazelcast.sql.impl.calcite.distribution.DistributionTrait;
import com.hazelcast.sql.impl.calcite.rel.physical.MapScanPhysicalRel;
import com.hazelcast.sql.impl.calcite.rel.physical.PhysicalRel;
import com.hazelcast.sql.impl.calcite.rel.physical.ReplicatedMapScanPhysicalRel;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.hazelcast.sql.impl.calcite.distribution.DistributionType.DISTRIBUTED;

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

        List<Integer> projects = scan.getProjects();

        PhysicalRel newScan;

        if (hazelcastTable.isReplicated()) {
            newScan = new ReplicatedMapScanPhysicalRel(
                scan.getCluster(),
                RuleUtils.toPhysicalConvention(scan.getTraitSet(), DistributionTrait.REPLICATED_DIST),
                table,
                projects,
                scan.getFilter()
            );
        } else {
            DistributionTrait distributionTrait = getDistributionTrait(hazelcastTable, projects);

            newScan = new MapScanPhysicalRel(
                scan.getCluster(),
                RuleUtils.toPhysicalConvention(scan.getTraitSet(), distributionTrait),
                table,
                projects,
                scan.getFilter()
            );
        }

        call.transformTo(newScan);
    }

    /**
     * Get distribution trait for the given table.
     *
     * @param hazelcastTable Table.
     * @param projects Projects.
     * @return Distribution trait.
     */
    private static DistributionTrait getDistributionTrait(HazelcastTable hazelcastTable, List<Integer> projects) {
        List<DistributionField> distributionFields = getDistributionFields(hazelcastTable);

        if (distributionFields.isEmpty()) {
            return DistributionTrait.DISTRIBUTED_DIST;
        } else {
            // Remap internal scan distribution fields to projected fields.
            List<DistributionField> res = new ArrayList<>(distributionFields.size());

            for (DistributionField distributionField : distributionFields) {
                int distributionFieldIndex = distributionField.getIndex();

                int projectIndex = projects.indexOf(distributionFieldIndex);

                if (projectIndex == -1) {
                    return DistributionTrait.DISTRIBUTED_DIST;
                }

                res.add(new DistributionField(projectIndex, distributionField.getNestedField()));
            }

            return DistributionTrait.Builder.ofType(DISTRIBUTED).addFieldGroup(res).build();
        }
    }

    /**
     * Get distribution field of the given table.
     *
     * @param hazelcastTable Table.
     * @return Distribution field wrapped into a list or an empty list if no distribution field could be determined.
     */
    private static List<DistributionField> getDistributionFields(HazelcastTable hazelcastTable) {
        if (hazelcastTable.isReplicated()) {
            return Collections.emptyList();
        }

        String distributionFieldName = hazelcastTable.getDistributionField();

        int index = 0;

        for (RelDataTypeField field : hazelcastTable.getFieldList()) {
            String path = SqlUtils.normalizeAttributePath(field.getName(), hazelcastTable.getAliases());

            if (path.equals(QueryConstants.KEY_ATTRIBUTE_NAME.value())) {
                // If there is no distribution field, use the whole key.
                if (distributionFieldName == null) {
                    return Collections.singletonList(new DistributionField(index));
                }

                // TODO: Enable this for nested field support.
//                // Otherwise try to find desired field as a nested field of the key.
//                for (RelDataTypeField nestedField : field.getType().getFieldList()) {
//                    String nestedFieldName = nestedField.getName();
//
//                    if (nestedField.getName().equals(distributionField)) {
//                        return Collections.singletonList(new DistributionField(index, nestedFieldName));
//                    }
//                }
            } else {
                // Try extracting the field from the key-based path and check if it is the distribution field.
                // E.g. "field" -> (attribute) -> "__key.distField" -> (strategy) -> "distField".
                String keyPath = SqlUtils.extractKeyPath(path);

                if (keyPath != null) {
                    if (keyPath.equals(distributionFieldName)) {
                        return Collections.singletonList(new DistributionField(index));
                    }
                }
            }

            index++;
        }

        return Collections.emptyList();
    }
}
