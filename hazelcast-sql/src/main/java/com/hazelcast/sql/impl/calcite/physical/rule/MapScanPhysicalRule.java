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

public class MapScanPhysicalRule extends RelOptRule {
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
                RuleUtils.toPhysicalConvention(scan.getTraitSet(), PhysicalDistributionTrait.DISTRIBUTED_REPLICATED),
                table,
                scan.deriveRowType()
            );
        }
        else {
            List<PhysicalDistributionField> distributionFields = getDistributionFields(hazelcastTable);

            PhysicalDistributionTrait distributionTrait = PhysicalDistributionTrait.distributedPartitioned(distributionFields);

            newScan = new MapScanPhysicalRel(
                scan.getCluster(),
                RuleUtils.toPhysicalConvention(scan.getTraitSet(), distributionTrait),
                table,
                scan.deriveRowType()
            );
        }

        call.transformTo(newScan);
    }

    private List<PhysicalDistributionField> getDistributionFields(HazelcastTable hazelcastTable) {
        assert !hazelcastTable.isReplicated();

        MapProxyImpl map = hazelcastTable.getContainer();

        PartitioningStrategy strategy = map.getPartitionStrategy();

        if (strategy instanceof DeclarativePartitioningStrategy) {
            String distributionField = ((DeclarativePartitioningStrategy) strategy).getField();

            int index = 0;

            for (RelDataTypeField field : hazelcastTable.getFieldList()) {
                if (field.getName().equals(QueryConstants.KEY_ATTRIBUTE_NAME.value())) {
                    for (RelDataTypeField nestedField : field.getType().getFieldList()) {
                        String nestedFieldName = nestedField.getName();

                        if (nestedField.getName().equals(distributionField))
                            return Collections.singletonList(new PhysicalDistributionField(index, nestedFieldName));
                    }
                }

                index++;
            }
        }
        else {
            // TODO: Can we use __key when other partitioning strategies are used?
        }

        return Collections.emptyList();
    }
}
