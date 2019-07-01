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

import com.hazelcast.sql.impl.calcite.SqlCalciteConventions;
import com.hazelcast.sql.impl.calcite.logical.rel.MapScanLogicalRel;
import com.hazelcast.sql.impl.calcite.physical.distribution.PhysicalDistributionTrait;
import com.hazelcast.sql.impl.calcite.physical.rel.MapScanPhysicalRel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;

public class MapScanPhysicalRule extends RelOptRule {
    public static final RelOptRule INSTANCE = new MapScanPhysicalRule();

    private MapScanPhysicalRule() {
        // TODO: Set LOGICAL convention.
        super(
            RelOptRule.operand(MapScanLogicalRel.class, RelOptRule.any()),
            "MapScanPhysicalRule"
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        MapScanLogicalRel scan = call.rel(0);

        RelTraitSet traits = scan.getTraitSet()
            .plus(SqlCalciteConventions.HAZELCAST_PHYSICAL)
            .plus(PhysicalDistributionTrait.PARTITIONED);

        call.transformTo(new MapScanPhysicalRel(scan.getCluster(), traits, scan.getTable(), scan.deriveRowType()));
    }
}
