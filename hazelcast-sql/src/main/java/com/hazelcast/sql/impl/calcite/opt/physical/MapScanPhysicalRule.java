/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.calcite.opt.physical;

import com.hazelcast.sql.impl.calcite.opt.HazelcastConventions;
import com.hazelcast.sql.impl.calcite.opt.OptUtils;
import com.hazelcast.sql.impl.calcite.opt.distribution.DistributionTrait;
import com.hazelcast.sql.impl.calcite.opt.logical.MapScanLogicalRel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;

/**
 * Convert logical map scan to physical map scan.
 */
public final class MapScanPhysicalRule extends RelOptRule {
    public static final RelOptRule INSTANCE = new MapScanPhysicalRule();

    private MapScanPhysicalRule() {
        super(
            OptUtils.single(MapScanLogicalRel.class, HazelcastConventions.LOGICAL),
            MapScanPhysicalRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        MapScanLogicalRel scan = call.rel(0);

        DistributionTrait distribution = OptUtils.getDistributionDef(scan).getTraitPartitionedUnknown();

        call.transformTo(new MapScanPhysicalRel(
            scan.getCluster(),
            OptUtils.toPhysicalConvention(scan.getTraitSet(), distribution),
            scan.getTable()
        ));
    }
}
