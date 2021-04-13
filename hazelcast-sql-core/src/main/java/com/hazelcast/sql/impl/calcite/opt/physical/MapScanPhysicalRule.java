/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.sql.impl.calcite.opt.physical.index.IndexResolver;
import com.hazelcast.sql.impl.schema.map.MapTableIndex;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTable;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.sql.impl.calcite.opt.physical.index.IndexResolver.createFullIndexScan;

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

        PartitionedMapTable table = (PartitionedMapTable) scan.getMap();

        DistributionTrait distribution = OptUtils.getDistributionDef(scan).getTraitPartitionedUnknown();

        List<RelNode> transforms = new ArrayList<>(1);

        MapScanPhysicalRel mapScan = new MapScanPhysicalRel(
                scan.getCluster(),
                OptUtils.toPhysicalConvention(scan.getTraitSet(), distribution),
                scan.getTable()
        );

        if (!table.isHd()) {
            // Add normal map scan. For HD, Map scan is not supported
            transforms.add(mapScan);
        }

        // Try adding index scans.
        List<MapTableIndex> indexes = table.getIndexes();
        Collection<RelNode> indexScans = IndexResolver.createIndexScans(scan, distribution, indexes);
        transforms.addAll(indexScans);

        if (transforms.isEmpty() && table.isHd()) {
            // No transforms created so far for HD, try using the index scan.
            RelNode indexScan = createFullIndexScan(scan, distribution, indexes);

            if (indexScan != null) {
                transforms.add(indexScan);
            } else {
                // Failed to create any index-based access path for HD. Add standard map scan. It will fail during plan
                // creation with proper exception.
                transforms.add(mapScan);
            }
        }

        for (RelNode transform : transforms) {
            call.transformTo(transform);
        }
    }
}
