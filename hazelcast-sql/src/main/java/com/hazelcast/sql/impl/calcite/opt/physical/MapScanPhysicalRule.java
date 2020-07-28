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
import com.hazelcast.sql.impl.calcite.opt.distribution.DistributionTraitDef;
import com.hazelcast.sql.impl.calcite.opt.logical.MapScanLogicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.index.IndexResolver;
import com.hazelcast.sql.impl.schema.map.MapTableIndex;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTable;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.hazelcast.sql.impl.calcite.opt.physical.index.IndexResolver.createFullIndexScan;

/**
 * Convert logical map scan to either replicated or partitioned physical scan.
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

        if (scan.isReplicated()) {
            PhysicalRel newScan = new ReplicatedMapScanPhysicalRel(
                scan.getCluster(),
                OptUtils.toPhysicalConvention(scan.getTraitSet(), OptUtils.getDistributionDef(scan).getTraitReplicated()),
                scan.getTable()
            );

            call.transformTo(newScan);
        } else {
            PartitionedMapTable table = (PartitionedMapTable) scan.getMap();

            DistributionTrait distribution = getDistributionTrait(
                OptUtils.getDistributionDef(scan),
                table,
                scan.getTableUnwrapped().getProjects()
            );

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
            List<RelNode> indexScans = IndexResolver.createIndexScans(scan, distribution, indexes);
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

    /**
     * Get distribution trait for the given table.
     *
     * @param table Table.
     * @param projects Projects.
     * @return Distribution trait.
     */
    private static DistributionTrait getDistributionTrait(
        DistributionTraitDef distributionTraitDef,
        PartitionedMapTable table,
        List<Integer> projects
    ) {
        if (!table.hasDistributionField()) {
            return distributionTraitDef.getTraitPartitionedUnknown();
        }

        // TODO: Simplify, there is only one field here!
        List<Integer> distributionFields = Collections.singletonList(table.getDistributionFieldOrdinal());

        // Remap internal scan distribution fields to projected fields.
        List<Integer> res = new ArrayList<>(distributionFields.size());

        for (Integer distributionField : distributionFields) {
            int projectIndex = projects.indexOf(distributionField);

            if (projectIndex == -1) {
                return distributionTraitDef.getTraitPartitionedUnknown();
            }

            res.add(projectIndex);
        }

        return distributionTraitDef.createPartitionedTrait(Collections.singletonList(res));
    }
}
