/*
 * Copyright 2021 Hazelcast Inc.
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
import com.hazelcast.jet.sql.impl.opt.logical.FullScanLogicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.index.JetIndexResolver;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.map.MapTableIndex;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTable;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.jet.sql.impl.opt.JetConventions.LOGICAL;
import static com.hazelcast.jet.sql.impl.opt.JetConventions.PHYSICAL;

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

        return new FullScanPhysicalRel(
                logicalScan.getCluster(),
                OptUtils.toPhysicalConvention(logicalScan.getTraitSet()),
                logicalScan.getTable()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        RelNode rel = call.rel(0);
        Table tableUnwrapped = getTableUnwrapped(rel.getTable());
        List<RelNode> transforms = new ArrayList<>();

        // Only PartitionedMapTable supposed to have indices.
        if (tableUnwrapped instanceof PartitionedMapTable) {
            PartitionedMapTable table = (PartitionedMapTable) tableUnwrapped;
            if (table.getIndexes().isEmpty()) {
                call.transformTo(convert(rel));
                return;
            }

            List<MapTableIndex> indexes = table.getIndexes();
            FullScanLogicalRel logicalScan = (FullScanLogicalRel) rel;
            Collection<RelNode> indexScans = JetIndexResolver.createIndexScans(logicalScan, indexes);
            // TODO: HD indices will be added to this transforms list later.
            //noinspection CollectionAddAllCanBeReplacedWithConstructor
            transforms.addAll(indexScans);
        }

        // Produce simple map scan if Calcite haven't produce index scan or indexes aren't suppose to be.
        if (transforms.isEmpty()) {
            transforms.add(convert(rel));
        }

        for (RelNode transform : transforms) {
            call.transformTo(transform);
        }
    }

    private Table getTableUnwrapped(RelOptTable table) {
        return table.unwrap(HazelcastTable.class).getTarget();
    }
}
