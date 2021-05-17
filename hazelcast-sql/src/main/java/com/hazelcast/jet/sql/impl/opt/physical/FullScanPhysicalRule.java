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

package com.hazelcast.jet.sql.impl.opt.physical;

import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.opt.logical.FullScanLogicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.index.JetIndexResolver;
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
        if (rel.getTraitSet().contains(getInTrait())) {
            call.transformTo(convert(rel));

            PartitionedMapTable table = (PartitionedMapTable) getTableUnwrapped(rel.getTable());
            if (table.getIndexes().isEmpty()) {
                return;
            }

            List<RelNode> transforms = new ArrayList<>();
            List<MapTableIndex> indexes = table.getIndexes();
            FullScanLogicalRel logicalScan = (FullScanLogicalRel) rel;
            Collection<RelNode> indexScans = JetIndexResolver.createIndexScans(logicalScan, indexes);
            // TODO: HD indices will be added to this transforms list later.
            //noinspection CollectionAddAllCanBeReplacedWithConstructor
            transforms.addAll(indexScans);

            for (RelNode transform : transforms) {
                call.transformTo(transform);
            }

            /*
               java.lang.AssertionError: Relational expression rel#65:IMapIndexScanPhysicalRel#65 has calling-convention LOGICAL
               but does not implement the required interface 'interface jet.sql.impl.opt.logical.LogicalRel' of that convention
             */
        }
    }

    private Table getTableUnwrapped(RelOptTable table) {
        return table.unwrap(HazelcastTable.class).getTarget();
    }
}
