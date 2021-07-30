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
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTable;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;

import static com.hazelcast.jet.sql.impl.opt.JetConventions.LOGICAL;

final class IndexScanMapPhysicalRule extends RelOptRule {

    static final RelOptRule INSTANCE = new IndexScanMapPhysicalRule();

    private IndexScanMapPhysicalRule() {
        super(
                operandJ(
                        FullScanLogicalRel.class,
                        LOGICAL,
                        scan -> OptUtils.hasTableType(scan, PartitionedMapTable.class),
                        none()
                ),
                IndexScanMapPhysicalRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        FullScanLogicalRel logicalScan = call.rel(0);
        PartitionedMapTable table = table(logicalScan);

        if (table.isHd()) {
            RelNode indexScan = JetIndexResolver.createFullHDIndexScan(logicalScan, table.getIndexes());
            if (indexScan != null) {
                call.transformTo(indexScan);
            }
            // Otherwise, normal HD scan will be generated at FullScanPhysicalRule,
            // since Jet engine also supports normal HD maps scan.
        } else {
            for (RelNode indexScan : JetIndexResolver.createIndexScans(logicalScan, table.getIndexes())) {
                call.transformTo(indexScan);
            }
        }
    }

    private static <T extends Table> T table(FullScanLogicalRel scan) {
        return OptUtils.extractHazelcastTable(scan).getTarget();
    }
}
