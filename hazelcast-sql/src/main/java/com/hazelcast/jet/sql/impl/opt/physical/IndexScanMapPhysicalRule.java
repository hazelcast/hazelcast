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
import com.hazelcast.jet.sql.impl.opt.physical.index.IndexResolver;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTable;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.immutables.value.Value;

import static com.hazelcast.jet.sql.impl.opt.Conventions.LOGICAL;

@Value.Enclosing
final class IndexScanMapPhysicalRule extends RelRule<RelRule.Config> {

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableIndexScanMapPhysicalRule.Config.builder()
                .description(IndexScanMapPhysicalRule.class.getSimpleName())
                .operandSupplier(
                        b -> b.operand(FullScanLogicalRel.class)
                                .trait(LOGICAL)
                                .predicate(scan -> OptUtils.hasTableType(scan, PartitionedMapTable.class))
                                .noInputs())
                .build();

        @Override
        default RelOptRule toRule() {
            return new IndexScanMapPhysicalRule(this);
        }
    }

    @SuppressWarnings("checkstyle:DeclarationOrder")
    static final RelOptRule INSTANCE = new IndexScanMapPhysicalRule(Config.DEFAULT);

    private IndexScanMapPhysicalRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        FullScanLogicalRel logicalScan = call.rel(0);
        PartitionedMapTable table = table(logicalScan);

        for (RelNode indexScan : IndexResolver.createIndexScans(logicalScan, table.getIndexes())) {
            call.transformTo(indexScan);
        }
    }

    private static <T extends Table> T table(FullScanLogicalRel scan) {
        return OptUtils.extractHazelcastTable(scan).getTarget();
    }
}
