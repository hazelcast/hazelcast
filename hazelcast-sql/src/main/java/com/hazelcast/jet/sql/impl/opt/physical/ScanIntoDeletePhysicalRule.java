/*
 * Copyright 2023 Hazelcast Inc.
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
import com.hazelcast.jet.sql.impl.opt.logical.DeleteLogicalRel;
import com.hazelcast.jet.sql.impl.opt.logical.FullScanLogicalRel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.immutables.value.Value;

import static com.hazelcast.jet.sql.impl.connector.SqlConnectorUtil.getJetSqlConnector;
import static com.hazelcast.jet.sql.impl.opt.Conventions.LOGICAL;

@Value.Enclosing
public final class ScanIntoDeletePhysicalRule extends RelRule<RelRule.Config> {

    @Value.Immutable
    public interface Config extends RelRule.Config {
        ScanIntoDeletePhysicalRule.Config DEFAULT = ImmutableScanIntoDeletePhysicalRule.Config.builder()
                .description(ScanIntoDeletePhysicalRule.class.getSimpleName())
                .operandSupplier(b0 -> b0.operand(DeleteLogicalRel.class)
                        .trait(LOGICAL)
                        .inputs(b1 -> b1.operand(FullScanLogicalRel.class)
                                .predicate(scan -> getJetSqlConnector(OptUtils.extractHazelcastTable(scan).getTarget())
                                        .dmlSupportsPredicates())
                                .noInputs()
                        )
                )
                .build();

        @Override
        default RelOptRule toRule() {
            return new ScanIntoDeletePhysicalRule(this);
        }
    }

    @SuppressWarnings("checkstyle:DeclarationOrder")
    static final RelOptRule INSTANCE = new ScanIntoDeletePhysicalRule(Config.DEFAULT);

    private ScanIntoDeletePhysicalRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        DeleteLogicalRel delete = call.rel(0);
        FullScanLogicalRel scan = call.rel(1);

//        scan.
    }
}
