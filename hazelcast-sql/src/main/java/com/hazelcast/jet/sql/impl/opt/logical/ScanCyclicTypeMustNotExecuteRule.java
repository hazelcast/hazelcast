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

package com.hazelcast.jet.sql.impl.opt.logical;

import com.hazelcast.jet.sql.impl.validate.types.HazelcastObjectType;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryUtils;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelRule.Config;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.immutables.value.Value;

import java.util.HashSet;

/**
 * A rule that throws if any cyclic type usage attempt is detected.
 */
@Value.Enclosing
public final class ScanCyclicTypeMustNotExecuteRule extends RelRule<Config> implements TransformationRule {

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableScanCyclicTypeMustNotExecuteRule.Config.builder()
                .description(ScanCyclicTypeMustNotExecuteRule.class.getSimpleName())
                .operandSupplier(b0 -> b0.operand(TableScan.class)
                        .anyInputs()
                )
                .build();

        @Override
        default RelOptRule toRule() {
            return new ScanCyclicTypeMustNotExecuteRule(this);
        }
    }

    private ScanCyclicTypeMustNotExecuteRule(Config config) {
        super(config);
    }

    @SuppressWarnings("checkstyle:DeclarationOrder")
    public static final RelOptRule INSTANCE = new ScanCyclicTypeMustNotExecuteRule(Config.DEFAULT);

    @Override
    public boolean matches(RelOptRuleCall call) {
        TableScan rel = call.rel(0);
        for (final RelDataTypeField field : rel.getRowType().getFieldList()) {
            final RelDataType fieldType = field.getType();
            if (!(fieldType instanceof HazelcastObjectType)) {
                continue;
            }

            if (QueryUtils.containsCycles((HazelcastObjectType) fieldType, new HashSet<>())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        throw QueryException.error("Experimental feature of using cyclic custom types isn't enabled. "
                + "To enable, set " + ClusterProperty.SQL_CUSTOM_CYCLIC_TYPES_ENABLED + " to true");
    }
}
