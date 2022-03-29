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

import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

public final class PhysicalRules {

    private PhysicalRules() {
    }

    public static RuleSet getRuleSet() {
        return RuleSets.ofList(
                // Filter rules
                CalcPhysicalRule.INSTANCE,

                // Scan rules
                FullScanPhysicalRule.INSTANCE,
                IndexScanMapPhysicalRule.INSTANCE,

                // Windowing rules
                WatermarkPhysicalRule.INSTANCE,
                SlidingWindowPhysicalRule.INSTANCE,
                DropLateItemsPhysicalRule.INSTANCE,

                // Aggregate rules
                AggregateBatchPhysicalRule.INSTANCE,
                AggregateSlidingWindowPhysicalRule.WITH_CALC_INSTANCE,
                AggregateSlidingWindowPhysicalRule.NO_CALC_INSTANCE,
                StreamAggregateCannotExecuteRule.INSTANCE,

                // Sort rules
                SortPhysicalRule.INSTANCE,
                StreamingSortMustNotExecuteRule.INSTANCE,

                // Join rules
                JoinPhysicalRule.INSTANCE,
                JoinValidationRule.INSTANCE,

                // Union rules
                UnionPhysicalRule.INSTANCE,

                // Value rules
                ValuesPhysicalRule.INSTANCE,

                // DML rules
                InsertPhysicalRule.INSTANCE,
                SinkPhysicalRule.INSTANCE,
                UpdatePhysicalRule.INSTANCE,
                DeletePhysicalRule.INSTANCE,

                SelectByKeyMapPhysicalRule.INSTANCE,
                InsertMapPhysicalRule.INSTANCE,
                SinkMapPhysicalRule.INSTANCE,
                UpdateByKeyMapPhysicalRule.INSTANCE,
                DeleteByKeyMapPhysicalRule.INSTANCE,

                StreamingInsertMustNotExecuteRule.INSTANCE,

                MustNotExecuteRule.INSTANCE,

                new AbstractConverter.ExpandConversionRule(RelFactories.LOGICAL_BUILDER)
        );
    }
}
