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

package com.hazelcast.jet.sql.impl.opt.nojobshortcuts;

import com.hazelcast.jet.sql.impl.opt.common.CalcIntoScanRule;
import com.hazelcast.jet.sql.impl.opt.logical.ValuesLogicalRules;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.rules.CoreRules;

import java.util.Collection;

import static java.util.Arrays.asList;

public final class NoJobShortcutRules {

    private NoJobShortcutRules() {
    }

    public static Collection<RelOptRule> getRules() {
        return asList(
                SelectByKeyMapRules.INSTANCE,
                InsertMapRule.INSTANCE,
                SinkMapRule.INSTANCE,
                UpdateByKeyMapRule.INSTANCE,
                DeleteByKeyMapRule.INSTANCE,
                MapSizeRule.INSTANCE,

                // auxiliary rules
                CalcIntoScanRule.INSTANCE,
                CoreRules.PROJECT_MERGE,
                CoreRules.FILTER_MERGE,

                ValuesLogicalRules.CONVERT_INSTANCE,
                ValuesLogicalRules.CALC_INSTANCE,
                ValuesLogicalRules.UNION_INSTANCE
        );
    }
}
