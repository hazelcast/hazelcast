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

import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

public final class PhysicalRules {

    private PhysicalRules() {
    }

    public static RuleSet getRuleSet() {
        return RuleSets.ofList(
                FilterPhysicalRule.INSTANCE,
                ProjectPhysicalRule.INSTANCE,
                FullScanPhysicalRule.INSTANCE,
                AggregatePhysicalRule.INSTANCE,
                JoinPhysicalRule.INSTANCE,
                ValuesPhysicalRule.INSTANCE,
                InsertPhysicalRule.INSTANCE,
                SortPhysicalRule.INSTANCE,
                new AbstractConverter.ExpandConversionRule(RelFactories.LOGICAL_BUILDER)
        );
    }
}
