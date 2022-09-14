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

package com.hazelcast.jet.sql.impl.opt.logical;

import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

public final class IMapByKeyOptRules {

    private IMapByKeyOptRules() {
    }

    public static RuleSet getRuleSet() {
        return RuleSets.ofList(
                // imap-by-key access optimization rules
                SelectByKeyMapLogicalRules.INSTANCE
//                TODO:
//                InsertMapLogicalRule.INSTANCE,
//                SinkMapLogicalRule.INSTANCE,
//                UpdateByKeyMapLogicalRule.INSTANCE,
//                DeleteByKeyMapLogicalRule.INSTANCE
        );
    }
}
