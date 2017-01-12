/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.cascading;

import cascading.flow.FlowConnector;
import cascading.flow.planner.FlowPlanner;
import cascading.flow.planner.rule.RuleRegistrySet;
import cascading.scheme.Scheme;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.cascading.planner.JetFlowPlanner;
import com.hazelcast.jet.cascading.planner.rule.tez.HashJoinHadoop2TezRuleRegistry;
import com.hazelcast.jet.cascading.planner.rule.tez.NoHashJoinHadoop2TezRuleRegistry;

public class JetFlowConnector extends FlowConnector {

    private final JetInstance instance;
    private final JetConfig config;

    public JetFlowConnector(JetInstance instance, JetConfig config) {
        super(config.getProperties());
        this.instance = instance;
        this.config = config;
    }

    @Override
    protected Class<? extends Scheme> getDefaultIntermediateSchemeClass() {
        return null;
    }

    @Override
    protected FlowPlanner createFlowPlanner() {
        return new JetFlowPlanner(instance, config);
    }

    @Override
    protected RuleRegistrySet createDefaultRuleRegistrySet() {
        return new RuleRegistrySet(new NoHashJoinHadoop2TezRuleRegistry(), new HashJoinHadoop2TezRuleRegistry());
    }
}
