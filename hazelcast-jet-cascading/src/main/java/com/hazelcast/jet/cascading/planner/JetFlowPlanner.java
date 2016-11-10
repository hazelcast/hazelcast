/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.cascading.planner;

import cascading.flow.FlowDef;
import cascading.flow.FlowElement;
import cascading.flow.FlowStep;
import cascading.flow.planner.BaseFlowStepFactory;
import cascading.flow.planner.FlowPlanner;
import cascading.flow.planner.PlannerInfo;
import cascading.flow.planner.PlatformInfo;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.iso.transformer.ElementFactory;
import cascading.flow.planner.process.FlowNodeGraph;
import cascading.flow.planner.process.FlowStepFactory;
import cascading.flow.planner.rule.RuleRegistry;
import cascading.flow.planner.rule.transformer.BoundaryElementFactory;
import cascading.pipe.Boundary;
import cascading.tap.Tap;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.cascading.JetFlow;
import com.hazelcast.jet2.JetEngineConfig;

public class JetFlowPlanner extends FlowPlanner<JetFlow, JetEngineConfig> {

    public static final String PLATFORM = "Hazelcast Jet";
    public static final String VENDOR = "Hazelcast";
    private final HazelcastInstance instance;
    private final JetEngineConfig config;

    public JetFlowPlanner(HazelcastInstance instance, JetEngineConfig config) {
        this.instance = instance;
        this.config = config;
    }


    @Override
    public JetEngineConfig getDefaultConfig() {
        return config;
    }

    @Override
    public PlannerInfo getPlannerInfo(String registry) {
        return new PlannerInfo(getClass().getSimpleName(), PLATFORM, registry);
    }

    @Override
    public PlatformInfo getPlatformInfo() {
        return new PlatformInfo(PLATFORM, VENDOR, getClass().getPackage().getImplementationVersion());
    }

    @Override
    protected JetFlow createFlow(FlowDef flowDef) {
        return new JetFlow(instance, getPlatformInfo(), getDefaultProperties(), config, flowDef);
    }

    @Override
    public FlowStepFactory<JetEngineConfig> getFlowStepFactory() {
        return new BaseFlowStepFactory<JetEngineConfig>(getFlowNodeFactory()) {
            @Override
            public FlowStep<JetEngineConfig> createFlowStep(ElementGraph stepElementGraph,
                                                      FlowNodeGraph flowNodeGraph) {
                return new JetFlowStep(stepElementGraph, flowNodeGraph);
            }
        };
    }

    @Override
    protected Tap makeTempTap(String prefix, String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void configRuleRegistryDefaults(RuleRegistry ruleRegistry) {
        ruleRegistry.addDefaultElementFactory(BoundaryElementFactory.BOUNDARY_PIPE,
                new IntermediateBoundaryElementFactory());
    }

    public static class IntermediateBoundaryElementFactory implements ElementFactory {
        @Override
        public FlowElement create(ElementGraph graph, FlowElement flowElement) {
            return new Boundary();
        }
    }
}
