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

package com.hazelcast.jet.cascading.runtime;

import cascading.flow.FlowNode;
import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.Inbox;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Outbox;
import com.hazelcast.jet.cascading.JetFlowProcess;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import static com.hazelcast.util.ExceptionUtil.rethrow;

public class FlowNodeProcessor extends AbstractProcessor {

    private JetStreamGraph graph;
    private final JetInstance instance;
    private final Properties properties;
    private final FlowNode node;

    private final Map<String, Map<Integer, Integer>> inputMap;
    private final Map<String, Set<Integer>> outputMap;

    public FlowNodeProcessor(JetInstance instance, Properties properties, FlowNode node,
                             Map<String, Map<Integer, Integer>> inputMap, Map<String, Set<Integer>> outputMap) {
        this.instance = instance;
        this.properties = properties;
        this.node = node;
        this.inputMap = inputMap;
        this.outputMap = outputMap;
    }

    @Override
    public void init(@Nonnull Outbox outbox) {
        JetConfig jetConfig = new JetConfig().setProperties(properties);
        JetFlowProcess flowProcess = new JetFlowProcess(jetConfig, instance);
        flowProcess.setCurrentSliceNum(System.identityHashCode(this));

        graph = new JetStreamGraph(flowProcess, node, outbox, inputMap, outputMap);
        graph.prepare();
    }

    @Override
    public void process(int ordinal, Inbox inbox) {
        try {
            // find the element for this jet ordinal
            Entry<Integer, ProcessorInputSource> entry = graph.getForOrdinal(ordinal);
            // process element according to cascading ordinal
            entry.getValue().process(inbox, entry.getKey());
            //TODO: backpressure
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    @Override
    public boolean complete() {
        graph.complete();
        //TODO: backpressure
        return true;
    }
}
