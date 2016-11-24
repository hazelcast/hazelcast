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

package com.hazelcast.jet.cascading.runtime;

import cascading.flow.FlowNode;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.cascading.JetFlowProcess;
import com.hazelcast.jet2.AbstractProcessor;
import com.hazelcast.jet2.Inbox;
import com.hazelcast.jet2.JetEngineConfig;
import com.hazelcast.jet2.Outbox;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.hazelcast.util.ExceptionUtil.rethrow;

public class FlowNodeProcessor extends AbstractProcessor {

    private JetStreamGraph graph;
    private final JetEngineConfig config;
    private final FlowNode node;

    private final Map<String, Map<Integer, Integer>> inputMap;
    private final Map<String, Set<Integer>> outputMap;
    private final HazelcastInstance instance;

    public FlowNodeProcessor(HazelcastInstance instance, JetEngineConfig config, FlowNode node,
                             Map<String, Map<Integer, Integer>> inputMap, Map<String, Set<Integer>> outputMap) {
        this.instance = instance;
        this.config = config;
        this.node = node;
        this.inputMap = inputMap;
        this.outputMap = outputMap;
    }

    @Override
    public void init(@Nonnull Outbox outbox) {
//        HazelcastInstance hazelcastInstance = context.getHazelcastInstance();
//        JetEngineConfig config = context.getConfig();
//        config.getProperties().put(StreamGraph.DOT_FILE_PATH, "/tmp");

        JetFlowProcess flowProcess = new JetFlowProcess(config, instance);
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
