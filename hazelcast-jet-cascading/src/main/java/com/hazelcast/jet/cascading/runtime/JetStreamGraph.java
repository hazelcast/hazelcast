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

import cascading.flow.FlowElement;
import cascading.flow.FlowElements;
import cascading.flow.FlowNode;
import cascading.flow.FlowProcess;
import cascading.flow.stream.duct.Duct;
import cascading.flow.stream.duct.Gate;
import cascading.flow.stream.element.SinkStage;
import cascading.flow.stream.graph.IORole;
import cascading.flow.stream.graph.NodeStreamGraph;
import cascading.pipe.Boundary;
import cascading.pipe.CoGroup;
import cascading.pipe.GroupBy;
import cascading.pipe.HashJoin;
import cascading.pipe.Merge;
import cascading.tap.Tap;
import com.hazelcast.jet.Outbox;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

class JetStreamGraph extends NodeStreamGraph {

    private final Outbox outbox;
    // element id -> jet input ordinal -> cascading ordinal
    private final Map<String, Map<Integer, Integer>> inputMap;
    // element id -> jet output ordinals
    private final Map<String, Set<Integer>> outputMap;

    // jet ordinal -->  (cascading ordinal, input source)
    private Map<Integer, Map.Entry<Integer, ProcessorInputSource>> inputSources = new HashMap<>();

    JetStreamGraph(FlowProcess flowProcess, FlowNode node, Outbox outbox,
                          Map<String, Map<Integer, Integer>> inputMap, Map<String, Set<Integer>> outputMap) {
        super(flowProcess, node);
        this.outbox = outbox;
        this.inputMap = inputMap;
        this.outputMap = outputMap;

        buildGraph();
        setTraps();
        setScopes();

        printGraph(node.getID(), node.getName(), flowProcess.getCurrentSliceNum());

        bind();
    }

    @Override
    public void prepare() {
        super.prepare();

        inputSources.forEach((k, v) -> v.getValue().before());
    }

    @Override
    protected Gate createCoGroupGate(CoGroup element, IORole role) {
        if (role == IORole.sink) {
            return new JetGroupingGate(flowProcess, element, getOutboxWithOrdinal(element));
        } else if (role == IORole.source) {
            return new JetCoGroupGate(flowProcess, element);
        } else {
            throw new UnsupportedOperationException("Unsupported role " + role);
        }
    }

    @Override
    protected Gate createGroupByGate(GroupBy element, IORole role) {
        if (role == IORole.sink) {
            return new JetGroupingGate(flowProcess, element, getOutboxWithOrdinal(element));
        } else if (role == IORole.source) {
            return new JetGroupByGate(flowProcess, element);
        } else {
            throw new UnsupportedOperationException("Unsupported role " + role);
        }
    }

    @Override
    protected Duct createMergeStage(Merge merge, IORole role) {
        if (role == IORole.sink) {
            return new JetMergeGate(flowProcess, merge, getOutboxWithOrdinal(merge));
        } else if (role == IORole.source) {
            return new JetMergeGate(flowProcess, merge);
        } else {
            throw new UnsupportedOperationException(role + " must be source or sink");
        }
    }

    @Override
    protected Gate createHashJoinGate(HashJoin join) {
        return new JetHashJoinGate(flowProcess, join);
    }

    @Override
    protected Duct createBoundaryStage(Boundary element, IORole role) {
        if (role == IORole.sink) {
            return new JetBoundaryStage(flowProcess, element, getOutboxWithOrdinal(element));
        } else if (role == IORole.source) {
            return new JetBoundaryStage(flowProcess, element);
        } else {
            throw new UnsupportedOperationException(role + " must be source or sink");
        }
    }

    @Override
    protected SinkStage createSinkStage(Tap element) {
        return new JetSinkStage(flowProcess, element, getOutboxWithOrdinal(element));
    }

    private Outbox getOutboxWithOrdinal(FlowElement element) {
        Set<Integer> ordinals = outputMap.get(FlowElements.id(element));
        return new OutboxWithOrdinals(outbox, ordinals);
    }

    protected void buildGraph() {
        Set<FlowElement> sourceElements = node.getSourceElements();
        for (FlowElement element : sourceElements) {
            String id = FlowElements.id(element);
            ProcessorInputSource inputSource = handleHead(element);
            Map<Integer, Integer> jetToCascadingOrdinalMap = inputMap.get(id);
            for (Entry<Integer, Integer> entry : jetToCascadingOrdinalMap.entrySet()) {
                int cascadingOrdinal = entry.getValue();
                inputSources.put(entry.getKey(), new SimpleImmutableEntry<>(cascadingOrdinal, inputSource));
            }
        }
    }

    public Map.Entry<Integer, ProcessorInputSource> getForOrdinal(int ordinal) {
        return inputSources.get(ordinal);
    }

    private ProcessorInputSource handleHead(FlowElement element) {
        Duct sourceDuct = getSource(element);
        addHead(sourceDuct);
        handleDuct(element, sourceDuct);
        return (ProcessorInputSource) sourceDuct;
    }

    protected Duct getSource(FlowElement element) {
        if (element instanceof Tap) {
            return new JetSourceStage(flowProcess, (Tap) element);
        } else if (element instanceof GroupBy) {
            return createGroupByGate((GroupBy) element, IORole.source);
        } else if (element instanceof CoGroup) {
            return createCoGroupGate((CoGroup) element, IORole.source);
        } else if (element instanceof Merge) {
            return createMergeStage((Merge) element, IORole.source);
        } else if (element instanceof Boundary) {
            return createBoundaryStage((Boundary) element, IORole.source);
        }
        throw new UnsupportedOperationException(" FlowElement " + element + " can't be used as a source");
    }

    public void complete() {
        inputSources.entrySet().forEach(v -> v.getValue().getValue().complete());
    }

    private static final class OutboxWithOrdinals implements Outbox {
        private final Outbox outbox;
        private final int[] ordinals;

        private OutboxWithOrdinals(Outbox outbox, Collection<Integer> ordinals) {
            this.outbox = outbox;
            this.ordinals = ordinals.stream().mapToInt(i -> i).toArray();
        }

        @Override
        public void add(int ordinal, Object item) {
            if (ordinal != -1) {
                throw new UnsupportedOperationException();
            }
            for (int ord : ordinals) {
                outbox.add(ord, item);
            }
        }

        @Override
        public boolean isHighWater(int ordinal) {
            if (ordinal != -1) {
                throw new UnsupportedOperationException();
            }
            for (int ord : ordinals) {
                if (outbox.isHighWater(ord)) {
                    return true;
                }
            }
            return false;
        }


    }
}
