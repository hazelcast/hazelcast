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
import cascading.flow.stream.annotations.StreamMode;
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
import cascading.tuple.Tuple;
import cascading.util.Util;
import com.hazelcast.jet.Edge;
import com.hazelcast.jet.io.Pair;
import com.hazelcast.jet.runtime.OutputCollector;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

class JetStreamGraph extends NodeStreamGraph {

    private Holder<OutputCollector<Pair<Tuple, Tuple>>> outputHolder = new Holder<>();
    private final List<Edge> inputEdges;
    private ProcessorInputSource streamedInputSource;
    private Map<String, ProcessorInputSource> accumulatedInputSources = new HashMap<>();

    JetStreamGraph(FlowProcess flowProcess, FlowNode node,
                   Holder<OutputCollector<Pair<Tuple, Tuple>>> outputHolder, List<Edge> inputEdges) {
        super(flowProcess, node);
        this.outputHolder = outputHolder;
        this.inputEdges = inputEdges;

        buildGraph();
        setTraps();
        setScopes();

        printGraph(node.getID(), node.getName(), flowProcess.getCurrentSliceNum());

        bind();
    }

    @Override
    protected Gate createCoGroupGate(CoGroup element, IORole role) {
        if (role == IORole.sink) {
            return new JetGroupingGate(flowProcess, element, outputHolder);
        } else if (role == IORole.source) {
            return new JetCoGroupGate(flowProcess, element);
        } else {
            throw new UnsupportedOperationException("Unsupported role " + role);
        }
    }

    @Override
    protected Gate createGroupByGate(GroupBy element, IORole role) {
        if (role == IORole.sink) {
            return new JetGroupingGate(flowProcess, element, outputHolder);
        } else if (role == IORole.source) {
            return new JetGroupByGate(flowProcess, element);
        } else {
            throw new UnsupportedOperationException("Unsupported role " + role);
        }
    }

    @Override
    protected Duct createMergeStage(Merge merge, IORole role) {
        if (role == IORole.sink) {
            return new JetMergeGate(flowProcess, merge, outputHolder);
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
            return new JetBoundaryStage(flowProcess, element, outputHolder);
        } else if (role == IORole.source) {
            return new JetBoundaryStage(flowProcess, element);
        } else {
            throw new UnsupportedOperationException(role + " must be source or sink");
        }
    }

    @Override
    protected SinkStage createSinkStage(Tap element) {
        return new JetSinkStage(flowProcess, element, outputHolder);
    }

    protected void buildGraph() {
        Set<FlowElement> sourceElements = new HashSet<>(node.getSourceElements());
        Set<? extends FlowElement> accumulated = node.getSourceElements(StreamMode.Accumulated);

        sourceElements.removeAll(accumulated);

        if (sourceElements.size() != 1) {
            throw new IllegalStateException("too many input source keys, got: " + Util.join(sourceElements, ", "));
        }

        FlowElement streamedSource = Util.getFirst(sourceElements);
        streamedInputSource = handleHead(streamedSource);

        for (FlowElement element : accumulated) {
            for (Edge inputEdge : inputEdges) {
                if (inputEdge.getName().equals(FlowElements.id(element))) {
                    ProcessorInputSource accumulatedSource = handleHead(element);
                    String id = inputEdge.getInputVertex().getName();
                    this.accumulatedInputSources.put(id, accumulatedSource);
                }
            }
        }
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

    public ProcessorInputSource getStreamedInputSource() {
        return streamedInputSource;
    }

    public Map<String, ProcessorInputSource> getAccumulatedInputSources() {
        return accumulatedInputSources;
    }
}
