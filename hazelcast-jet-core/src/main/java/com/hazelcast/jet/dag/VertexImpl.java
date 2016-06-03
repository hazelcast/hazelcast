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

package com.hazelcast.jet.dag;

import com.hazelcast.jet.dag.tap.HazelcastSinkTap;
import com.hazelcast.jet.dag.tap.HazelcastSourceTap;
import com.hazelcast.jet.dag.tap.SinkTap;
import com.hazelcast.jet.dag.tap.SinkTapWriteStrategy;
import com.hazelcast.jet.dag.tap.SourceTap;
import com.hazelcast.jet.dag.tap.TapType;
import com.hazelcast.jet.processor.ProcessorDescriptor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.hazelcast.util.Preconditions.checkNotNull;

public class VertexImpl implements Vertex {
    private String name;
    private ProcessorDescriptor descriptor;

    private List<Edge> inputEdges = new ArrayList<Edge>();
    private List<Edge> outputEdges = new ArrayList<Edge>();
    private List<SinkTap> sinks = new ArrayList<SinkTap>();
    private List<Vertex> inputVertices = new ArrayList<Vertex>();
    private List<SourceTap> sources = new ArrayList<SourceTap>();
    private List<Vertex> outputVertices = new ArrayList<Vertex>();

    public VertexImpl(String name,
                      ProcessorDescriptor descriptor) {
        checkNotNull(descriptor);
        checkNotNull(name);

        this.name = name;
        this.descriptor = descriptor;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public void addSourceTap(SourceTap sourceTap) {
        this.sources.add(sourceTap);
    }

    @Override
    public void addSinkTap(SinkTap sinkTap) {
        this.sinks.add(sinkTap);
    }

    @Override
    public void addSourceList(String name) {
        this.sources.add(new HazelcastSourceTap(name, TapType.HAZELCAST_LIST));
    }

    @Override
    public void addSourceMap(String name) {
        this.sources.add(new HazelcastSourceTap(name, TapType.HAZELCAST_MAP));
    }

    @Override
    public void addSourceMultiMap(String name) {
        this.sources.add(new HazelcastSourceTap(name, TapType.HAZELCAST_MULTIMAP));
    }

    @Override
    public void addSinkList(String name) {
        this.sinks.add(new HazelcastSinkTap(name, TapType.HAZELCAST_LIST));
    }

    @Override
    public void addSinkList(String name, SinkTapWriteStrategy sinkTapWriteStrategy) {
        this.sinks.add(new HazelcastSinkTap(name, TapType.HAZELCAST_LIST, sinkTapWriteStrategy));
    }

    @Override
    public void addSinkMap(String name) {
        this.sinks.add(new HazelcastSinkTap(name, TapType.HAZELCAST_MAP));
    }

    @Override
    public void addSinkMap(String name, SinkTapWriteStrategy sinkTapWriteStrategy) {
        this.sinks.add(new HazelcastSinkTap(name, TapType.HAZELCAST_MAP, sinkTapWriteStrategy));
    }

    @Override
    public void addSinkMultiMap(String name) {
        this.sinks.add(new HazelcastSinkTap(name, TapType.HAZELCAST_MULTIMAP));
    }

    @Override
    public void addSinkMultiMap(String name, SinkTapWriteStrategy sinkTapWriteStrategy) {
        this.sinks.add(new HazelcastSinkTap(name, TapType.HAZELCAST_MULTIMAP, sinkTapWriteStrategy));
    }

    @Override
    public void addSourceFile(String name) {
        this.sources.add(new HazelcastSourceTap(name, TapType.FILE));
    }

    @Override
    public void addSinkFile(String name, SinkTapWriteStrategy sinkTapWriteStrategy) {
        sinks.add(new HazelcastSinkTap(name, TapType.FILE, sinkTapWriteStrategy));
    }

    @Override
    public void addSinkFile(String name) {
        sinks.add(new HazelcastSinkTap(name, TapType.FILE));
    }

    @Override
    public void addOutputVertex(Vertex outputVertex, Edge edge) {
        this.outputVertices.add(outputVertex);
        this.outputEdges.add(edge);
    }

    @Override
    public void addInputVertex(Vertex inputVertex, Edge edge) {
        this.inputVertices.add(inputVertex);
        this.inputEdges.add(edge);
    }

    @Override
    public List<Edge> getInputEdges() {
        return Collections.unmodifiableList(this.inputEdges);
    }

    @Override
    public List<Edge> getOutputEdges() {
        return Collections.unmodifiableList(this.outputEdges);
    }

    @Override
    public List<Vertex> getInputVertices() {
        return Collections.unmodifiableList(this.inputVertices);
    }

    @Override
    public List<Vertex> getOutputVertices() {
        return Collections.unmodifiableList(this.outputVertices);
    }

    @Override
    public List<SourceTap> getSources() {
        return Collections.unmodifiableList(this.sources);
    }

    @Override
    public List<SinkTap> getSinks() {
        return Collections.unmodifiableList(this.sinks);
    }

    @Override
    public ProcessorDescriptor getDescriptor() {
        return this.descriptor;
    }

    @Override
    public boolean hasOutputShuffler() {
        for (Edge edge : this.outputEdges) {
            if (edge.isShuffled()) {
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        VertexImpl vertex = (VertexImpl) o;
        return !(this.name != null ? !this.name.equals(vertex.name) : vertex.name != null);
    }

    @Override
    public int hashCode() {
        return this.name != null ? this.name.hashCode() : 0;
    }
}
