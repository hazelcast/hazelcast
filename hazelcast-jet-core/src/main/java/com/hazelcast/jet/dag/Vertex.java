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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Represents vertex of the Direct Acyclic Graph;
 */
public class Vertex implements Serializable {
    private String name;
    private ProcessorDescriptor descriptor;

    private List<Edge> inputEdges = new ArrayList<Edge>();
    private List<Edge> outputEdges = new ArrayList<Edge>();
    private List<SinkTap> sinks = new ArrayList<SinkTap>();
    private List<Vertex> inputVertices = new ArrayList<Vertex>();
    private List<SourceTap> sources = new ArrayList<SourceTap>();
    private List<Vertex> outputVertices = new ArrayList<Vertex>();

    public Vertex(String name,
                      ProcessorDescriptor descriptor) {
        checkNotNull(descriptor);
        checkNotNull(name);

        this.name = name;
        this.descriptor = descriptor;
    }

    /**
     * @return - name of the vertex;
     */
    public String getName() {
        return this.name;
    }

    /**
     * Add abstract source tap object to the vertex;
     *
     * @param sourceTap - corresponding source tap;
     */
    public void addSourceTap(SourceTap sourceTap) {
        this.sources.add(sourceTap);
    }

    /**
     * Add abstract sink tap object to the vertex;
     *
     * @param sinkTap - corresponding sink tap;
     */
    public void addSinkTap(SinkTap sinkTap) {
        this.sinks.add(sinkTap);
    }

    /**
     * Add Hazelcast IList object as source tap;
     *
     * @param name -  name of the corresponding Hazelcast Ilist;
     */
    public void addSourceList(String name) {
        this.sources.add(new HazelcastSourceTap(name, TapType.HAZELCAST_LIST));
    }

    /**
     * Add Hazelcast IMap object as source tap;
     *
     * @param name -  name of the corresponding Hazelcast IMap;
     */
    public void addSourceMap(String name) {
        this.sources.add(new HazelcastSourceTap(name, TapType.HAZELCAST_MAP));
    }

    /**
     * Add Hazelcast IMultiMap object as source tap;
     *
     * @param name -  name of the corresponding Hazelcast MultiMap;
     */
    public void addSourceMultiMap(String name) {
        this.sources.add(new HazelcastSourceTap(name, TapType.HAZELCAST_MULTIMAP));
    }

    /**
     * Add Hazelcast IList as sink tap with
     * {@link SinkTapWriteStrategy#CLEAR_AND_REPLACE} sink strategy;
     *
     * @param name -  name of the corresponding Hazelcast Ilist;
     */
    public void addSinkList(String name) {
        this.sinks.add(new HazelcastSinkTap(name, TapType.HAZELCAST_LIST));
    }

    /**
     * Add Hazelcast IList object as sink tap;
     *
     * @param name                 -  name of the corresponding Hazelcast Ilist;
     * @param sinkTapWriteStrategy - corresponding write strategy;
     */
    public void addSinkList(String name, SinkTapWriteStrategy sinkTapWriteStrategy) {
        this.sinks.add(new HazelcastSinkTap(name, TapType.HAZELCAST_LIST, sinkTapWriteStrategy));
    }

    /**
     * Add Hazelcast IMap as sink tap with
     * {@link SinkTapWriteStrategy#CLEAR_AND_REPLACE} sink strategy;
     *
     * @param name -  name of the corresponding Hazelcast IMap;
     */
    public void addSinkMap(String name) {
        this.sinks.add(new HazelcastSinkTap(name, TapType.HAZELCAST_MAP));
    }

    /**
     * Add Hazelcast IMap object as sink tap;
     *
     * @param name                 -  name of the corresponding Hazelcast IMap;
     * @param sinkTapWriteStrategy - corresponding write strategy;
     */
    public void addSinkMap(String name, SinkTapWriteStrategy sinkTapWriteStrategy) {
        this.sinks.add(new HazelcastSinkTap(name, TapType.HAZELCAST_MAP, sinkTapWriteStrategy));
    }

    /**
     * Add Hazelcast MultiMap object as sink tap with
     * {@link SinkTapWriteStrategy#CLEAR_AND_REPLACE} sink strategy;
     *
     * @param name -  name of the corresponding Hazelcast MultiMap;
     */
    public void addSinkMultiMap(String name) {
        this.sinks.add(new HazelcastSinkTap(name, TapType.HAZELCAST_MULTIMAP));
    }

    /**
     * Add Hazelcast MultiMap object as sink tap;
     *
     * @param name                 - name of the multiMap;
     * @param sinkTapWriteStrategy - corresponding write strategy;
     */
    public void addSinkMultiMap(String name, SinkTapWriteStrategy sinkTapWriteStrategy) {
        this.sinks.add(new HazelcastSinkTap(name, TapType.HAZELCAST_MULTIMAP, sinkTapWriteStrategy));
    }

    /**
     * Add simple file on disk like source tap;
     *
     * @param name - path to the file;
     */
    public void addSourceFile(String name) {
        this.sources.add(new HazelcastSourceTap(name, TapType.FILE));
    }

    /**
     * Add simple file on disk like sink tap;
     *
     * @param name                 - path to the file;
     * @param sinkTapWriteStrategy - sink tap writer strategy;
     */
    public void addSinkFile(String name, SinkTapWriteStrategy sinkTapWriteStrategy) {
        sinks.add(new HazelcastSinkTap(name, TapType.FILE, sinkTapWriteStrategy));
    }

    /**
     * Add simple file on disk like sink tap with
     * {@link SinkTapWriteStrategy#CLEAR_AND_REPLACE} sink strategy;
     *
     * @param name - path to the file;
     */
    public void addSinkFile(String name) {
        sinks.add(new HazelcastSinkTap(name, TapType.FILE));
    }

    /**
     * Add outputVertex as  output vertex for the corresponding edge and this vertex;
     *
     * @param outputVertex - next output vertex;
     * @param edge         - corresponding edge;
     */
    public void addOutputVertex(Vertex outputVertex, Edge edge) {
        this.outputVertices.add(outputVertex);
        this.outputEdges.add(edge);
    }

    /**
     * Add inputVertex as inout  vertex for the corresponding edge and this vertex;
     *
     * @param inputVertex - previous inout vertex;
     * @param edge        - corresponding edge;
     */
    public void addInputVertex(Vertex inputVertex, Edge edge) {
        this.inputVertices.add(inputVertex);
        this.inputEdges.add(edge);
    }

    /**
     * @return - list of the input edges;
     */
    public List<Edge> getInputEdges() {
        return Collections.unmodifiableList(this.inputEdges);
    }

    /**
     * @return - list of the output edges;
     */
    public List<Edge> getOutputEdges() {
        return Collections.unmodifiableList(this.outputEdges);
    }

    /**
     * @return - list of the input vertices;
     */
    public List<Vertex> getInputVertices() {
        return Collections.unmodifiableList(this.inputVertices);
    }

    /**
     * @return - list of the output vertices;
     */
    public List<Vertex> getOutputVertices() {
        return Collections.unmodifiableList(this.outputVertices);
    }

    /**
     * @return - list of the input source taps;
     */
    public List<SourceTap> getSources() {
        return Collections.unmodifiableList(this.sources);
    }

    /**
     * @return - list of the output sink taps;
     */
    public List<SinkTap> getSinks() {
        return Collections.unmodifiableList(this.sinks);
    }

    /**
     * @return - processor descriptor of vertex;
     */
    public ProcessorDescriptor getDescriptor() {
        return this.descriptor;
    }

    /**
     * @return - true if vertex has at least one output edge which will represent distributed
     * channel for shuffling data between cluster nodes;
     */
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

        Vertex vertex = (Vertex) o;
        return !(this.name != null ? !this.name.equals(vertex.name) : vertex.name != null);
    }

    @Override
    public int hashCode() {
        return this.name != null ? this.name.hashCode() : 0;
    }
}
