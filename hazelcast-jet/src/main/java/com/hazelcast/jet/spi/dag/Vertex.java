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

package com.hazelcast.jet.spi.dag;

import java.util.List;

import com.hazelcast.jet.spi.dag.tap.SinkTap;
import com.hazelcast.jet.spi.dag.tap.SourceTap;
import com.hazelcast.jet.spi.dag.tap.SinkTapWriteStrategy;
import com.hazelcast.jet.spi.processor.ProcessorDescriptor;

/**
 * Represents vertex of the Direct Acyclic Graph;
 */
public interface Vertex extends DagElement {
    /**
     * Add abstract source tap object to the vertex;
     *
     * @param sourceTap - corresponding source tap;
     */
    void addSourceTap(SourceTap sourceTap);

    /**
     * Add abstract sink tap object to the vertex;
     *
     * @param sinkTap - corresponding sink tap;
     */
    void addSinkTap(SinkTap sinkTap);

    /**
     * Add Hazelcast IList object as source tap;
     *
     * @param name -  name of the corresponding Hazelcast Ilist;
     */
    void addSourceList(String name);

    /**
     * Add Hazelcast IMap object as source tap;
     *
     * @param name -  name of the corresponding Hazelcast IMap;
     */
    void addSourceMap(String name);

    /**
     * Add Hazelcast IMultiMap object as source tap;
     *
     * @param name -  name of the corresponding Hazelcast MultiMap;
     */
    void addSourceMultiMap(String name);

    /**
     * Add Hazelcast IList as sink tap with
     * {@link com.hazelcast.jet.spi.dag.tap.SinkTapWriteStrategy#CLEAR_AND_REPLACE} sink strategy;
     *
     * @param name -  name of the corresponding Hazelcast Ilist;
     */
    void addSinkList(String name);

    /**
     * Add Hazelcast IList object as sink tap;
     *
     * @param name                 -  name of the corresponding Hazelcast Ilist;
     * @param sinkTapWriteStrategy - corresponding write strategy;
     */
    void addSinkList(String name, SinkTapWriteStrategy sinkTapWriteStrategy);

    /**
     * Add Hazelcast IMap as sink tap with
     * {@link com.hazelcast.jet.spi.dag.tap.SinkTapWriteStrategy#CLEAR_AND_REPLACE} sink strategy;
     *
     * @param name -  name of the corresponding Hazelcast IMap;
     */
    void addSinkMap(String name);

    /**
     * Add Hazelcast IMap object as sink tap;
     *
     * @param name                 -  name of the corresponding Hazelcast IMap;
     * @param sinkTapWriteStrategy - corresponding write strategy;
     */
    void addSinkMap(String name, SinkTapWriteStrategy sinkTapWriteStrategy);

    /**
     * Add Hazelcast MultiMap object as sink tap with
     * {@link com.hazelcast.jet.spi.dag.tap.SinkTapWriteStrategy#CLEAR_AND_REPLACE} sink strategy;
     *
     * @param name -  name of the corresponding Hazelcast MultiMap;
     */
    void addSinkMultiMap(String name);

    /**
     * Add Hazelcast MultiMap object as sink tap;
     *
     * @param name                 - name of the multiMap;
     * @param sinkTapWriteStrategy - corresponding write strategy;
     */
    void addSinkMultiMap(String name, SinkTapWriteStrategy sinkTapWriteStrategy);

    /**
     * Add simple file on disk like source tap;
     *
     * @param name - path to the file;
     */
    void addSourceFile(String name);

    /**
     * Add simple file on disk like sink tap with
     * {@link com.hazelcast.jet.spi.dag.tap.SinkTapWriteStrategy#CLEAR_AND_REPLACE} sink strategy;
     *
     * @param name - path to the file;
     */
    void addSinkFile(String name);

    /**
     * Add simple file on disk like sink tap;
     *
     * @param name                 - path to the file;
     * @param sinkTapWriteStrategy - sink tap writer strategy;
     */
    void addSinkFile(String name, SinkTapWriteStrategy sinkTapWriteStrategy);

    /**
     * Add outputVertex as  output vertex for the corresponding edge and this vertex;
     *
     * @param outputVertex - next output vertex;
     * @param edge         - corresponding edge;
     */
    void addOutputVertex(Vertex outputVertex, Edge edge);

    /**
     * Add inputVertex as inout  vertex for the corresponding edge and this vertex;
     *
     * @param inputVertex - previous inout vertex;
     * @param edge        - corresponding edge;
     */
    void addInputVertex(Vertex inputVertex, Edge edge);

    /**
     * @return - name of the vertex;
     */
    String getName();

    /**
     * @return - list of the input edges;
     */
    List<Edge> getInputEdges();

    /**
     * @return - list of the output edges;
     */
    List<Edge> getOutputEdges();

    /**
     * @return - list of the input vertices;
     */
    List<Vertex> getInputVertices();

    /**
     * @return - list of the output vertices;
     */
    List<Vertex> getOutputVertices();

    /**
     * @return - list of the input source taps;
     */
    List<SourceTap> getSources();

    /**
     * @return - list of the output sink taps;
     */
    List<SinkTap> getSinks();

    /**
     * @return - processor descriptor of vertex;
     */
    ProcessorDescriptor getDescriptor();

    /**
     * @return - true if vertex has at least one output edge which will represent distributed
     * channel for shuffling data between cluster nodes;
     */
    boolean hasOutputShuffler();
}
