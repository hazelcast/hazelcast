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

package com.hazelcast.jet2;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 * Represents an edge between two vertices in a DAG
 */
public class Edge implements IdentifiedDataSerializable {

    private Vertex source;
    private int outputOrdinal;
    private Vertex destination;
    private int inputOrdinal;

    private int priority;

    Edge() {

    }

    /**
     * Creates an edge between two vertices.
     *
     * @param source      the source vertex
     * @param destination the destination vertex
     */
    public Edge(Vertex source,
                Vertex destination) {
        this(source, 0, destination, 0);
    }

    /**
     * Creates an edge between two vertices.
     *
     * @param source             the source vertex
     * @param outputOrdinal      ordinal at the source
     * @param destination        the destination vertex
     * @param inputOrdinal ordinal at the destination
     */
    public Edge(Vertex source, int outputOrdinal,
                Vertex destination, int inputOrdinal) {
        this.source = source;
        this.outputOrdinal = outputOrdinal;

        this.destination = destination;
        this.inputOrdinal = inputOrdinal;
    }


    public Vertex getSource() {
        return source;
    }

    public int getOutputOrdinal() {
        return outputOrdinal;
    }

    public Vertex getDestination() {
        return destination;
    }

    public int getInputOrdinal() {
        return inputOrdinal;
    }

    /**
     * Inputs with higher priority will be read to completion before others.
     */
    public Edge priority(int priority) {
        this.priority = priority;
        return this;
    }

    public int getPriority() {
        return priority;
    }

    @Override
    public String toString() {
        return "Edge{"
                + "source=" + source
                + ", destination=" + destination
                + '}';
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(source);
        out.writeInt(outputOrdinal);

        out.writeObject(destination);
        out.writeInt(inputOrdinal);

        out.writeInt(priority);

    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        source = in.readObject();
        outputOrdinal = in.readInt();

        destination = in.readObject();
        inputOrdinal = in.readInt();

        priority = in.readInt();
    }

    @Override
    public int getFactoryId() {
        return JetDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getId() {
        return JetDataSerializerHook.EDGE;
    }


}
