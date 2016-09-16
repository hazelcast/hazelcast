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

package com.hazelcast.jet;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Represents vertex of the Direct Acyclic Graph
 */
public class Vertex implements IdentifiedDataSerializable {
    private String name;
    private String processorClass;
    private Object[] processorArgs;
    private int parallelism = 1;
    private List<Sink> sinks = new ArrayList<Sink>();
    private List<Source> sources = new ArrayList<Source>();


    Vertex() {

    }

    /**
     * Constructs a new vertex
     *
     * @param name           name of the vertex
     * @param processorClass class name of the processor
     * @param processorArgs  constructor arguments of the processor
     */
    public Vertex(String name, Class<? extends Processor> processorClass, Object... processorArgs) {
        checkNotNull(name);
        this.name = name;
        this.processorClass = processorClass.getName();
        this.processorArgs = processorArgs.clone();
    }

    /**
     * @return name of the vertex
     */
    public String getName() {
        return this.name;
    }

    /**
     * @return number of parallel instances of this vertex
     */
    public int getParallelism() {
        return parallelism;
    }

    /**
     * Sets the number of parallel instances of this vertex
     */
    public Vertex parallelism(int parallelism) {
        if (parallelism <= 0) {
            throw new IllegalArgumentException("Parallelism must be greater than 0");
        }
        this.parallelism = parallelism;
        return this;
    }

    /**
     * @return constructor arguments for the processor
     */
    public Object[] getProcessorArgs() {
        return processorArgs.clone();
    }

    /**
     * @return name of the processor class
     */
    public String getProcessorClass() {
        return processorClass;
    }

    /**
     * Add abstract source source object to the vertex
     *
     * @param source corresponding source
     */
    public Vertex addSource(Source source) {
        this.sources.add(source);
        return this;
    }

    /**
     * Add abstract sink object to the vertex
     *
     * @param sink corresponding sink
     */
    public Vertex addSink(Sink sink) {
        this.sinks.add(sink);
        return this;
    }

    /**
     * @return list of the input sources
     */
    public List<Source> getSources() {
        return Collections.unmodifiableList(this.sources);
    }

    /**
     * @return list of the output sinks
     */
    public List<Sink> getSinks() {
        return Collections.unmodifiableList(this.sinks);
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

    @Override
    public String toString() {
        return "Vertex{"
                + "name='" + name + '\''
                + '}';
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(processorClass);
        out.writeInt(processorArgs.length);
        for (Object processorArg : processorArgs) {
            out.writeObject(processorArg);
        }

        out.writeInt(parallelism);

        out.writeInt(sources.size());
        for (Source source : sources) {
            out.writeObject(source);
        }

        out.writeInt(sinks.size());
        for (Sink sink : sinks) {
            out.writeObject(sink);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        processorClass = in.readUTF();
        processorArgs = new Object[in.readInt()];

        for (int i = 0; i < processorArgs.length; i++) {
            processorArgs[i] = in.readObject();
        }

        parallelism = in.readInt();

        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            sources.add(in.readObject());
        }

        count = in.readInt();
        for (int i = 0; i < count; i++) {
            sinks.add(in.readObject());
        }
    }

    @Override
    public int getFactoryId() {
        return JetDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getId() {
        return JetDataSerializerHook.VERTEX;
    }
}
