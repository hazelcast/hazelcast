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

import com.hazelcast.jet.impl.CustomClassLoadedObject;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Represents a unit of data processing in a Jet computation job. Conceptually,
 * a vertex receives data items over its inbound {@link Edge edges} and pushes
 * data items to its outbound edges. Practically, a single vertex is represented
 * by a set of instances of {@link Processor}. The {@code parallelism} property
 * determines the number of processor instances running on each cluster member.
 * <p>
 * Each instance of processor is assigned a set of partition IDs it is responsible
 * for. When an inbound edge is <em>partitioned</em>, the processor will receive
 * only those data items whose partition ID it is responsible for. For data traveling
 * over a partitioned edge which is also <em>distributed</em>, the whole cluster
 * contains a single unique processor instance responsible for any given partition ID.
 * For non-distributed edges, the processor is unique only within a member and each
 * member has its own processor for any given partition ID.
 * <p>
 * There is also a guarantee of collation across all the partitioned edges impinging
 * on a vertex: within each member, all the data with a given partition ID is
 * received by the same processor.
 */
public class Vertex implements IdentifiedDataSerializable {

    private ProcessorMetaSupplier supplier;
    private String name;
    private int parallelism = -1;

    Vertex() {
    }

    /**
     * Javadoc pending
     */
    public Vertex(String name, SimpleProcessorSupplier processorSupplier) {
        checkNotNull(name, "name");
        checkNotNull(processorSupplier, "supplier");

        this.supplier = ProcessorMetaSupplier.of(processorSupplier);
        this.name = name;
    }

    /**
     * Javadoc pending
     */
    public Vertex(String name, ProcessorSupplier processorSupplier) {
        checkNotNull(name, "name");
        checkNotNull(processorSupplier, "supplier");

        this.supplier = ProcessorMetaSupplier.of(processorSupplier);
        this.name = name;
    }

    /**
     * Javadoc pending
     */
    public Vertex(String name, ProcessorMetaSupplier supplier) {
        checkNotNull(name, "name");
        checkNotNull(supplier, "supplier");

        this.supplier = supplier;
        this.name = name;
    }

    /**
     * @return name of the vertex
     */
    public String getName() {
        return name;
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
     * @return the processor supplier
     */
    public ProcessorMetaSupplier getSupplier() {
        return supplier;
    }

    @Override
    public String toString() {
        return "Vertex " + name;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        CustomClassLoadedObject.write(out, supplier);
        out.writeInt(parallelism);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        supplier = CustomClassLoadedObject.read(in);
        parallelism = in.readInt();
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
