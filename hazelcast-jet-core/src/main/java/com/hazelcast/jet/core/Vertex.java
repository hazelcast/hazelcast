/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.core;

import com.hazelcast.jet.function.SupplierEx;
import com.hazelcast.jet.impl.execution.init.CustomClassLoadedObject;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.function.UnaryOperator;

import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Represents a unit of data processing in a Jet computation job. Conceptually,
 * a vertex receives data items over its inbound {@link Edge edges} and pushes
 * data items to its outbound edges. Practically, a single vertex is represented
 * by a set of instances of {@link Processor}. The {@code localParallelism} property
 * determines the number of processor instances running on each cluster member.
 * <p>
 * Each processor is assigned a set of partition IDs it is responsible for. When
 * an inbound edge is <em>partitioned</em>, the processor will receive only those
 * data items whose partition ID it is responsible for. For data traveling over a
 * partitioned edge which is also <em>distributed</em>, the whole cluster contains
 * a single unique processor instance responsible for any given partition ID. For
 * non-distributed edges, the processor is unique only within a member and each
 * member has its own processor for any given partition ID. Finally, there is a
 * guarantee of collation across all the partitioned edges impinging on a vertex:
 * within each member, all the data with a given partition ID is received by the
 * same processor.
 * <p>
 * A vertex is uniquely identified in a DAG by its name.
 */
public class Vertex implements IdentifiedDataSerializable {

    /**
     * The value of {@link #localParallelism(int)} with the meaning
     * "use the default local parallelism".
     */
    public static final int LOCAL_PARALLELISM_USE_DEFAULT = -1;

    private ProcessorMetaSupplier metaSupplier;
    private String name;
    private int localParallelism = -1;

    /**
     * Constructor used internally for deserialization.
     */
    Vertex() {
    }

    /**
     * Creates a vertex from a {@code Supplier<Processor>}.
     * <p>
     * This is useful for vertices where all the {@code Processor} instances
     * will be instantiated the same way.
     * <p>
     * <strong>NOTE:</strong> this constructor should not be abused with a stateful
     * implementation which produces a different processor each time. In such a
     * case the full {@code ProcessorSupplier} type should be implemented.
     *
     * @param name the unique name of the vertex. This name identifies the vertex in the snapshot
     * @param processorSupplier the simple, parameterless supplier of {@code Processor} instances
     */
    public Vertex(@Nonnull String name, @Nonnull SupplierEx<? extends Processor> processorSupplier) {
        this(name, ProcessorMetaSupplier.of(processorSupplier));
    }

    /**
     * Creates a vertex from a {@code ProcessorSupplier}.
     *
     * @param name the unique name of the vertex. This name identifies the vertex in the snapshot
     * @param processorSupplier the supplier of {@code Processor} instances which will be used on all members
     */
    public Vertex(@Nonnull String name, @Nonnull ProcessorSupplier processorSupplier) {
        this(name, ProcessorMetaSupplier.of(processorSupplier));
    }

    /**
     * Creates a vertex from a {@code ProcessorMetaSupplier}.
     *
     * @param name the unique name of the vertex. This name identifies the vertex in the snapshot
     * @param metaSupplier the meta-supplier of {@code ProcessorSupplier}s for each member
     *
     */
    public Vertex(@Nonnull String name, @Nonnull ProcessorMetaSupplier metaSupplier) {
        checkNotNull(name, "name");
        checkNotNull(metaSupplier, "supplier");
        checkSerializable(metaSupplier, "metaSupplier");

        this.metaSupplier = metaSupplier;
        this.name = name;
    }

    /**
     * Says whether the given integer is valid as the value of {@link
     * #localParallelism(int) localParallelism}.
     */
    public static int checkLocalParallelism(int parallelism) {
        if (parallelism != LOCAL_PARALLELISM_USE_DEFAULT && parallelism <= 0) {
            throw new IllegalArgumentException("Parallelism must be either -1 or a positive number");
        }
        return parallelism;
    }

    /**
     * Sets the number of processors corresponding to this vertex that will be
     * created on each member.
     * <p>
     * If the value is {@value #LOCAL_PARALLELISM_USE_DEFAULT}, Jet will
     * determine the vertex's local parallelism during job initialization
     * from the global default and processor meta-supplier's preferred value.
     */
    @Nonnull
    public Vertex localParallelism(int localParallelism) {
        this.localParallelism = checkLocalParallelism(localParallelism);
        return this;
    }

    /**
     * Returns the number of processors corresponding to this vertex that will
     * be created on each member. A value of {@code -1} means that this
     * property is not set; in that case the default configured on the Jet
     * instance will be used.
     */
    public int getLocalParallelism() {
        return localParallelism;
    }

    /**
     * Returns the name of this vertex.
     */
    @Nonnull
    public String getName() {
        return name;
    }

    /**
     * Returns this vertex's meta-supplier of processors.
     */
    @Nonnull
    public ProcessorMetaSupplier getMetaSupplier() {
        return metaSupplier;
    }

    /**
     * Applies the provided operator function to the current processor
     * meta-supplier and replaces it with the one it returns. Typically used to
     * decorate the existing meta-supplier.
     */
    public void updateMetaSupplier(@Nonnull UnaryOperator<ProcessorMetaSupplier> updateFn) {
        metaSupplier = updateFn.apply(metaSupplier);
    }

    @Override
    public String toString() {
        return "Vertex " + name;
    }


    // Implementation of IdentifiedDataSerializable

    @Override
    public void writeData(@Nonnull ObjectDataOutput out) throws IOException {
        out.writeInt(localParallelism);
        out.writeUTF(name);
        CustomClassLoadedObject.write(out, metaSupplier);
    }

    @Override
    public void readData(@Nonnull ObjectDataInput in) throws IOException {
        localParallelism = in.readInt();
        name = in.readUTF();
        try {
            metaSupplier = CustomClassLoadedObject.read(in);
        } catch (HazelcastSerializationException e) {
            throw new HazelcastSerializationException("Error deserializing vertex '" + name + "': " + e, e);
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

    // END Implementation of IdentifiedDataSerializable
}
