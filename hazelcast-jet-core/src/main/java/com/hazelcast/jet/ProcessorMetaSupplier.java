/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.Distributed.Function;
import com.hazelcast.jet.Distributed.Supplier;
import com.hazelcast.nio.Address;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.List;

/**
 * Factory of {@link ProcessorSupplier} instances. The starting point of
 * the chain leading to the eventual creation of {@code Processor} instances
 * on each cluster member:
 * <ol><li>
 * client creates {@code ProcessorMetaSupplier} as a part of the DAG;
 * </li><li>
 * serializes it and sends to a cluster member;
 * </li<li>
 * the member deserializes and uses it to create one {@code ProcessorSupplier}
 * for each cluster member;
 * </li><li>
 * serializes each {@code ProcessorSupplier} and sends it to its target member;
 * </li><li>
 * the target member deserializes and uses it to instantiate as many instances
 * of {@code Processor} as requested by the <em>parallelism</em> property on
 * the corresponding {@code Vertex}.
 * </li></ol>
 * Before being asked to create {@code ProcessorSupplier}s this meta-supplier will
 * be given access to the Hazelcast instance and, in particular, its cluster topology
 * and partitioning services. It can use the information from these services to
 * precisely parameterize each {@code Processor} instance that will be created on
 * each member.
 */
@FunctionalInterface
public interface ProcessorMetaSupplier extends Serializable {

    /**
     * Called on the cluster member that receives the client request, after
     * deserializing the meta-supplier instance. Gives access to the Hazelcast
     * instance's services and provides the parallelism parameters determined
     * from the cluster size.
     */
    default void init(@Nonnull Context context) {
    }

    /**
     * Called to create a mapping from member {@link Address} to the
     * {@link ProcessorSupplier} that will be sent to that member. Jet calls
     * this method with a list of all cluster members' addresses and the
     * returned function must be a mapping that returns a non-null value for
     * each given address.
     * <p>
     * The method will be called once per job execution on the job's
     * <em>coordinator</em> member. {@code init()} will have already
     * been called.
     */
    @Nonnull
    java.util.function.Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses);

    /**
     * Factory method that wraps the given {@code ProcessorSupplier}
     * and returns the same instance for each given {@code Address}.
     */
    @Nonnull
    static ProcessorMetaSupplier of(@Nonnull ProcessorSupplier procSupplier) {
        return of((Address x) -> procSupplier);
    }

    /**
     * Factory method that wraps the given {@code Supplier<Processor>}
     * and uses it as the supplier of all {@code Processor} instances.
     * Specifically, returns a meta-supplier that will always return the
     * result of calling {@link ProcessorSupplier#of(Supplier<Processor>)}.
     */
    @Nonnull
    static ProcessorMetaSupplier of(@Nonnull Supplier<Processor> procSupplier) {
        return ProcessorMetaSupplier.of(ProcessorSupplier.of(procSupplier));
    }

    /**
     * Factory method that creates a {@link ProcessorMetaSupplier} based on a mapping to
     * {@link ProcessorSupplier} for each given address
     */
    static ProcessorMetaSupplier of(Function<Address, ProcessorSupplier> addressToSupplier) {
        return x -> addressToSupplier;
    }

    /**
     * Context passed to the meta-supplier at init time on the member that
     * received a job request from the client.
     */
    interface Context {

        /**
         * Returns the current Jet instance.
         */
        @Nonnull
        JetInstance jetInstance();

        /**
         * Returns the total number of {@code Processor}s that will be
         * created across the cluster.
         */
        int totalParallelism();

        /**
         * Returns the number of processors that each {@code ProcessorSupplier}
         * will be asked to create once deserialized on each member.
         */
        int localParallelism();
    }

}
