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

import com.hazelcast.nio.Address;

import javax.annotation.Nonnull;
import java.io.Serializable;

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
     * Called to create a {@link ProcessorSupplier} which will be sent to the
     * cluster member with the supplied address. All calls of this method are
     * made on the cluster member that received the job request, after calling
     * {@code init()}.
     */
    @Nonnull
    ProcessorSupplier get(@Nonnull Address address);

    /**
     * Factory method that wraps the given {@code ProcessorSupplier}
     * and returns it as a constant from each {@link #get(Address)} call.
     */
    @Nonnull
    static ProcessorMetaSupplier of(@Nonnull ProcessorSupplier procSupplier) {
        return address -> procSupplier;
    }

    /**
     * Factory method that wraps the given {@code SimpleProcessorSupplier}
     * and uses it as the supplier of all {@code Processor} instances.
     * Specifically, returns a meta-supplier that will always return the
     * result of calling {@link ProcessorSupplier#of(SimpleProcessorSupplier)}.
     */
    @Nonnull
    static ProcessorMetaSupplier of(@Nonnull SimpleProcessorSupplier procSupplier) {
        return address -> ProcessorSupplier.of(procSupplier);
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
