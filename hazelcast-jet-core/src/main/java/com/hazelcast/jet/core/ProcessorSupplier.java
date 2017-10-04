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

package com.hazelcast.jet.core;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Collection;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * Factory of {@link Processor} instances. Part of the initialization
 * chain as explained on {@link ProcessorMetaSupplier}.
 */
@FunctionalInterface
public interface ProcessorSupplier extends Serializable {

    /**
     * Called on each cluster member after deserialization.
     */
    default void init(@Nonnull Context context) {
    }

    /**
     * Called after {@link #init(Context)} to retrieve instances of
     * {@link Processor} that will be used during the execution of the Jet job.
     *
     * @param count the number of processor this method is required to create
     *              and return
     */
    @Nonnull
    Collection<? extends Processor> get(int count);

    /**
     * Called after execution is finished on all the nodes, whether successfully
     * or not. Execution can also be <em>aborted</em>, for example if a topology
     * change is detected in the cluster. In such a case this method will be
     * called immediately, without waiting for completion on other members.
     *
     * @param error the exception (if any) that caused the job to fail;
     *              {@code null} in the case of successful job completion
     */
    default void complete(Throwable error) {
    }

    /**
     * Returns a {@code ProcessorSupplier} which will delegate to the given
     * {@code Supplier<Processor>} to create all {@code Processor} instances.
     */
    @Nonnull
    static ProcessorSupplier of(@Nonnull DistributedSupplier<? extends Processor> processorSupplier) {
        return count -> Stream.generate(processorSupplier).limit(count).collect(toList());
    }

    /**
     * Context passed to the supplier in the {@link #init(Context) init()} call.
     */
    interface Context {

        /**
         * Returns the current Jet instance
         */
        @Nonnull
        JetInstance jetInstance();

        /**
         * Returns the number of processors that the associated {@code ProcessorSupplier}
         * will be asked to create.
         */
        int localParallelism();

        /**
         * Returns true, if snapshots will be saved for this job.
         */
        boolean snapshottingEnabled();

        /**
         * Returns a logger for the associated {@code ProcessorSupplier}.
         */
        @Nonnull
        ILogger logger();
    }
}
