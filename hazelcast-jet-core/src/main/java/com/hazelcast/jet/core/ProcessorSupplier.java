/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
     * Called after the execution has finished on all members - successfully or
     * not. It is called immediately when the execution was <em>aborted</em>
     * due to a member leaving the cluster. If called immediately, it can
     * happen that the job is still running on some other member (but not on
     * this member).
     * <p>
     * If this method throws an exception, it will be logged and ignored; it
     * won't be reported as a job failure.
     * <p>
     * Note: this method can be called even if {@link #init(Context) init()} or
     * {@link #get(int) get()} were not called yet in case the job fails during
     * the init phase.
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
    interface Context extends ProcessorMetaSupplier.Context {

        /**
         * Returns a logger for the associated {@code ProcessorSupplier}.
         */
        @Nonnull
        ILogger logger();
    }
}
