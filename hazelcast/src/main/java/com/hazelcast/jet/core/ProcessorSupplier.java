/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.Address;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.security.impl.function.SecuredFunction;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.processor.ProcessorSupplierFromSimpleSupplier;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.Serializable;
import java.util.Collection;

/**
 * Factory of {@link Processor} instances. Part of the initialization
 * chain as explained on {@link ProcessorMetaSupplier}.
 *
 * @since Jet 3.0
 */
@FunctionalInterface
public interface ProcessorSupplier extends Serializable, SecuredFunction {

    /**
     * Called on each cluster member after deserialization.
     */
    default void init(@Nonnull Context context) throws Exception {
    }

    /**
     * Called after {@link #init(Context)} to retrieve instances of
     * {@link Processor} that will be used during the execution of the Jet job.
     *
     * @param count the number of processor this method is required to create
     *              and return. It is equal to {@link Context#localParallelism()}.
     */
    @Nonnull
    Collection<? extends Processor> get(int count);

    /**
     * Called after the execution has finished on this member - successfully or
     * not. The execution might still be running on other members. This method
     * will be called after {@link Processor#close} has been called on all
     * processors running on this member for this job.
     * <p>
     * If this method throws an exception, it will be logged and ignored; it
     * won't be reported as a job failure.
     * <p>
     * Note: this method can be called even if {@link #init(Context) init()} or
     * {@link #get(int) get()} methods were not called in case the job fails
     * during the init phase.
     *
     * @param error the exception (if any) that caused the job to fail;
     *              {@code null} in the case of successful job completion.
     *              Note that it might not be the actual error that caused the job
     *              to fail - it can be several other exceptions. We only guarantee
     *              that it's non-null if the job didn't complete successfully.
     */
    default void close(@Nullable Throwable error) throws Exception {
    }

    /**
     * Returns a {@code ProcessorSupplier} which will delegate to the given
     * {@code Supplier<Processor>} to create all {@code Processor} instances.
     */
    @Nonnull
    static ProcessorSupplier of(@Nonnull SupplierEx<? extends Processor> processorSupplier) {
        return new ProcessorSupplierFromSimpleSupplier(processorSupplier);
    }

    /**
     * Context passed to the supplier in the {@link #init(Context) init()} call.
     *
     * @since Jet 3.0
     */
    interface Context extends ProcessorMetaSupplier.Context {

        /**
         * Returns a logger for the associated {@code ProcessorSupplier}.
         */
        @Nonnull
        ILogger logger();

        /**
         * Returns the index of the member among all the members that run this
         * job: it's a unique cluster-wide index.
         * <p>
         * The value is in the range {@code [0...memberCount-1]}.
         */
        int memberIndex();

        /**
         * Uses the supplied ID to look up a directory you attached to the current
         * Jet job. Creates a temporary directory with the same contents on the
         * local cluster member and returns the location of the created directory.
         * If the directory was already created, just returns its location.
         *
         * @param id the ID you used in a previous {@link JobConfig#attachDirectory} call
         * @since Jet 4.0
         */
        @Nonnull
        File attachedDirectory(@Nonnull String id);

        /**
         * Behaves like {@link #attachedDirectory}, but if the directory already
         * exists, it deletes and recreates all its contents.
         *
         * @since Jet 4.3
         */
        @Nonnull
        File recreateAttachedDirectory(@Nonnull String id);

        /**
         * Uses the supplied ID to look up a file you attached to the current Jet
         * job. Creates a temporary file with the same contents on the local
         * cluster member and returns the location of the created file. If the
         * file was already created, just returns its location.
         *
         * @param id the ID you used in a previous {@link JobConfig#attachFile} call
         * @since Jet 4.0
         */
        @Nonnull
        File attachedFile(@Nonnull String id);

        /**
         * Behaves like {@link #attachedFile}, but if the file already exists, it
         * deletes and recreates it.
         *
         * @since Jet 4.3
         */
        @Nonnull
        File recreateAttachedFile(@Nonnull String id);

        /**
         * Returns {@link ManagedContext} associated with this job.
         */
        @Nonnull
        ManagedContext managedContext();

        /**
         * Returns the partitions from {@link #partitionAssignment()} pertaining to
         * this member. The returned list can be empty.
         */
        @Nonnull
        default int[] memberPartitions() {
            Address address = hazelcastInstance().getCluster().getLocalMember().getAddress();
            return partitionAssignment().get(address);
        }
    }
}
