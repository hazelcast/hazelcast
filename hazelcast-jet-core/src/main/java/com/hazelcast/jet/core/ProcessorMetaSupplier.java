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

import com.hazelcast.cluster.Address;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.logging.ILogger;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.List;
import java.util.function.Function;

import static com.hazelcast.internal.util.UuidUtil.newUnsecureUuidString;
import static java.util.Collections.singletonList;

/**
 * Factory of {@link ProcessorSupplier} instances. The starting point of
 * the chain leading to the eventual creation of {@code Processor} instances
 * on each cluster member:
 * <ol><li>
 * client creates {@code ProcessorMetaSupplier} as a part of the DAG;
 * </li><li>
 * serializes it and sends to a cluster member;
 * </li><li>
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
 *
 * @since 3.0
 */
@FunctionalInterface
public interface ProcessorMetaSupplier extends Serializable {

    /**
     * Returns the local parallelism the vertex should be configured with.
     * The default implementation returns {@link
     * Vertex#LOCAL_PARALLELISM_USE_DEFAULT}.
     */
    default int preferredLocalParallelism() {
        return Vertex.LOCAL_PARALLELISM_USE_DEFAULT;
    }

    /**
     * Called on the cluster member that receives the client request, after
     * deserializing the meta-supplier instance. Gives access to the Hazelcast
     * instance's services and provides the parallelism parameters determined
     * from the cluster size.
     */
    default void init(@Nonnull Context context) throws Exception {
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
    Function<? super Address, ? extends ProcessorSupplier> get(@Nonnull List<Address> addresses);

    /**
     * Called on coordinator member after execution has finished on all
     * members, successfully or not. This method will be called after {@link
     * ProcessorSupplier#close(Throwable)} has been called on all
     * <em>available</em> members.
     * <p>
     * If there is an exception during the creation of the execution plan, this
     * method will be called regardless of whether the {@link #init(Context)
     * init()} or {@link #get(List) get()} method have been called or not.
     * If this method throws an exception, it will be logged and ignored; it
     * won't be reported as a job failure.
     * <p>
     * If you rely on the fact that this method is run once per cluster, it can
     * happen that it is not called at all, if the coordinator member crashed.
     *
     * @param error the exception (if any) that caused the job to fail;
     *              {@code null} in the case of successful job completion
     */
    default void close(@Nullable Throwable error) throws Exception {
    }

    /**
     * Factory method that wraps the given {@code ProcessorSupplier} and
     * returns the same instance for each given {@code Address}.
     *
     * @param preferredLocalParallelism the value to return from {@link #preferredLocalParallelism()}
     * @param procSupplier the processor supplier
     */
    @Nonnull
    static ProcessorMetaSupplier of(int preferredLocalParallelism, @Nonnull ProcessorSupplier procSupplier) {
        return of(preferredLocalParallelism, (Address x) -> procSupplier);
    }

    /**
     * Wraps the provided {@code ProcessorSupplier} into a meta-supplier that
     * will always return it. The {@link #preferredLocalParallelism()} of
     * the meta-supplier will be {@link Vertex#LOCAL_PARALLELISM_USE_DEFAULT}.
     */
    @Nonnull
    static ProcessorMetaSupplier of(@Nonnull ProcessorSupplier procSupplier) {
        return of(Vertex.LOCAL_PARALLELISM_USE_DEFAULT, procSupplier);
    }

    /**
     * Factory method that wraps the given {@code Supplier<Processor>}
     * and uses it as the supplier of all {@code Processor} instances.
     * Specifically, returns a meta-supplier that will always return the
     * result of calling {@link ProcessorSupplier#of(SupplierEx)}.
     *
     * @param preferredLocalParallelism the value to return from {@link #preferredLocalParallelism()}
     * @param procSupplier              the supplier of processors
     */
    @Nonnull
    static ProcessorMetaSupplier of(
            int preferredLocalParallelism, @Nonnull SupplierEx<? extends Processor> procSupplier
    ) {
        return of(preferredLocalParallelism, ProcessorSupplier.of(procSupplier));
    }

    /**
     * Factory method that wraps the given {@code Supplier<Processor>}
     * and uses it as the supplier of all {@code Processor} instances.
     * Specifically, returns a meta-supplier that will always return the
     * result of calling {@link ProcessorSupplier#of(SupplierEx)}.
     * The {@link #preferredLocalParallelism()} of the meta-supplier will be
     * {@link Vertex#LOCAL_PARALLELISM_USE_DEFAULT}.
     */
    @Nonnull
    static ProcessorMetaSupplier of(@Nonnull SupplierEx<? extends Processor> procSupplier) {
        return of(Vertex.LOCAL_PARALLELISM_USE_DEFAULT, procSupplier);
    }

    /**
     * Factory method that creates a {@link ProcessorMetaSupplier} from the
     * supplied function that maps a cluster member address to a {@link
     * ProcessorSupplier}.
     *
     * @param preferredLocalParallelism the value to return from {@link #preferredLocalParallelism()}
     * @param addressToSupplier the mapping from address to ProcessorSupplier
     */
    @Nonnull
    static ProcessorMetaSupplier of(
            int preferredLocalParallelism,
            @Nonnull FunctionEx<? super Address, ? extends ProcessorSupplier> addressToSupplier
    ) {
        Vertex.checkLocalParallelism(preferredLocalParallelism);
        return new ProcessorMetaSupplier() {
            @Override
            public int preferredLocalParallelism() {
                return preferredLocalParallelism;
            }

            @Nonnull @Override
            public Function<? super Address, ? extends ProcessorSupplier> get(@Nonnull List<Address> addresses) {
                return addressToSupplier;
            }
        };
    }

    /**
     * Factory method that creates a {@link ProcessorMetaSupplier} from the
     * supplied function that maps a cluster member address to a {@link
     * ProcessorSupplier}. The {@link #preferredLocalParallelism()} of
     * the meta-supplier will be {@link Vertex#LOCAL_PARALLELISM_USE_DEFAULT}.
     */
    @Nonnull
    static ProcessorMetaSupplier of(
            @Nonnull FunctionEx<? super Address, ? extends ProcessorSupplier> addressToSupplier
    ) {
        return of(Vertex.LOCAL_PARALLELISM_USE_DEFAULT, addressToSupplier);
    }


    /**
     * Wraps the provided {@code ProcessorSupplier} into a meta-supplier that
     * will always return it. The {@link #preferredLocalParallelism()} of
     * the meta-supplier will be one, i.e., no local parallelization.
     * <p>
     * The parallelism will be overridden if the {@link Vertex#localParallelism(int)} is
     * set to a specific value.
     */
    @Nonnull
    static ProcessorMetaSupplier preferLocalParallelismOne(@Nonnull ProcessorSupplier supplier) {
        return of(1, supplier);
    }

    /**
     * Variant of {@link #preferLocalParallelismOne(ProcessorSupplier)} where
     * the supplied {@code SupplierEx<Processor>} will be
     * wrapped into a {@link ProcessorSupplier}.
     */
    @Nonnull
    static ProcessorMetaSupplier preferLocalParallelismOne(
            @Nonnull SupplierEx<? extends Processor> procSupplier
    ) {
        return of(1, ProcessorSupplier.of(procSupplier));
    }

    /**
     * Variant of {@link #forceTotalParallelismOne(ProcessorSupplier, String)} where the node
     * for the supplier will be chosen randomly.
     */
    @Nonnull
    static ProcessorMetaSupplier forceTotalParallelismOne(@Nonnull ProcessorSupplier supplier) {
        return forceTotalParallelismOne(supplier, newUnsecureUuidString());
    }

    /**
     * Wraps the provided {@code ProcessorSupplier} into a meta-supplier that
     * will only use the given {@code ProcessorSupplier} on a single node.
     * The node will be chosen according to the {@code partitionKey} supplied.
     * This is mainly provided as a convenience for implementing
     * non-distributed sources where data can't be read in parallel by multiple
     * consumers. When used as a sink or intermediate vertex, the DAG should ensure
     * that only the processor instance on the designated node receives any data,
     * otherwise an {@code IllegalStateException} will be thrown.
     * <p>
     * The vertex containing the {@code ProcessorMetaSupplier} must have a local
     * parallelism setting of 1, otherwise {code IllegalArgumentException} is thrown.
     *
     * @param supplier the supplier that will be wrapped
     * @param partitionKey the supplier will only be created on the node that owns the supplied
     *                     partition key
     * @return the wrapped {@code ProcessorMetaSupplier}
     *
     * @throws IllegalArgumentException if vertex has local parallelism setting of greater than 1
     */
    @Nonnull
    static ProcessorMetaSupplier forceTotalParallelismOne(
            @Nonnull ProcessorSupplier supplier, @Nonnull String partitionKey
    ) {
        return new ProcessorMetaSupplier() {

            private transient Address ownerAddress;

            @Override
            public void init(@Nonnull Context context) {
                if (context.localParallelism() != 1) {
                    throw new IllegalArgumentException(
                            "Local parallelism of " + context.localParallelism() + " was requested for a vertex that "
                                    + "supports only total parallelism of 1. Local parallelism should be 1.");
                }
                String key = StringPartitioningStrategy.getPartitionKey(partitionKey);
                ownerAddress = context.jetInstance().getHazelcastInstance().getPartitionService()
                                      .getPartition(key).getOwner().getAddress();
            }

            @Nonnull @Override
            public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
                return addr -> addr.equals(ownerAddress) ?
                        supplier
                        :
                        count -> singletonList(new AbstractProcessor() {
                            @Override
                            protected boolean tryProcess(int ordinal, @Nonnull Object item) {
                                throw new IllegalStateException(
                                        "This vertex has a total parallelism of one and as such only"
                                                + " expects input on a specific node. Edge configuration must be adjusted"
                                                + " to make sure that only the expected node receives any input."
                                                + " Unexpected input received from ordinal " + ordinal + ": " + item
                                );
                            }

                            @Override
                            protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
                                // state might be broadcast to all instances - ignore it in the no-op instances
                            }
                        });
            }

            @Override
            public int preferredLocalParallelism() {
                return 1;
            }
        };
    }

    /**
     * Context passed to the meta-supplier at init time on the member that
     * received a job request from the client.
     *
     * @since 3.0
     */
    interface Context {

        /**
         * Returns the current Jet instance.
         */
        @Nonnull
        JetInstance jetInstance();

        /**
         * Returns the job ID. Job id is unique for job submission and doesn't
         * change when the job restarts. It's also unique for all running and
         * archived jobs.
         */
        long jobId();

        /**
         * Returns the job execution ID. It's unique for one execution, but
         * changes when the job restarts.
         */
        long executionId();

        /**
         * Returns the {@link JobConfig}.
         */
        @Nonnull
        JobConfig jobConfig();

        /**
         * Returns the total number of {@code Processor}s that will be created
         * across the cluster. This number remains stable for entire job
         * execution. It is equal to {@link #memberCount()} * {@link
         * #localParallelism()}.
         */
        int totalParallelism();

        /**
         * Returns the number of processors that each {@code ProcessorSupplier}
         * will be asked to create once deserialized on each member. All
         * members have equal local parallelism. The count doesn't change
         * unless the job restarts.
         */
        int localParallelism();

        /**
         * Returns the number of members running this job.
         * <p>
         * Note that the value might be lower than current member count if
         * members were added after the job started. The count doesn't change
         * unless the job restarts.
         */
        int memberCount();

        /**
         * Returns the name of the associated vertex.
         */
        @Nonnull
        String vertexName();

        /**
         * Returns a logger for the associated {@code ProcessorMetaSupplier}.
         */
        @Nonnull
        ILogger logger();

        /**
         * Returns true, if snapshots will be saved for this job.
         */
        default boolean snapshottingEnabled() {
            return processingGuarantee() != ProcessingGuarantee.NONE;
        }

        /**
         * Returns the guarantee for current job.
         */
        ProcessingGuarantee processingGuarantee();
    }
}
