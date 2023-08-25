/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.serialization.impl.compact.schema;

import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.serialization.impl.compact.Schema;
import com.hazelcast.internal.util.InvocationUtil;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.hazelcast.internal.util.ConcurrencyUtil.CALLER_RUNS;

/**
 * Manages the replication of the schemas across the cluster.
 */
public class SchemaReplicator {

    // Not private for tests
    static final int MAX_RETRIES_FOR_REQUESTS = 100;

    private final MemberSchemaService schemaService;

    // Guards the modifications to replications and inFlightOperations so
    // that their contents are in valid states.
    private final Object mutex = new Object();
    private final ConcurrentHashMap<Long, SchemaReplication> replications = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, InternalCompletableFuture<Collection<UUID>>>
            inFlightOperations = new ConcurrentHashMap<>();

    // Not final due to late initialization with init method.
    private NodeEngine nodeEngine;
    private Executor internalAsyncExecutor;

    public SchemaReplicator(MemberSchemaService schemaService) {
        this.schemaService = schemaService;
    }

    /**
     * Sets the {@link NodeEngine} reference to make the replicator ready to
     * work.
     *
     * @param nodeEngine to set.
     */
    public void init(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.internalAsyncExecutor = nodeEngine.getExecutionService()
                .getExecutor(ExecutionService.ASYNC_EXECUTOR);
    }

    /**
     * Clears the local state of the replicator.
     */
    public void clear() {
        for (InternalCompletableFuture<Collection<UUID>> future : inFlightOperations.values()) {
            future.completeExceptionally(new HazelcastException("The state of the SchemaReplicator is being cleared."));
        }
        inFlightOperations.clear();
        replications.clear();
    }

    /**
     * Replicates the schema to the cluster.
     * <p>
     * If the schema is known to be already replicated, it returns immediately.
     * <p>
     * If not, it initiates the process by:
     * <ul>
     *     <li>
     *         Preparing for the replication process in local by:
     *         <ul>
     *             <li>Putting the schema to an in-memory registry</li>
     *             <li>Persisting the schema to HotRestart(if available and
     *             enabled)</li>
     *             <li>Sending the schema to WAN clusters and waiting for it
     *             to be replicated(if available and enabled)</li>
     *         </ul>
     *     </li>
     *     <li>Marking the replication status of the schema as
     *     {@link SchemaReplicationStatus#PREPARED}</li>
     *     <li>Sending the request for preparation to all cluster members.</li>
     *     <li>On successful acknowledgment from all participants, sending the
     *     request for marking the schema
     *     as {@link SchemaReplicationStatus#REPLICATED} to all cluster
     *     nodes.</li>
     * </ul>
     *
     * @param schema to be replicated.
     * @return the future which will be completed once the replication process
     * ends.
     */
    public InternalCompletableFuture<Collection<UUID>> replicate(Schema schema) {
        long schemaId = schema.getSchemaId();
        if (isSchemaReplicated(schemaId)) {
            return InternalCompletableFuture.newCompletedFuture(getCurrentMemberUuids());
        }

        InternalCompletableFuture<Collection<UUID>> future = inFlightOperations.get(schemaId);
        if (future != null) {
            return future;
        }

        synchronized (mutex) {
            if (isSchemaReplicated(schemaId)) {
                return InternalCompletableFuture.newCompletedFuture(getCurrentMemberUuids());
            }

            future = inFlightOperations.get(schemaId);
            if (future != null) {
                return future;
            }

            future = new InternalCompletableFuture<>();
            inFlightOperations.put(schemaId, future);
        }

        SchemaReplication replication = replications.get(schemaId);
        if (replication == null) {
            doReplicate(schema, future);
            return future;
        }

        switch (replication.getStatus()) {
            case REPLICATED:
                // Schema is already known to be replicated across cluster
                inFlightOperations.remove(schemaId, future);
                future.complete(getCurrentMemberUuids());
                break;
            case PREPARED:
                // The schema is prepared, but we need to make sure that it is
                // replicated in the cluster as well
                doReplicatePreparedSchema(schema, future);
                break;
            default:
                IllegalStateException exception = new IllegalStateException("Unexpected replication status");
                completeInFlightOperationExceptionally(schemaId, future, exception);
                throw exception;
        }

        return future;
    }

    private boolean isSchemaReplicated(long schemaId) {
        SchemaReplication replication = replications.get(schemaId);
        return replication != null && replication.getStatus() == SchemaReplicationStatus.REPLICATED;
    }

    /**
     * Replicates all the schemas by calling {@link #replicate(Schema)} on all
     * of them.
     * <p>
     * This will ensure that, we will only initiate the replication process for
     * schemas that are not {@link SchemaReplicationStatus#REPLICATED} yet.
     *
     * @param schemas to replicate.
     */
    public InternalCompletableFuture<Void> replicateAll(List<Schema> schemas) {
        InternalCompletableFuture[] replications = schemas.stream()
                .map(this::replicate)
                .toArray(InternalCompletableFuture[]::new);

        InternalCompletableFuture<Void> future = new InternalCompletableFuture<>();
        CompletableFuture.allOf(replications)
                .whenCompleteAsync((result, throwable) -> {
                    if (throwable == null) {
                        future.complete(null);
                    } else {
                        future.completeExceptionally(throwable);
                    }
                }, internalAsyncExecutor);
        return future;
    }

    /**
     * Marks the replication status of the schema as
     * {@link SchemaReplicationStatus#PREPARED} if it is not marked yet.
     *
     * @param schema to set the status.
     */
    public void markSchemaAsPrepared(Schema schema) {
        long schemaId = schema.getSchemaId();
        SchemaReplication replication = new SchemaReplication(schema, SchemaReplicationStatus.PREPARED);
        replications.putIfAbsent(schemaId, replication);
    }

    /**
     * Marks the replication status of the schema as
     * {@link SchemaReplicationStatus#REPLICATED}.
     * <p>
     * It assumes that there is already a replication registered for that schema
     * id.
     *
     * @param schemaId to set the status.
     */
    public void markSchemaAsReplicated(long schemaId) {
        SchemaReplication existing = replications.get(schemaId);
        if (existing == null) {
            // Can only happen after the #clear is called. At this point, we
            // shouldn't be marking the schema as replicated, as either we
            // are about to shut down, or healing from the split-brain.
            return;
        }
        existing.setStatus(SchemaReplicationStatus.REPLICATED);
    }

    /**
     * Returns the replication status for the given schema, if it exists.
     */
    public SchemaReplicationStatus getReplicationStatus(Schema schema) {
        SchemaReplication replication = replications.get(schema.getSchemaId());
        if (replication == null) {
            return null;
        }

        return replication.getStatus();
    }

    /**
     * Returns the collection of all replications(prepared&replicated) so far.
     */
    public Collection<SchemaReplication> getReplications() {
        // shallow copy is enough, we won't mutate the replications
        return new ArrayList<>(replications.values());
    }

    /**
     * Sets the status of the replications as they are available in the
     * {@code replications}.
     * <p>
     * It assumes that the {@code this.replications} is an empty map.
     * <p>
     * This method might be called on two occasions:
     * <ul>
     *     <li>For newly joining members, where {@code this.replications} is
     *     empty</li>
     *     <li>For members of the smaller cluster when they join the larger
     *     cluster, during the split brain healing, where
     *     {@link #clear()} is called beforehand, which clears the
     *     {@code this.replications}.</li>
     * </ul>
     */
    public void setReplications(Collection<SchemaReplication> replications) {
        for (SchemaReplication replication : replications) {
            long schemaId = replication.getSchema().getSchemaId();
            this.replications.put(schemaId, replication);
        }
    }

    private void doReplicate(Schema schema, InternalCompletableFuture<Collection<UUID>> future) {
        long schemaId = schema.getSchemaId();
        try {
            prepareOnCaller(schema)
                    .thenComposeAsync(result -> {
                        markSchemaAsPrepared(schema);
                        return sendRequestForPreparation(schema);
                    }, CALLER_RUNS)
                    .thenComposeAsync(result -> sendRequestForAcknowledgment(schemaId), CALLER_RUNS)
                    .thenAcceptAsync(result -> completeInFlightOperation(schemaId, future, result), CALLER_RUNS)
                    .exceptionally(throwable -> {
                        completeInFlightOperationExceptionally(schemaId, future, throwable);
                        return null;
                    });
        } catch (Throwable t) {
            // to avoid risk of prepareOnCaller throwing synchronously
            completeInFlightOperationExceptionally(schemaId, future, t);
        }
    }

    private InternalCompletableFuture<Void> prepareOnCaller(Schema schema) {
        schemaService.putLocal(schema);
        return schemaService.persistSchemaToHotRestartAsync(schema);
    }

    private void doReplicatePreparedSchema(Schema schema, InternalCompletableFuture<Collection<UUID>> future) {
        long schemaId = schema.getSchemaId();
        try {
            sendRequestForPreparation(schema)
                    .thenComposeAsync(result -> sendRequestForAcknowledgment(schemaId), CALLER_RUNS)
                    .thenAcceptAsync(result -> completeInFlightOperation(schemaId, future, result), CALLER_RUNS)
                    .exceptionally(throwable -> {
                        completeInFlightOperationExceptionally(schemaId, future, throwable);
                        return null;
                    });
        } catch (Throwable t) {
            // to avoid risk of sendRequestForPreparation throwing synchronously
            completeInFlightOperationExceptionally(schemaId, future, t);
        }
    }

    private void completeInFlightOperation(long schemaId,
                                           InternalCompletableFuture<Collection<UUID>> future,
                                           Collection<UUID> memberUuids
    ) {
        synchronized (mutex) {
            markSchemaAsReplicated(schemaId);
            inFlightOperations.remove(schemaId, future);
        }
        future.complete(memberUuids);
    }

    private void completeInFlightOperationExceptionally(long schemaId,
                                                        InternalCompletableFuture<Collection<UUID>> future,
                                                        Throwable t
    ) {
        inFlightOperations.remove(schemaId, future);
        future.completeExceptionally(t);
    }

    // Not private for tests
    InternalCompletableFuture<Collection<UUID>> sendRequestForPreparation(Schema schema) {
        return InvocationUtil.invokeOnStableClusterParallelExcludeLocal(
                nodeEngine,
                new PrepareSchemaReplicationOperationSupplier(schema, nodeEngine),
                MAX_RETRIES_FOR_REQUESTS
        );
    }

    // Not private for tests
    InternalCompletableFuture<Collection<UUID>> sendRequestForAcknowledgment(long schemaId) {
        return InvocationUtil.invokeOnStableClusterParallelExcludeLocal(
                nodeEngine,
                new AckSchemaReplicationOperationSupplier(schemaId, nodeEngine),
                MAX_RETRIES_FOR_REQUESTS
        );
    }

    private Collection<UUID> getCurrentMemberUuids() {
        return nodeEngine.getClusterService().getMembers()
                .stream()
                .map(Member::getUuid)
                .collect(Collectors.toList());
    }

    // Used in tests
    ConcurrentHashMap<Long, InternalCompletableFuture<Collection<UUID>>> getInFlightOperations() {
        return inFlightOperations;
    }

    private static final class PrepareSchemaReplicationOperationSupplier implements Supplier<Operation> {

        private final Schema schema;
        private final NodeEngine nodeEngine;

        PrepareSchemaReplicationOperationSupplier(Schema schema, NodeEngine nodeEngine) {
            this.schema = schema;
            this.nodeEngine = nodeEngine;
        }

        @Override
        public Operation get() {
            int memberListVersion = nodeEngine.getClusterService().getMemberListVersion();
            return new PrepareSchemaReplicationOperation(schema, memberListVersion);
        }
    }

    private static final class AckSchemaReplicationOperationSupplier implements Supplier<Operation> {

        private final long schemaId;
        private final NodeEngine nodeEngine;

        AckSchemaReplicationOperationSupplier(long schemaId, NodeEngine nodeEngine) {
            this.schemaId = schemaId;
            this.nodeEngine = nodeEngine;
        }

        @Override
        public Operation get() {
            int memberListVersion = nodeEngine.getClusterService().getMemberListVersion();
            return new AckSchemaReplicationOperation(schemaId, memberListVersion);
        }
    }
}

