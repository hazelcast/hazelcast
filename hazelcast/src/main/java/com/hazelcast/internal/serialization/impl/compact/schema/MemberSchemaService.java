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

import com.hazelcast.internal.serialization.impl.compact.Schema;
import com.hazelcast.internal.serialization.impl.compact.SchemaService;
import com.hazelcast.internal.services.CoreService;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.internal.services.PreJoinAwareService;
import com.hazelcast.internal.services.SplitBrainHandlerService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Responsible for replicating the schemas across the cluster and giving a
 * mechanism for clients/members to access schemas that they don't own in their
 * local registry.
 */
public class MemberSchemaService implements
        ManagedService,
        PreJoinAwareService<SendSchemaReplicationsOperation>,
        SchemaService,
        SplitBrainHandlerService,
        CoreService {

    private final ConcurrentHashMap<Long, Schema> schemas = new ConcurrentHashMap<>();
    private final SchemaReplicator replicator = new SchemaReplicator(this);

    private ILogger logger;

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.logger = nodeEngine.getLogger(SchemaService.class);

        // Set the node engine reference to replicator as well.
        replicator.init(nodeEngine);
    }

    @Override
    public void reset() {
        // Called in the current node before it joins to larger cluster in the
        // split-brain healing process. Resetting the state of the replications
        // allow replaying the replications in the larger cluster in this node.
        // Before calling this, we already copied current state of the
        // replications so that the replications occurred in the smaller
        // cluster can be replicated to the larger cluster as well.
        // See `prepareMergeRunnable` for that.
        replicator.clear();
    }

    @Override
    public void shutdown(boolean terminate) {
        schemas.clear();
        replicator.clear();
    }

    @Nullable
    @Override
    public Schema get(long schemaId) {
        return schemas.get(schemaId);
    }

    @Override
    public void put(Schema schema) {
        putAsync(schema).join();
    }

    /**
     * Puts the schema to the cluster, by replicating it across the cluster, if
     * necessary.
     *
     * @param schema to replicate.
     */
    public InternalCompletableFuture<Collection<UUID>> putAsync(Schema schema) {
        return replicator.replicate(schema);
    }

    /**
     * Out of the {@code schemas} sent by the client, only replicates the ones
     * that are not yet replicated in the cluster.
     *
     * @param schemas to replicate, if necessary
     */
    public InternalCompletableFuture<Void> putAllAsync(List<Schema> schemas) {
        if (schemas.isEmpty()) {
            return InternalCompletableFuture.newCompletedFuture(null);
        }
        if (logger.isFinestEnabled()) {
            logger.finest("Putting schemas to the cluster" + schemas);
        }

        return replicator.replicateAll(schemas);
    }

    @Override
    public void putLocal(Schema schema) {
        long schemaId = schema.getSchemaId();
        Schema existingSchema = schemas.putIfAbsent(schemaId, schema);
        if (existingSchema == null) {
            return;
        }
        if (!schema.equals(existingSchema)) {
            throw new IllegalStateException("Schema with schemaId " + schemaId + " already exists. Existing schema "
                    + existingSchema + "new schema " + schema);
        }
    }

    /**
     * Returns the list of schemas that are stored in the in-memory registry of
     * the service.
     */
    public Collection<Schema> getAllSchemas() {
        return schemas.values();
    }

    @Override
    public SendSchemaReplicationsOperation getPreJoinOperation() {
        // Called in the master node to retrieve an operation that will be
        // executed on the joining member.

        Collection<SchemaReplication> replications = replicator.getReplications();
        if (replications.isEmpty()) {
            // Nothing has been replicated to master node, no need to return
            // an operation to invoke.
            return null;
        }

        if (logger.isFinestEnabled()) {
            logger.finest("Preparing pre-join operation with replications " + replications);
        }

        return new SendSchemaReplicationsOperation(replications);
    }

    /**
     * Called in the joining member to replay all the replications that were
     * available in the master node, when the pre-join operation is prepared.
     * <p>
     * It puts the schemas to the in-memory registry of the joining member,
     * persists them to HotRestart, and updates the local replicator with the
     * replications using the same status as they are sent from the master.
     * <p>
     * See the documentation of {@link SendSchemaReplicationsOperation} to see
     * the idea behind this.
     *
     * @param replications to replay.
     */
    public void replayReplications(Collection<SchemaReplication> replications) {
        for (SchemaReplication replication : replications) {
            Schema schema = replication.getSchema();
            putLocal(schema);
        }
        List<Schema> schemas = replications.stream()
                .map(SchemaReplication::getSchema)
                .collect(Collectors.toList());

        persistAllSchemasToHotRestart(schemas);
        replicator.setReplications(replications);
    }

    /**
     * Called on the participant members to prepare for the replication process
     * by:
     * <ul>
     *     <li>Putting the schema to an in-memory registry</li>
     *     <li>Persisting the schema to HotRestart(if available)</li>
     *     <li>Marking the schema as {@link SchemaReplicationStatus#PREPARED}
     *     </li>
     * </ul>
     *
     * @param schema to be prepared for replication.
     */
    public void onSchemaPreparationRequest(Schema schema) {
        // If it is already PREPARED or REPLICATED, do nothing
        if (replicator.getReplicationStatus(schema) != null) {
            return;
        }

        putLocal(schema);
        persistSchemaToHotRestart(schema);
        replicator.markSchemaAsPrepared(schema);
    }

    /**
     * Called on the participant members to mark the replication status of the
     * schema as {@link SchemaReplicationStatus#REPLICATED}.
     *
     * @param schemaId of the schema that is replicated to the cluster.
     */
    public void onSchemaAckRequest(long schemaId) {
        replicator.markSchemaAsReplicated(schemaId);
    }

    /**
     * Called when the schemas are read from the HotRestart data.
     * <p>
     * Each schema will be put into the in-memory registry and will be marked as
     * {@link SchemaReplicationStatus#PREPARED}, as we cannot distinguish
     * schemas that are {@link SchemaReplicationStatus#PREPARED} and
     * {@link SchemaReplicationStatus#REPLICATED} from the HotRestart data.
     *
     * @param schemas read from the HotRestart data
     */
    public void onHotRestartRestore(Collection<Schema> schemas) {
        for (Schema schema : schemas) {
            putLocal(schema);
            replicator.markSchemaAsPrepared(schema);
        }
    }

    @Override
    public Runnable prepareMergeRunnable() {
        // Called in the member of the smaller cluster that will join the
        // larger cluster.

        // Since this class implements CoreService interface, we are sure that
        // this task will run before any task that would merge the data
        // (like MapService etc.) from smaller cluster to larger cluster.
        // That makes sure that the schema is replicated before the data.

        // List of schemas that are replicated in the smaller cluster.
        Collection<SchemaReplication> replications = replicator.getReplications();

        return new SchemaReplicationsMerger(replicator, replications);
    }

    /**
     * Persists the given schemas to HotRestart, if it is available.
     */
    private void persistSchemaToHotRestart(Schema schema) {
        persistSchemaToHotRestartAsync(schema).join();
    }

    /**
     * Persists all the schemas to HotRestart, if it is available, and wait for
     * it to complete.
     */
    protected void persistAllSchemasToHotRestart(Collection<Schema> schemas) {
        // no-op, actual implementation is in the EnterpriseMemberSchemaService
    }

    protected InternalCompletableFuture<Void> persistSchemaToHotRestartAsync(Schema schema) {
        // no-op, actual implementation is in the EnterpriseMemberSchemaService
        return InternalCompletableFuture.newCompletedFuture(null);
    }

    // Used only for testing
    SchemaReplicator getReplicator() {
        return replicator;
    }
}
