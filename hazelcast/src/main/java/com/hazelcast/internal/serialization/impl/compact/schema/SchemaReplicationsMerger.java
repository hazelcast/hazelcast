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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.util.FutureUtil;
import com.hazelcast.spi.impl.InternalCompletableFuture;

import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * A runnable to be executed during the split-brain merge process, which
 * replicates schemas in the smaller cluster to larger cluster, if they are
 * not replicated in the larger cluster yet.
 */
public final class SchemaReplicationsMerger implements Runnable {

    private final SchemaReplicator replicator;
    private final Collection<SchemaReplication> replications;

    public SchemaReplicationsMerger(
            SchemaReplicator replicator,
            Collection<SchemaReplication> replications) {
        this.replicator = replicator;
        this.replications = replications;
    }

    @Override
    public void run() {
        try {
            // Filter out replications already present in the larger cluster.
            List<InternalCompletableFuture<Collection<UUID>>> replicationFutures = replications.stream()
                    .filter(replication -> {
                        SchemaReplicationStatus status = replicator.getReplicationStatus(replication.getSchema());
                        if (status == null) {
                            // We don't even have a replication for it in the
                            // larger cluster
                            return true;
                        }

                        // Let's replicate it if it is not replicated yet in
                        // the larger cluster
                        return status != SchemaReplicationStatus.REPLICATED;
                    })
                    .map(replication -> replicator.replicate(replication.getSchema()))
                    .collect(Collectors.toList());

            FutureUtil.waitForever(replicationFutures, FutureUtil.RETHROW_EVERYTHING);
        } catch (Exception e) {
            throw new HazelcastException("Error while replaying replications " + replications, e);
        }
    }
}
