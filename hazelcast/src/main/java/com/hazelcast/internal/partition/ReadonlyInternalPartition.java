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

package com.hazelcast.internal.partition;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Readonly/immutable implementation of {@link InternalPartition} interface.
 */
public class ReadonlyInternalPartition extends AbstractInternalPartition {

    private final PartitionReplica[] replicas;
    private final int version;

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public ReadonlyInternalPartition(PartitionReplica[] replicas, int partitionId, int version) {
        super(partitionId);
        this.replicas = replicas;
        this.version = version;
    }

    public ReadonlyInternalPartition(InternalPartition partition) {
        super(partition.getPartitionId());
        this.replicas = partition.getReplicasCopy();
        this.version = partition.version();
    }

    @Override
    public boolean isLocal() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isMigrating() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int version() {
        return version;
    }

    @Override
    protected PartitionReplica[] replicas() {
        return replicas;
    }
}
