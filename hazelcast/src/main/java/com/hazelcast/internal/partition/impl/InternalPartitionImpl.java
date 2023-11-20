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

package com.hazelcast.internal.partition.impl;

import com.hazelcast.internal.partition.AbstractInternalPartition;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.internal.partition.PartitionReplicaInterceptor;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import static java.util.Arrays.copyOf;

public class InternalPartitionImpl extends AbstractInternalPartition implements InternalPartition {

    @SuppressFBWarnings(value = "VO_VOLATILE_REFERENCE_TO_ARRAY", justification =
            "The contents of this array will never be updated, so it can be safely read using a volatile read."
                    + " Writing to `replicas` is done under InternalPartitionServiceImpl.lock,"
                    + " so there's no need to guard `replicas` field or to use a CAS.")
    private volatile PartitionReplica[] replicas = new PartitionReplica[MAX_REPLICA_COUNT];
    private final PartitionReplicaInterceptor interceptor;
    private volatile int version;
    private volatile PartitionReplica localReplica;
    private volatile boolean isMigrating;

    InternalPartitionImpl(int partitionId, PartitionReplica localReplica, PartitionReplicaInterceptor interceptor) {
        super(partitionId);
        this.localReplica = localReplica;
        this.interceptor = interceptor;
    }

    @SuppressFBWarnings("EI_EXPOSE_REP")
    public InternalPartitionImpl(int partitionId, PartitionReplica localReplica, PartitionReplica[] replicas, int version,
            PartitionReplicaInterceptor interceptor) {
        this(partitionId, localReplica, interceptor);
        this.replicas = replicas;
        this.version = version;
    }

    @Override
    public boolean isMigrating() {
        return isMigrating;
    }

    /**
     * Sets migrating flag if it's not set already.
     * @return true if migrating flag is updated, false otherwise
     */
    public boolean setMigrating() {
        if (isMigrating) {
            return false;
        }
        isMigrating = true;
        return true;
    }

    /**
     * Resets migrating flag.
     */
    public void resetMigrating() {
        isMigrating = false;
    }

    @Override
    public boolean isLocal() {
        PartitionReplica local = localReplica;
        return local != null && local.equals(getOwnerReplicaOrNull());
    }

    @Override
    public int version() {
        return version;
    }

    @Override
    public PartitionReplica getReplica(int replicaIndex) {
        return replicas[replicaIndex];
    }

    /** Swaps the replicas for {@code index1} and {@code index2} and call the partition listeners */
    void swapReplicas(int index1, int index2) {
        PartitionReplica[] newReplicas = copyOf(replicas, MAX_REPLICA_COUNT);

        PartitionReplica a1 = newReplicas[index1];
        PartitionReplica a2 = newReplicas[index2];
        newReplicas[index1] = a2;
        newReplicas[index2] = a1;

        replicas = newReplicas;
        onReplicaChange(index1, a1, a2);
        onReplicaChange(index2, a2, a1);
    }

    /**
     * This method is always called from batch partition update situations, so it does
     * not invoke interceptors individually per partition.
     * (apply partition assignments from master while joining, apply partition table recovered
     * from hot restart)
     * @return {@code true} if partition owner was changed, otherwise {@code false}.
     */
    boolean setReplicasAndVersion(InternalPartition partition) {
        boolean ownerChanged = setReplicas(partition.getReplicasCopy(), false);
        version = partition.version();
        return ownerChanged;
    }

    void setVersion(int version) {
        this.version = version;
    }

    // Not doing a defensive copy of given Address[]
    // This method is called under InternalPartitionServiceImpl.lock,
    // so there's no need to guard `addresses` field or to use a CAS.
    void setReplicas(PartitionReplica[] newReplicas) {
        PartitionReplica[] oldReplicas = replicas;
        replicas = newReplicas;
        onReplicasChange(newReplicas, oldReplicas);
    }

    /**
     * Variant of {@link #setReplicas(PartitionReplica[])} that's suitable for batch partition replica updates.
     *
     * @return {@code true} if partition owner was changed, otherwise {@code false}.
     */
    boolean setReplicas(PartitionReplica[] newReplicas, boolean invokeInterceptor) {
        PartitionReplica[] oldReplicas = replicas;
        replicas = newReplicas;
        return onReplicasChange(newReplicas, oldReplicas, invokeInterceptor);
    }

    void setReplica(int replicaIndex, PartitionReplica newReplica) {
        PartitionReplica[] newReplicas = copyOf(replicas, MAX_REPLICA_COUNT);
        PartitionReplica oldReplica = newReplicas[replicaIndex];
        newReplicas[replicaIndex] = newReplica;
        replicas = newReplicas;
        onReplicaChange(replicaIndex, oldReplica, newReplica);
    }

    /**
     * Calls the partition replica change interceptor for all changed replicas.
     * @return {@code true} if partition owner change was detected, otherwise {@code false}.
     */
    private boolean onReplicasChange(PartitionReplica[] newReplicas, PartitionReplica[] oldReplicas) {
        return onReplicasChange(newReplicas, oldReplicas, true);
    }

    private boolean onReplicasChange(PartitionReplica[] newReplicas, PartitionReplica[] oldReplicas, boolean invokeInterceptor) {
        PartitionReplica oldReplicasOwner = oldReplicas[0];
        PartitionReplica newReplicasOwner = newReplicas[0];
        boolean partitionOwnerChanged = onReplicaChange(0, oldReplicasOwner, newReplicasOwner, invokeInterceptor);

        for (int replicaIndex = 1; replicaIndex < MAX_REPLICA_COUNT; replicaIndex++) {
            PartitionReplica oldReplicasId = oldReplicas[replicaIndex];
            PartitionReplica newReplicasId = newReplicas[replicaIndex];
            onReplicaChange(replicaIndex, oldReplicasId, newReplicasId, invokeInterceptor);
        }
        return partitionOwnerChanged;
    }

    /**
     * If a replica change is detected, then increments partition version and calls the partition replica change interceptor
     * for the changed replica.
     * @return {@code true} if a replica change was detected, otherwise {@code false}.
     */
    @SuppressFBWarnings(value = "VO_VOLATILE_INCREMENT",
            justification = "This method is called under InternalPartitionServiceImpl.lock")
    private boolean onReplicaChange(int replicaIndex, PartitionReplica oldReplica, PartitionReplica newReplica) {
        return onReplicaChange(replicaIndex, oldReplica, newReplica, true);
    }

    /**
     * Calls the partition replica change interceptor for the changed replica.
     * @return {@code true} if a replica change was detected, otherwise {@code false}.
     */
    @SuppressFBWarnings(value = "VO_VOLATILE_INCREMENT",
            justification = "This method is called under InternalPartitionServiceImpl.lock")
    private boolean onReplicaChange(int replicaIndex, PartitionReplica oldReplica, PartitionReplica newReplica,
                                 boolean invokeInterceptor) {
        boolean changed;
        if (oldReplica == null) {
            changed = newReplica != null;
        } else {
            changed = !oldReplica.equals(newReplica);
        }
        if (!changed) {
            return false;
        }
        version++;
        if (interceptor != null && invokeInterceptor) {
            interceptor.replicaChanged(partitionId, replicaIndex, oldReplica, newReplica);
        }
        return true;
    }

    InternalPartitionImpl copy(PartitionReplicaInterceptor interceptor) {
        return new InternalPartitionImpl(partitionId, localReplica, copyOf(replicas, MAX_REPLICA_COUNT), version, interceptor);
    }

    @Override
    protected PartitionReplica[] replicas() {
        return replicas;
    }

    int replaceReplica(PartitionReplica oldReplica, PartitionReplica newReplica) {
        for (int i = 0; i < MAX_REPLICA_COUNT; i++) {
            PartitionReplica currentReplica = replicas[i];
            if (currentReplica == null) {
                break;
            }

            if (currentReplica.equals(oldReplica)) {
                PartitionReplica[] newReplicas = copyOf(replicas, MAX_REPLICA_COUNT);
                newReplicas[i] = newReplica;
                replicas = newReplicas;
                onReplicaChange(i, oldReplica, newReplica);
                return i;
            }
        }
        return -1;
    }

    void reset(PartitionReplica localReplica) {
        assert localReplica != null;
        this.replicas = new PartitionReplica[MAX_REPLICA_COUNT];
        this.localReplica = localReplica;
        version = 0;
        resetMigrating();
    }
}
