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

package com.hazelcast.internal.partition.impl;

import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.hazelcast.internal.partition.impl.InternalPartitionImpl.getReplicaIndex;
import static java.lang.String.format;

/**
 * Decides type and order of migrations that will move the state from current replica ownerships
 * to the targeted replica ownerships. Migrations are planned in such a way that the current replica state
 * will be moved to the targeted replica state one migration at a time.
 * {@link MigrationDecisionCallback} implementation passed to the
 * {@link MigrationPlanner#planMigrations(int, PartitionReplica[], PartitionReplica[], MigrationDecisionCallback)}
 * is notified with the planned migrations.
 * Planned migrations have a key property that they never decrease the available replica count of a partition.
 */
class MigrationPlanner {

    private static final boolean ASSERTION_ENABLED = MigrationPlanner.class.desiredAssertionStatus();

    interface MigrationDecisionCallback {
        void migrate(PartitionReplica source, int sourceCurrentReplicaIndex, int sourceNewReplicaIndex,
                PartitionReplica destination, int destinationCurrentReplicaIndex, int destinationNewReplicaIndex);
    }

    private final ILogger logger;
    private final PartitionReplica[] state = new PartitionReplica[InternalPartition.MAX_REPLICA_COUNT];
    private final Set<PartitionReplica> verificationSet = ASSERTION_ENABLED ? new HashSet<>() : Collections.emptySet();

    MigrationPlanner() {
        logger = Logger.getLogger(getClass());
    }

    MigrationPlanner(ILogger logger) {
        this.logger = logger;
    }

    // the CheckStyle warnings are suppressed intentionally, because the algorithm is followed easier within fewer methods
    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity", "checkstyle:methodlength"})
    void planMigrations(int partitionId, PartitionReplica[] oldReplicas, PartitionReplica[] newReplicas,
            MigrationDecisionCallback callback) {
        assert oldReplicas.length == newReplicas.length : "Replica addresses with different lengths! Old: "
                + Arrays.toString(oldReplicas) + ", New: " + Arrays.toString(newReplicas);

        if (logger.isFinestEnabled()) {
            logger.finest(format("partitionId=%d, Initial state: %s", partitionId, Arrays.toString(oldReplicas)));
            logger.finest(format("partitionId=%d, Final state: %s", partitionId, Arrays.toString(newReplicas)));
        }

        initState(oldReplicas);
        assertNoDuplicate(partitionId, oldReplicas, newReplicas);

        // fix cyclic partition replica movements
        if (fixCycle(oldReplicas, newReplicas)) {
            if (logger.isFinestEnabled()) {
                logger.finest(format("partitionId=%d, Final state (after cycle fix): %s", partitionId,
                        Arrays.toString(newReplicas)));
            }
        }

        int currentIndex = 0;
        while (currentIndex < oldReplicas.length) {
            if (logger.isFinestEnabled()) {
                logger.finest(format("partitionId=%d, Current index: %d, state: %s", partitionId, currentIndex,
                        Arrays.toString(state)));
            }
            assertNoDuplicate(partitionId, oldReplicas, newReplicas);

            if (newReplicas[currentIndex] == null) {
                if (state[currentIndex] != null) {
                    // replica owner is removed and no one will own this replica
                    trace("partitionId=%d, New address is null at index: %d", partitionId, currentIndex);
                    callback.migrate(state[currentIndex], currentIndex, -1, null, -1, -1);
                    state[currentIndex] = null;
                }
                currentIndex++;
                continue;
            }

            if (state[currentIndex] == null) {
                int i = getReplicaIndex(state, newReplicas[currentIndex]);
                if (i == -1) {
                    // fresh replica copy is needed, so COPY replica to newReplicas[currentIndex] from partition owner
                    trace("partitionId=%d, COPY %s to index: %d", partitionId, newReplicas[currentIndex], currentIndex);
                    callback.migrate(null, -1, -1, newReplicas[currentIndex], -1, currentIndex);
                    state[currentIndex] = newReplicas[currentIndex];
                    currentIndex++;
                    continue;
                }

                if (i > currentIndex) {
                    // SHIFT UP replica from i to currentIndex, copy data from partition owner
                    trace("partitionId=%d, SHIFT UP-2 %s from old addresses index: %d to index: %d", partitionId,
                            state[i], i, currentIndex);
                    callback.migrate(null, -1, -1, state[i], i, currentIndex);
                    state[currentIndex] = state[i];
                    state[i] = null;
                    continue;
                }

                throw new AssertionError("partitionId=" + partitionId
                        + "Migration decision algorithm failed during SHIFT UP! INITIAL: " + Arrays.toString(oldReplicas)
                        + ", CURRENT: " + Arrays.toString(state) + ", FINAL: " + Arrays.toString(newReplicas));
            }

            if (newReplicas[currentIndex].equals(state[currentIndex])) {
                // no change, no action needed
                currentIndex++;
                continue;
            }

            if (getReplicaIndex(newReplicas, state[currentIndex]) == -1
                    && getReplicaIndex(state, newReplicas[currentIndex]) == -1) {
                // MOVE partition replica from its old owner to new owner
                trace("partitionId=%d, MOVE %s to index: %d", partitionId, newReplicas[currentIndex], currentIndex);
                callback.migrate(state[currentIndex], currentIndex, -1, newReplicas[currentIndex], -1, currentIndex);
                state[currentIndex] = newReplicas[currentIndex];
                currentIndex++;
                continue;
            }

            if (getReplicaIndex(state, newReplicas[currentIndex]) == -1) {
                int newIndex = getReplicaIndex(newReplicas, state[currentIndex]);
                assert newIndex > currentIndex : "partitionId=" + partitionId
                        + ", Migration decision algorithm failed during SHIFT DOWN! INITIAL: "
                        + Arrays.toString(oldReplicas) + ", CURRENT: " + Arrays.toString(state)
                        + ", FINAL: " + Arrays.toString(newReplicas);

                if (state[newIndex] == null) {
                    // it is a SHIFT DOWN
                    trace("partitionId=%d, SHIFT DOWN %s to index: %d, COPY %s to index: %d", partitionId, state[currentIndex],
                            newIndex, newReplicas[currentIndex], currentIndex);
                    callback.migrate(state[currentIndex], currentIndex, newIndex, newReplicas[currentIndex], -1, currentIndex);
                    state[newIndex] = state[currentIndex];
                } else {
                    trace("partitionId=%d, MOVE-3 %s to index: %d", partitionId, newReplicas[currentIndex], currentIndex);
                    callback.migrate(state[currentIndex], currentIndex, -1, newReplicas[currentIndex], -1, currentIndex);
                }

                state[currentIndex] = newReplicas[currentIndex];
                currentIndex++;
                continue;
            }

            planMigrations(partitionId, oldReplicas, newReplicas, callback, currentIndex);
        }

        assert Arrays.equals(state, newReplicas)
                : "partitionId=" + partitionId + ", Migration decisions failed! INITIAL: " + Arrays.toString(oldReplicas)
                + " CURRENT: " + Arrays.toString(state) + ", FINAL: " + Arrays.toString(newReplicas);
    }

    private void planMigrations(int partitionId, PartitionReplica[] oldMembers, PartitionReplica[] newReplicas,
            MigrationDecisionCallback callback, int currentIndex) {
        while (true) {
            int targetIndex = getReplicaIndex(state, newReplicas[currentIndex]);
            assert targetIndex != -1 : "partitionId=" + partitionId + ", Migration algorithm failed during SHIFT UP! "
                    + newReplicas[currentIndex] + " is not present in " + Arrays.toString(state)
                    + ". INITIAL: " + Arrays.toString(oldMembers) + ", FINAL: " + Arrays.toString(newReplicas);

            if (newReplicas[targetIndex] == null) {
                if (state[currentIndex] == null) {
                    trace("partitionId=%d, SHIFT UP %s from old addresses index: %d to index: %d", partitionId,
                            state[targetIndex], targetIndex, currentIndex);
                    callback.migrate(state[currentIndex], currentIndex, -1, state[targetIndex], targetIndex, currentIndex);
                    state[currentIndex] = state[targetIndex];
                } else {
                    int newIndex = getReplicaIndex(newReplicas, state[currentIndex]);
                    if (newIndex == -1) {
                        trace("partitionId=%d, SHIFT UP %s from old addresses index: %d to index: %d with source: %s",
                                partitionId, state[targetIndex], targetIndex, currentIndex, state[currentIndex]);
                        callback.migrate(state[currentIndex], currentIndex, -1, state[targetIndex], targetIndex, currentIndex);
                        state[currentIndex] = state[targetIndex];
                    } else if (state[newIndex] == null) {
                        // SHIFT UP + SHIFT DOWN
                        trace("partitionId=%d, SHIFT UP %s from old addresses index: %d to index: %d "
                                        + "and SHIFT DOWN %s to index: %d",
                                partitionId, state[targetIndex], targetIndex, currentIndex, state[currentIndex], newIndex);
                        callback.migrate(state[currentIndex], currentIndex, newIndex, state[targetIndex], targetIndex,
                                currentIndex);
                        state[newIndex] = state[currentIndex];
                        state[currentIndex] = state[targetIndex];
                    } else {
                        // only SHIFT UP because source will come back with another move migration
                        trace("partitionId=%d, SHIFT UP %s from old addresses index: %d to index: %d with source: %s will get "
                                        + "another MOVE migration to index: %d", partitionId, state[targetIndex],
                                targetIndex, currentIndex, state[currentIndex], newIndex);
                        callback.migrate(state[currentIndex], currentIndex, -1, state[targetIndex], targetIndex, currentIndex);
                        state[currentIndex] = state[targetIndex];
                    }
                }
                state[targetIndex] = null;
                break;
            } else if (getReplicaIndex(state, newReplicas[targetIndex]) == -1) {
                // MOVE partition replica from its old owner to new owner
                trace("partitionId=%d, MOVE-2 %s  to index: %d", partitionId, newReplicas[targetIndex], targetIndex);
                callback.migrate(state[targetIndex], targetIndex, -1, newReplicas[targetIndex], -1, targetIndex);
                state[targetIndex] = newReplicas[targetIndex];
                break;
            } else {
                // newReplicas[targetIndex] is also present in old partition replicas
                currentIndex = targetIndex;
            }
        }
    }

    /**
     * Prioritizes a COPY / SHIFT UP migration against
     * - a non-conflicting MOVE migration on a hotter index,
     * - a non-conflicting SHIFT DOWN migration to a colder index.
     * Non-conflicting migrations have no common participant. Otherwise, order of the migrations should not be changed.
     * This method is called for migrations per partition.
     * <p>
     * The main motivation of the prioritization is COPY / SHIFT UP migrations increase
     * the available replica count of a migration while a MOVE migration doesn't have an effect on it.
     *
     * @param migrations migrations to perform prioritization
     */
    void prioritizeCopiesAndShiftUps(List<MigrationInfo> migrations) {
        for (int i = 0; i < migrations.size(); i++) {
            prioritize(migrations, i);
        }

        if (logger.isFinestEnabled()) {
            StringBuilder s = new StringBuilder("Migration order after prioritization: [");
            int ix = 0;
            for (MigrationInfo migration : migrations) {
                s.append("\n\t").append(ix++).append("- ").append(migration).append(",");
            }
            s.deleteCharAt(s.length() - 1);
            s.append("]");
            logger.finest(s.toString());
        }
    }

    private void prioritize(List<MigrationInfo> migrations, int i) {
        MigrationInfo migration = migrations.get(i);

        trace("Trying to prioritize migration: %s", migration);

        if (migration.getSourceCurrentReplicaIndex() != -1) {
            trace("Skipping non-copy migration: %s", migration);
            return;
        }

        int k = i - 1;
        for (; k >= 0; k--) {
            MigrationInfo other = migrations.get(k);
            if (other.getSourceCurrentReplicaIndex() == -1) {
                trace("Cannot prioritize against a copy / shift up. other: %s", other);
                break;
            }

            if (migration.getDestination().equals(other.getSource())
                    || migration.getDestination().equals(other.getDestination())) {
                trace("Cannot prioritize against a conflicting migration. other: %s", other);
                break;
            }

            if (other.getSourceNewReplicaIndex() != -1
                    && other.getSourceNewReplicaIndex() < migration.getDestinationNewReplicaIndex()) {
                trace("Cannot prioritize against a hotter shift down. other: %s", other);
                break;
            }
        }

        if ((k + 1) != i) {
            trace("Prioritizing migration %s to: %d", migration, (k + 1));
            migrations.remove(i);
            migrations.add(k + 1, migration);
        }
    }

    private void initState(PartitionReplica[] oldAddresses) {
        Arrays.fill(state, null);
        System.arraycopy(oldAddresses, 0, state, 0, oldAddresses.length);
    }

    private void assertNoDuplicate(int partitionId, PartitionReplica[] oldReplicas, PartitionReplica[] newReplicas) {
        if (!ASSERTION_ENABLED) {
            return;
        }

        try {
            for (PartitionReplica replica : state) {
                if (replica == null) {
                    continue;
                }
                assert verificationSet.add(replica)
                        : "partitionId=" + partitionId + ", Migration decision algorithm failed! DUPLICATE REPLICA ADDRESSES!"
                        + " INITIAL: " + Arrays.toString(oldReplicas) + ", CURRENT: " + Arrays.toString(state)
                        + ", FINAL: " + Arrays.toString(newReplicas);
            }
        } finally {
            verificationSet.clear();
        }
    }

    // Finds whether there's a migration cycle.
    // For example followings are cycles:
    // - [A,B] -> [B,A]
    // - [A,B,C] -> [B,C,A]
    boolean isCyclic(PartitionReplica[] oldReplicas, PartitionReplica[] newReplicas) {
        for (int i = 0; i < oldReplicas.length; i++) {
            final PartitionReplica oldAddress = oldReplicas[i];
            final PartitionReplica newAddress = newReplicas[i];

            if (oldAddress == null || newAddress == null || oldAddress.equals(newAddress)) {
                continue;
            }

            if (isCyclic(oldReplicas, newReplicas, i)) {
                return true;
            }
        }
        return false;
    }

    // Fix cyclic partition replica movements.
    // When there are cycles among replica migrations, it's impossible to define a migration order.
    // For example followings are cycles:
    // - [A,B] -> [B,A]
    // - [A,B,C] -> [B,C,A]
    boolean fixCycle(PartitionReplica[] oldReplicas, PartitionReplica[] newReplicas) {
        boolean cyclic = false;
        for (int i = 0; i < oldReplicas.length; i++) {
            final PartitionReplica oldAddress = oldReplicas[i];
            final PartitionReplica newAddress = newReplicas[i];

            if (oldAddress == null || newAddress == null || oldAddress.equals(newAddress)) {
                continue;
            }

            if (isCyclic(oldReplicas, newReplicas, i)) {
                fixCycle(oldReplicas, newReplicas, i);
                cyclic = true;
            }
        }
        return cyclic;
    }

    private boolean isCyclic(PartitionReplica[] oldReplicas, PartitionReplica[] newReplicas, final int index) {
        final PartitionReplica newOwner = newReplicas[index];
        int firstIndex = index;

        while (true) {
            int nextIndex = InternalPartitionImpl.getReplicaIndex(newReplicas, oldReplicas[firstIndex]);
            if (nextIndex == -1) {
                return false;
            }

            if (firstIndex == nextIndex) {
                return false;
            }

            if (newOwner.equals(oldReplicas[nextIndex])) {
                return true;
            }

            firstIndex = nextIndex;
        }
    }

    private void fixCycle(PartitionReplica[] oldReplicas, PartitionReplica[] newReplicas, int index) {
        while (true) {
            int nextIndex = InternalPartitionImpl.getReplicaIndex(newReplicas, oldReplicas[index]);
            newReplicas[index] = oldReplicas[index];
            if (nextIndex == -1) {
                return;
            }
            index = nextIndex;
        }
    }

    private void trace(String log, Object... args) {
        if (logger.isFinestEnabled()) {
            logger.finest(format(log, args));
        }
    }
}
