/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.internal.partition.impl.InternalPartitionImpl.getReplicaIndex;

/**
 * Decides type and order of migrations that will move the state from current replica ownerships
 * to the targeted replica ownerships. Migrations are planned in such a way that the current replica state will be moved to
 * the targeted replica state one migration at a time. {@link MigrationDecisionCallback} implementation passed to the
 * {@link MigrationPlanner#planMigrations(Address[], Address[], MigrationDecisionCallback)} is notified with the planned
 * migrations. Planned migrations have a key property that they never decrease the available replica count of a partition.
 */
class MigrationPlanner {

    private static final boolean ASSERTION_ENABLED = MigrationPlanner.class.desiredAssertionStatus();

    interface MigrationDecisionCallback {
        void migrate(Address source, int sourceCurrentReplicaIndex, int sourceNewReplicaIndex, Address destination,
                     int destinationCurrentReplicaIndex, int destinationNewReplicaIndex);
    }

    private final ILogger logger;
    private final Address[] state = new Address[InternalPartition.MAX_REPLICA_COUNT];
    private final Set<Address> verificationSet = new HashSet<Address>();

    MigrationPlanner() {
        logger = Logger.getLogger(getClass());
    }

    MigrationPlanner(ILogger logger) {
        this.logger = logger;
    }

    // checkstyle warnings are suppressed intentionally because the algorithm is followed easier within a single coherent method.
    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity", "checkstyle:methodlength"})
    void planMigrations(Address[] oldAddresses, Address[] newAddresses, MigrationDecisionCallback callback) {
        assert oldAddresses.length == newAddresses.length : "Replica addresses with different lengths! Old: "
                + Arrays.toString(oldAddresses) + ", New: " + Arrays.toString(newAddresses);

        log("Initial state: %s", Arrays.toString(oldAddresses));
        log("Final state: %s", Arrays.toString(newAddresses));

        initState(oldAddresses);
        assertNoDuplicate(oldAddresses, newAddresses);

        // Fix cyclic partition replica movements.
        if (fixCycle(oldAddresses, newAddresses)) {
            log("Final state (after cycle fix): %s", Arrays.toString(newAddresses));
        }

        int currentIndex = 0;
        while (currentIndex < oldAddresses.length) {
            log("Current index: %d, state: %s", currentIndex, Arrays.toString(state));
            assertNoDuplicate(oldAddresses, newAddresses);

            if (newAddresses[currentIndex] == null) {
                if (state[currentIndex] != null) {
                    // Replica owner is removed and no one will own this replica.
                    log("New address is null at index: %d", currentIndex);
                    callback.migrate(state[currentIndex], currentIndex, -1, null, -1, -1);
                    state[currentIndex] = null;
                }
                currentIndex++;
                continue;
            }

            if (state[currentIndex] == null) {
                int i = getReplicaIndex(state, newAddresses[currentIndex]);
                if (i == -1) {
                    // Fresh replica copy is needed. COPY replica to newAddresses[currentIndex] from partition owner
                    log("COPY %s to index: %d", newAddresses[currentIndex], currentIndex);
                    callback.migrate(null, -1, -1, newAddresses[currentIndex], -1, currentIndex);
                    state[currentIndex] = newAddresses[currentIndex];
                    currentIndex++;
                    continue;
                }

                if (i > currentIndex) {
                    // SHIFT UP replica from i to currentIndex, copy data from partition owner
                    log("SHIFT UP-2 %s from old addresses index: %d to index: %d", state[i], i, currentIndex);
                    callback.migrate(null, -1, -1, state[i], i, currentIndex);
                    state[currentIndex] = state[i];
                    state[i] = null;
                    continue;
                }

                throw new AssertionError(
                        "Migration decision algorithm failed during SHIFT UP! INITIAL: " + Arrays.toString(oldAddresses)
                                + ", CURRENT: " + Arrays.toString(state) + ", FINAL: " + Arrays.toString(newAddresses));
            }

            if (newAddresses[currentIndex].equals(state[currentIndex])) {
                // No change, no action needed.
                currentIndex++;
                continue;
            }

            if (getReplicaIndex(newAddresses, state[currentIndex]) == -1
                    && getReplicaIndex(state, newAddresses[currentIndex]) == -1) {
                // MOVE partition replica from its old owner to new owner
                log("MOVE %s to index: %d", newAddresses[currentIndex], currentIndex);
                callback.migrate(state[currentIndex], currentIndex, -1, newAddresses[currentIndex], -1, currentIndex);
                state[currentIndex] = newAddresses[currentIndex];
                currentIndex++;
                continue;
            }

            if (getReplicaIndex(state, newAddresses[currentIndex]) == -1) {
                int newIndex = getReplicaIndex(newAddresses, state[currentIndex]);

                assert newIndex > currentIndex : "Migration decision algorithm failed during SHIFT DOWN! INITIAL: "
                        + Arrays.toString(oldAddresses) + ", CURRENT: " + Arrays.toString(state)
                        + ", FINAL: " + Arrays.toString(newAddresses);

                if (state[newIndex] == null) {
                    // IT IS A SHIFT DOWN
                    log("SHIFT DOWN %s to index: %d, COPY %s to index: %d", state[currentIndex], newIndex,
                            newAddresses[currentIndex], currentIndex);
                    callback.migrate(state[currentIndex], currentIndex, newIndex, newAddresses[currentIndex], -1, currentIndex);
                    state[newIndex] = state[currentIndex];
                } else {
                    log("MOVE-3 %s to index: %d", newAddresses[currentIndex], currentIndex);
                    callback.migrate(state[currentIndex], currentIndex, -1, newAddresses[currentIndex], -1, currentIndex);
                }

                state[currentIndex] = newAddresses[currentIndex];
                currentIndex++;
                continue;
            }

            Address target = newAddresses[currentIndex];
            int i = currentIndex;
            while (true) {
                int j = getReplicaIndex(state, target);

                assert j != -1 : "Migration algorithm failed during SHIFT UP! " + target + " is not present in "
                        + Arrays.toString(state) + ". INITIAL: " + Arrays.toString(oldAddresses)
                        + ", FINAL: " + Arrays.toString(newAddresses);

                if (newAddresses[j] == null) {
                    if (state[i] == null) {
                        log("SHIFT UP %s from old addresses index: %d to index: %d", state[j], j, i);
                        callback.migrate(state[i], i, -1, state[j], j, i);
                        state[i] = state[j];
                    } else {
                        int k = getReplicaIndex(newAddresses, state[i]);
                        if (k == -1) {
                            log("SHIFT UP %s from old addresses index: %d to index: %d with source: %s",
                                    state[j], j, i, state[i]);
                            callback.migrate(state[i], i, -1, state[j], j, i);
                            state[i] = state[j];
                        } else if (state[k] == null) {
                            // shift up + shift down
                            log("SHIFT UP %s from old addresses index: %d to index: %d AND SHIFT DOWN %s to index: %d",
                                    state[j], j, i, state[i], k);
                            callback.migrate(state[i], i, k, state[j], j, i);
                            state[k] = state[i];
                            state[i] = state[j];
                        } else {
                            // only shift up because source will come back with another move migration
                            log("SHIFT UP %s from old addresses index: %d to index: %d with source: %s will get "
                                    + "another MOVE migration to index: %d", state[j], j, i, state[i], k);
                            callback.migrate(state[i], i, -1, state[j], j, i);
                            state[i] = state[j];
                        }
                    }
                    state[j] = null;
                    break;
                } else if (getReplicaIndex(state, newAddresses[j]) == -1) {
                    // MOVE partition replica from its old owner to new owner
                    log("MOVE-2 %s  to index: %d", newAddresses[j], j);
                    callback.migrate(state[j], j, -1, newAddresses[j], -1, j);
                    state[j] = newAddresses[j];
                    break;
                } else {
                    // newAddresses[j] is also present in old partition replicas
                    target = newAddresses[j];
                    i = j;
                }
            }
        }

        assert Arrays.equals(state, newAddresses)
                : "Migration decisions failed! INITIAL: " + Arrays.toString(oldAddresses)
                + " CURRENT: " + Arrays.toString(state) + ", FINAL: " + Arrays.toString(newAddresses);
    }

    private void initState(Address[] oldAddresses) {
        Arrays.fill(state, null);
        System.arraycopy(oldAddresses, 0, state, 0, oldAddresses.length);
    }

    private void assertNoDuplicate(Address[] oldAddresses, Address[] newAddresses) {
        if (!ASSERTION_ENABLED) {
            return;
        }

        try {
            for (Address address : state) {
                if (address == null) {
                    continue;
                }
                assert verificationSet.add(address)
                        : "Migration decision algorithm failed! DUPLICATE REPLICA ADDRESSES! INITIAL: " + Arrays
                                .toString(oldAddresses) + ", CURRENT: " + Arrays.toString(state) + ", FINAL: " + Arrays
                                .toString(newAddresses);
            }
        } finally {
            verificationSet.clear();
        }
    }

    // Finds whether there's a migration cycle.
    // For example followings are cycles:
    // - [A,B] -> [B,A]
    // - [A,B,C] -> [B,C,A]
    boolean isCyclic(Address[] oldReplicas, Address[] newReplicas) {
        for (int i = 0; i < oldReplicas.length; i++) {
            final Address oldAddress = oldReplicas[i];
            final Address newAddress = newReplicas[i];

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
    boolean fixCycle(Address[] oldReplicas, Address[] newReplicas) {
        boolean cyclic = false;
        for (int i = 0; i < oldReplicas.length; i++) {
            final Address oldAddress = oldReplicas[i];
            final Address newAddress = newReplicas[i];

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

    private boolean isCyclic(Address[] oldReplicas, Address[] newReplicas, final int index) {
        final Address newOwner = newReplicas[index];
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

    private void fixCycle(Address[] oldReplicas, Address[] newReplicas, int index) {
        while (true) {
            int nextIndex = InternalPartitionImpl.getReplicaIndex(newReplicas, oldReplicas[index]);
            newReplicas[index] = oldReplicas[index];
            if (nextIndex == -1) {
                return;
            }
            index = nextIndex;
        }
    }

    private void log(String log, Object... args) {
        if (logger.isFinestEnabled()) {
            logger.finest(String.format(log, args));
        }
    }
}
