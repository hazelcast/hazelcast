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
    private static final boolean TRACE = false;

    interface MigrationDecisionCallback {
        void migrate(Address source, int sourceCurrentReplicaIndex, int sourceNewReplicaIndex, Address destination,
                     int destinationCurrentReplicaIndex, int destinationNewReplicaIndex);
    }

    private final Set<Address> verificationSet = new HashSet<Address>();

    // checkstyle warnings are suppressed intentionally because the algorithm is followed easier within a single coherent method.
    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity", "checkstyle:methodlength"})
    void planMigrations(Address[] oldAddresses, Address[] newAddresses, MigrationDecisionCallback callback) {
        assert oldAddresses.length == newAddresses.length : "Replica addresses with different lengths! Old: "
                + Arrays.toString(oldAddresses) + ", New: " + Arrays.toString(newAddresses);

        if (TRACE) {
            callback = new TracingMigrationDecisionCallback(callback);
        }

        Address[] state = new Address[oldAddresses.length];
        System.arraycopy(oldAddresses, 0, state, 0, oldAddresses.length);

        assertNoDuplicate(oldAddresses, newAddresses, state);
        // Fix cyclic partition replica movements.
        fixCycle(oldAddresses, newAddresses);

        int currentIndex = 0;
        while (currentIndex < oldAddresses.length) {

            assertNoDuplicate(oldAddresses, newAddresses, state);

            if (newAddresses[currentIndex] == null) {
                if (state[currentIndex] != null) {
                    // Replica owner is removed and no one will own this replica.
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
                    callback.migrate(null, -1, -1, newAddresses[currentIndex], -1, currentIndex);
                    state[currentIndex] = newAddresses[currentIndex];
                    currentIndex++;
                    continue;
                }

                if (i > currentIndex) {
                    // SHIFT UP replica from i to currentIndex, copy data from partition owner
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
                callback.migrate(state[currentIndex], currentIndex, -1, newAddresses[currentIndex], -1, currentIndex);
                state[currentIndex] = newAddresses[currentIndex];
                currentIndex++;
                continue;
            }

            // IT IS A SHIFT DOWN
            if (getReplicaIndex(state, newAddresses[currentIndex]) == -1) {
                int newIndex = getReplicaIndex(newAddresses, state[currentIndex]);

                if (newIndex <= currentIndex) {
                    throw new AssertionError(
                            "Migration decision algorithm failed during SHIFT DOWN! INITIAL: " + Arrays.toString(oldAddresses)
                                    + ", CURRENT: " + Arrays.toString(state) + ", FINAL: " + Arrays.toString(newAddresses));
                }

                // SHIFT DOWN replica on its current owner from currentIndex to newIndex
                // and COPY replica on currentIndex to its new owner
                callback.migrate(state[currentIndex], currentIndex, newIndex, newAddresses[currentIndex], -1, currentIndex);
                state[newIndex] = state[currentIndex];
                state[currentIndex] = newAddresses[currentIndex];
                currentIndex++;
                continue;
            }

            Address target = newAddresses[currentIndex];
            int i = currentIndex;
            while (true) {
                int j = getReplicaIndex(state, target);

                if (j == -1) {
                    throw new AssertionError(
                            "Migration algorithm failed during SHIFT UP! " + target + " is not present in " + Arrays
                                    .toString(state) + "." + " INITIAL: " + Arrays.toString(oldAddresses) + ", FINAL: " + Arrays
                                    .toString(newAddresses));

                } else if (newAddresses[j] == null) {
                    // SHIFT UP replica from j to i, copy data from partition owner
                    callback.migrate(state[i], i, -1, state[j], j, i);
                    state[i] = state[j];
                    state[j] = null;
                    break;
                } else if (getReplicaIndex(state, newAddresses[j]) == -1) {
                    // MOVE partition replica from its old owner to new owner
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

        if (!Arrays.equals(state, newAddresses)) {
            throw new AssertionError(
                    "Migration decisions failed! INITIAL: " + Arrays.toString(oldAddresses) + " CURRENT: " + Arrays
                            .toString(state) + ", FINAL: " + Arrays.toString(newAddresses));
        }
    }

    private void assertNoDuplicate(Address[] oldAddresses, Address[] newAddresses, Address[] state) {
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

        int k = 0;
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

    private static final class TracingMigrationDecisionCallback
            implements MigrationDecisionCallback {

        final MigrationDecisionCallback delegate;

        private TracingMigrationDecisionCallback(MigrationDecisionCallback delegate) {
            this.delegate = delegate;
        }

        @Override
        public void migrate(Address source, int sourceCurrentReplicaIndex, int sourceNewReplicaIndex, Address destination,
                            int destinationCurrentReplicaIndex, int destinationNewReplicaIndex) {

            StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();

            System.out.print("source = " + source + ", sourceCurrentReplicaIndex = " + sourceCurrentReplicaIndex
                    + ", sourceNewReplicaIndex = " + sourceNewReplicaIndex + ", destination = " + destination
                    + ", destinationCurrentReplicaIndex = " + destinationCurrentReplicaIndex + ", destinationNewReplicaIndex = "
                    + destinationNewReplicaIndex);

            if (stackTrace != null && stackTrace.length > 2) {
                System.out.print(", at " + stackTrace[2]);
            }
            System.out.println();

            delegate.migrate(source, sourceCurrentReplicaIndex, sourceNewReplicaIndex, destination,
                    destinationCurrentReplicaIndex, destinationNewReplicaIndex);
        }
    }
}
