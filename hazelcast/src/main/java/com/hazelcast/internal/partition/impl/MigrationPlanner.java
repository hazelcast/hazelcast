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
 * TODO: Javadoc Pending...
 */
class MigrationPlanner {

    private static final boolean ASSERTION_ENABLED = MigrationPlanner.class.desiredAssertionStatus();
    private static final boolean TRACE = false;


    interface MigrationDecisionCallback {
        void migrate(Address source, int sourceCurrentReplicaIndex, int sourceNewReplicaIndex,
                Address destination, int destinationCurrentReplicaIndex, int destinationNewReplicaIndex);
    }

    private final Set<Address> verificationSet = new HashSet<Address>();

    void planMigrations(Address[] oldAddresses, Address[] newAddresses, MigrationDecisionCallback callback) {
        assert oldAddresses.length == newAddresses.length
                : "Replica addresses with different lengths! Old: " + Arrays.toString(oldAddresses)
                            + ", New: " + Arrays.toString(newAddresses);

        if (TRACE) {
            callback = new TracingMigrationDecisionCallback(callback);
        }

        Address[] state = new Address[oldAddresses.length];
        System.arraycopy(oldAddresses, 0, state, 0, oldAddresses.length);

        verifyState(oldAddresses, newAddresses, state);
        fixCycle(oldAddresses, newAddresses);

        int currentIndex = 0;
        while (currentIndex < oldAddresses.length) {

            verifyState(oldAddresses, newAddresses, state);

            if (newAddresses[currentIndex] == null) {
                if (state[currentIndex] != null) {
                    callback.migrate(state[currentIndex], currentIndex, -1, null, -1, -1);
                    state[currentIndex] = null;
                }
                currentIndex++;
                continue;
            }

            if (state[currentIndex] == null) {
                int i = getReplicaIndex(state, newAddresses[currentIndex]);
                if (i == -1) {
                    callback.migrate(null, -1, -1, newAddresses[currentIndex], -1, currentIndex);
                    state[currentIndex] = newAddresses[currentIndex];
                    currentIndex++;
                    continue;
                }

                if (i > currentIndex) {
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
                currentIndex++;
                continue;
            }

            if (getReplicaIndex(newAddresses, state[currentIndex]) == -1
                    && getReplicaIndex(state, newAddresses[currentIndex]) == -1) {
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
                            "Migration decision algorithm failed during SHIFT DOWN! INITIAL: "
                                    + Arrays.toString(oldAddresses) + ", CURRENT: " + Arrays.toString(state)
                                    + ", FINAL: " + Arrays.toString(newAddresses));
                }

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
                    throw new AssertionError("Migration algorithm failed during SHIFT UP! " + target
                            + " is not present in " + Arrays.toString(state) + "."
                            + " INITIAL: " + Arrays.toString(oldAddresses) + ", FINAL: " + Arrays.toString(newAddresses));

                } else if (newAddresses[j] == null) {
                    callback.migrate(state[i], i, -1, state[j], j, i);
                    state[i] = state[j];
                    state[j] = null;
                    break;
                } else if (getReplicaIndex(state, newAddresses[j]) == -1) {
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
            throw new AssertionError("Migration decisions failed! INITIAL: "
                    + Arrays.toString(oldAddresses) + " CURRENT: " + Arrays.toString(state)
                    + ", FINAL: " + Arrays.toString(newAddresses));
        }
    }

    private void verifyState(Address[] oldAddresses, Address[] newAddresses, Address[] state) {
        if (!ASSERTION_ENABLED) {
            return;
        }

        try {
            for (Address address : state) {
                if (address == null) {
                    continue;
                }
                assert verificationSet.add(address)
                        : "Migration decision algorithm failed! DUPLICATE REPLICA ADDRESSES! INITIAL: "
                            + Arrays.toString(oldAddresses) + ", CURRENT: " + Arrays.toString(state)
                            + ", FINAL: " + Arrays.toString(newAddresses);
            }
        } finally {
            verificationSet.clear();
        }
    }

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

    private static final class TracingMigrationDecisionCallback implements MigrationDecisionCallback {

        final MigrationDecisionCallback delegate;

        private TracingMigrationDecisionCallback(MigrationDecisionCallback delegate) {
            this.delegate = delegate;
        }

        @Override
        public void migrate(Address source, int sourceCurrentReplicaIndex, int sourceNewReplicaIndex,
                Address destination, int destinationCurrentReplicaIndex, int destinationNewReplicaIndex) {

            StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();

            System.out.print("source = " + source
                    + ", sourceCurrentReplicaIndex = " + sourceCurrentReplicaIndex
                    + ", sourceNewReplicaIndex = " + sourceNewReplicaIndex
                    + ", destination = " + destination
                    + ", destinationCurrentReplicaIndex = " + destinationCurrentReplicaIndex
                    + ", destinationNewReplicaIndex = " + destinationNewReplicaIndex);

            if (stackTrace != null && stackTrace.length > 2) {
                System.out.print(", at " + stackTrace[2]);
            }
            System.out.println();

            delegate.migrate(source, sourceCurrentReplicaIndex, sourceNewReplicaIndex,
                        destination, destinationCurrentReplicaIndex, destinationNewReplicaIndex);
        }
    }
}
