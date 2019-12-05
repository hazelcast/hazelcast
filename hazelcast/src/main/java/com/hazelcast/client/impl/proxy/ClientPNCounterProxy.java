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

package com.hazelcast.client.impl.proxy;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.PNCounterAddCodec;
import com.hazelcast.client.impl.protocol.codec.PNCounterGetCodec;
import com.hazelcast.client.impl.protocol.codec.PNCounterGetConfiguredReplicaCountCodec;
import com.hazelcast.client.impl.spi.ClientContext;
import com.hazelcast.client.impl.spi.ClientProxy;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.impl.VectorClock;
import com.hazelcast.cluster.memberselector.MemberSelectors;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.crdt.pncounter.PNCounter;
import com.hazelcast.internal.util.ThreadLocalRandomProvider;
import com.hazelcast.logging.ILogger;
import com.hazelcast.cluster.Address;
import com.hazelcast.partition.NoDataMemberInClusterException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * Client proxy implementation for a {@link PNCounter}.
 */
public class ClientPNCounterProxy extends ClientProxy implements PNCounter {

    /**
     * Atomic field updater for the observed clock field
     */
    private static final AtomicReferenceFieldUpdater<ClientPNCounterProxy, VectorClock> OBSERVED_TIMESTAMPS_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(ClientPNCounterProxy.class, VectorClock.class, "observedClock");
    private static final List<Address> EMPTY_ADDRESS_LIST = Collections.emptyList();
    private final ILogger logger;
    private volatile Address currentTargetReplicaAddress;
    private final Object targetSelectionMutex = new Object();
    private volatile int maxConfiguredReplicaCount;

    /**
     * The last vector clock observed by this proxy. It is used for maintaining
     * session consistency guarantees when reading from different replicas.
     */
    private volatile VectorClock observedClock;

    /**
     * Creates a client {@link PNCounter} proxy
     *
     * @param serviceName the service name
     * @param objectName  the PNCounter name
     * @param context     the client context containing references to services
     *                    and configuration
     */
    public ClientPNCounterProxy(String serviceName, String objectName, ClientContext context) {
        super(serviceName, objectName, context);
        this.logger = getContext().getLoggingService().getLogger(ClientPNCounterProxy.class);
        this.observedClock = new VectorClock();
    }

    @Override
    public String toString() {
        return "PNCounter{name='" + name + "\'}";
    }

    @Override
    public long get() {
        final Address target = getCRDTOperationTarget(EMPTY_ADDRESS_LIST);
        if (target == null) {
            throw new NoDataMemberInClusterException(
                    "Cannot invoke operations on a CRDT because the cluster does not contain any data members");
        }
        final ClientMessage response = invokeGetInternal(EMPTY_ADDRESS_LIST, null, target);
        final PNCounterGetCodec.ResponseParameters resultParameters = PNCounterGetCodec.decodeResponse(response);
        updateObservedReplicaTimestamps(resultParameters.replicaTimestamps);
        return resultParameters.value;
    }

    @Override
    public long getAndAdd(long delta) {
        final Address target = getCRDTOperationTarget(EMPTY_ADDRESS_LIST);
        if (target == null) {
            throw new NoDataMemberInClusterException(
                    "Cannot invoke operations on a CRDT because the cluster does not contain any data members");
        }
        final ClientMessage response = invokeAddInternal(delta, true, EMPTY_ADDRESS_LIST, null, target);
        final PNCounterAddCodec.ResponseParameters resultParameters = PNCounterAddCodec.decodeResponse(response);
        updateObservedReplicaTimestamps(resultParameters.replicaTimestamps);
        return resultParameters.value;
    }

    @Override
    public long addAndGet(long delta) {
        final Address target = getCRDTOperationTarget(EMPTY_ADDRESS_LIST);
        if (target == null) {
            throw new NoDataMemberInClusterException(
                    "Cannot invoke operations on a CRDT because the cluster does not contain any data members");
        }
        final ClientMessage response = invokeAddInternal(delta, false, EMPTY_ADDRESS_LIST, null, target);
        final PNCounterAddCodec.ResponseParameters resultParameters = PNCounterAddCodec.decodeResponse(response);
        updateObservedReplicaTimestamps(resultParameters.replicaTimestamps);
        return resultParameters.value;
    }

    @Override
    public long getAndSubtract(long delta) {
        final Address target = getCRDTOperationTarget(EMPTY_ADDRESS_LIST);
        if (target == null) {
            throw new NoDataMemberInClusterException(
                    "Cannot invoke operations on a CRDT because the cluster does not contain any data members");
        }
        final ClientMessage response = invokeAddInternal(-delta, true, EMPTY_ADDRESS_LIST, null, target);
        final PNCounterAddCodec.ResponseParameters resultParameters = PNCounterAddCodec.decodeResponse(response);
        updateObservedReplicaTimestamps(resultParameters.replicaTimestamps);
        return resultParameters.value;
    }

    @Override
    public long subtractAndGet(long delta) {
        final Address target = getCRDTOperationTarget(EMPTY_ADDRESS_LIST);
        if (target == null) {
            throw new NoDataMemberInClusterException(
                    "Cannot invoke operations on a CRDT because the cluster does not contain any data members");
        }
        final ClientMessage response = invokeAddInternal(-delta, false, EMPTY_ADDRESS_LIST, null, target);
        final PNCounterAddCodec.ResponseParameters resultParameters = PNCounterAddCodec.decodeResponse(response);
        updateObservedReplicaTimestamps(resultParameters.replicaTimestamps);
        return resultParameters.value;
    }

    @Override
    public long decrementAndGet() {
        final Address target = getCRDTOperationTarget(EMPTY_ADDRESS_LIST);
        if (target == null) {
            throw new NoDataMemberInClusterException(
                    "Cannot invoke operations on a CRDT because the cluster does not contain any data members");
        }
        final ClientMessage response = invokeAddInternal(-1, false, EMPTY_ADDRESS_LIST, null, target);
        final PNCounterAddCodec.ResponseParameters resultParameters = PNCounterAddCodec.decodeResponse(response);
        updateObservedReplicaTimestamps(resultParameters.replicaTimestamps);
        return resultParameters.value;
    }

    @Override
    public long incrementAndGet() {
        final Address target = getCRDTOperationTarget(EMPTY_ADDRESS_LIST);
        if (target == null) {
            throw new NoDataMemberInClusterException(
                    "Cannot invoke operations on a CRDT because the cluster does not contain any data members");
        }
        final ClientMessage response = invokeAddInternal(1, false, EMPTY_ADDRESS_LIST, null, target);
        final PNCounterAddCodec.ResponseParameters resultParameters = PNCounterAddCodec.decodeResponse(response);
        updateObservedReplicaTimestamps(resultParameters.replicaTimestamps);
        return resultParameters.value;
    }

    @Override
    public long getAndDecrement() {
        final Address target = getCRDTOperationTarget(EMPTY_ADDRESS_LIST);
        if (target == null) {
            throw new NoDataMemberInClusterException(
                    "Cannot invoke operations on a CRDT because the cluster does not contain any data members");
        }
        final ClientMessage response = invokeAddInternal(-1, true, EMPTY_ADDRESS_LIST, null, target);
        final PNCounterAddCodec.ResponseParameters resultParameters = PNCounterAddCodec.decodeResponse(response);
        updateObservedReplicaTimestamps(resultParameters.replicaTimestamps);
        return resultParameters.value;
    }

    @Override
    public long getAndIncrement() {
        final Address target = getCRDTOperationTarget(EMPTY_ADDRESS_LIST);
        if (target == null) {
            throw new NoDataMemberInClusterException(
                    "Cannot invoke operations on a CRDT because the cluster does not contain any data members");
        }
        final ClientMessage response = invokeAddInternal(1, true, EMPTY_ADDRESS_LIST, null, target);
        final PNCounterAddCodec.ResponseParameters resultParameters = PNCounterAddCodec.decodeResponse(response);
        updateObservedReplicaTimestamps(resultParameters.replicaTimestamps);
        return resultParameters.value;
    }

    @Override
    public void reset() {
        this.observedClock = new VectorClock();
    }


    /**
     * Transforms the list of replica logical timestamps to a vector clock instance.
     *
     * @param replicaLogicalTimestamps the logical timestamps
     * @return a vector clock instance
     */
    private VectorClock toVectorClock(List<Entry<UUID, Long>> replicaLogicalTimestamps) {
        final VectorClock timestamps = new VectorClock();
        for (Entry<UUID, Long> replicaTimestamp : replicaLogicalTimestamps) {
            timestamps.setReplicaTimestamp(replicaTimestamp.getKey(), replicaTimestamp.getValue());
        }
        return timestamps;
    }

    /**
     * Adds the {@code delta} and returns the value of the counter before the
     * update if {@code getBeforeUpdate} is {@code true} or the value after
     * the update if it is {@code false}.
     * It will invoke client messages recursively on viable replica addresses
     * until successful or the list of viable replicas is exhausted.
     * Replicas with addresses contained in the {@code excludedAddresses} are
     * skipped. If there are no viable replicas, this method will throw the
     * {@code lastException} if not {@code null} or a
     * {@link NoDataMemberInClusterException} if the {@code lastException} is
     * {@code null}.
     *
     * @param delta             the delta to add to the counter value, can be negative
     * @param getBeforeUpdate   {@code true} if the operation should return the
     *                          counter value before the addition, {@code false}
     *                          if it should return the value after the addition
     * @param excludedAddresses the addresses to exclude when choosing a replica
     *                          address, must not be {@code null}
     * @param lastException     the exception thrown from the last invocation of
     *                          the {@code request} on a replica, may be {@code null}
     * @return the result of the request invocation on a replica
     * @throws NoDataMemberInClusterException if there are no replicas and the
     *                                        {@code lastException} is {@code null}
     */
    private ClientMessage invokeAddInternal(long delta, boolean getBeforeUpdate,
                                            List<Address> excludedAddresses,
                                            HazelcastException lastException,
                                            Address target) {
        if (target == null) {
            throw lastException != null
                    ? lastException
                    : new NoDataMemberInClusterException(
                    "Cannot invoke operations on a CRDT because the cluster does not contain any data members");
        }
        try {
            final ClientMessage request = PNCounterAddCodec.encodeRequest(
                    name, delta, getBeforeUpdate, observedClock.entrySet(), target);
            return invokeOnAddress(request, target);
        } catch (HazelcastException e) {
            logger.fine("Unable to provide session guarantees when sending operations to " + target
                    + ", choosing different target");
            if (excludedAddresses == EMPTY_ADDRESS_LIST) {
                excludedAddresses = new ArrayList<>();
            }
            excludedAddresses.add(target);
            final Address newTarget = getCRDTOperationTarget(excludedAddresses);
            return invokeAddInternal(delta, getBeforeUpdate, excludedAddresses, e, newTarget);
        }
    }

    /**
     * Returns the current value of the counter.
     * It will invoke client messages recursively on viable replica addresses
     * until successful or the list of viable replicas is exhausted.
     * Replicas with addresses contained in the {@code excludedAddresses} are
     * skipped. If there are no viable replicas, this method will throw the
     * {@code lastException} if not {@code null} or a
     * {@link NoDataMemberInClusterException} if the {@code lastException} is
     * {@code null}.
     *
     * @param excludedAddresses the addresses to exclude when choosing a replica
     *                          address, must not be {@code null}
     * @param lastException     the exception thrown from the last invocation of
     *                          the {@code request} on a replica, may be {@code null}
     * @return the result of the request invocation on a replica
     * @throws NoDataMemberInClusterException if there are no replicas and the
     *                                        {@code lastException} is false
     */
    private ClientMessage invokeGetInternal(List<Address> excludedAddresses,
                                            HazelcastException lastException,
                                            Address target) {
        if (target == null) {
            throw lastException != null
                    ? lastException
                    : new NoDataMemberInClusterException(
                    "Cannot invoke operations on a CRDT because the cluster does not contain any data members");
        }
        try {
            final ClientMessage request = PNCounterGetCodec.encodeRequest(name, observedClock.entrySet(), target);
            return invokeOnAddress(request, target);
        } catch (HazelcastException e) {
            logger.fine("Exception occurred while invoking operation on target " + target + ", choosing different target", e);
            if (excludedAddresses == EMPTY_ADDRESS_LIST) {
                excludedAddresses = new ArrayList<>();
            }
            excludedAddresses.add(target);
            final Address newTarget = getCRDTOperationTarget(excludedAddresses);
            return invokeGetInternal(excludedAddresses, e, newTarget);
        }
    }

    /**
     * Returns the target on which this proxy should invoke a CRDT operation.
     * On first invocation of this method, the method will choose a target
     * address and return that address on future invocations. Replicas with
     * addresses contained in the {@code excludedAddresses} list are excluded
     * and if the chosen replica is in this list, a new replica is chosen and
     * returned on future invocations.
     * The method may return {@code null} if there are no viable target addresses.
     *
     * @param excludedAddresses the addresses to exclude when choosing a replica
     *                          address, must not be {@code null}
     * @return a CRDT replica address or {@code null} if there are no viable
     * addresses
     */
    private Address getCRDTOperationTarget(Collection<Address> excludedAddresses) {
        if (currentTargetReplicaAddress != null && !excludedAddresses.contains(currentTargetReplicaAddress)) {
            return currentTargetReplicaAddress;
        }

        synchronized (targetSelectionMutex) {
            if (currentTargetReplicaAddress == null || excludedAddresses.contains(currentTargetReplicaAddress)) {
                currentTargetReplicaAddress = chooseTargetReplica(excludedAddresses);
            }
        }
        return currentTargetReplicaAddress;
    }

    /**
     * Chooses and returns a CRDT replica address. Replicas with addresses
     * contained in the {@code excludedAddresses} list are excluded and the
     * method chooses randomly between the collection of viable target addresses.
     * <p>
     * The method may return {@code null} if there are no viable addresses.
     *
     * @param excludedAddresses the addresses to exclude when choosing a replica
     *                          address, must not be {@code null}
     * @return a CRDT replica address or {@code null} if there are no viable addresses
     */
    private Address chooseTargetReplica(Collection<Address> excludedAddresses) {
        final List<Address> replicaAddresses = getReplicaAddresses(excludedAddresses);
        if (replicaAddresses.isEmpty()) {
            return null;
        }
        final int randomReplicaIndex = ThreadLocalRandomProvider.get().nextInt(replicaAddresses.size());
        return replicaAddresses.get(randomReplicaIndex);
    }

    /**
     * Returns the addresses of the CRDT replicas from the current state of the
     * local membership list. Addresses contained in the {@code excludedAddresses}
     * collection are excluded.
     *
     * @param excludedAddresses the addresses to exclude when choosing a replica
     *                          address, must not be {@code null}
     * @return list of possible CRDT replica addresses
     */
    private List<Address> getReplicaAddresses(Collection<Address> excludedAddresses) {
        final Collection<Member> dataMembers = getContext().getClusterService()
                                                           .getMembers(MemberSelectors.DATA_MEMBER_SELECTOR);
        final int maxConfiguredReplicaCount = getMaxConfiguredReplicaCount();
        final int currentReplicaCount = Math.min(maxConfiguredReplicaCount, dataMembers.size());
        final ArrayList<Address> replicaAddresses = new ArrayList<>(currentReplicaCount);
        final Iterator<Member> dataMemberIterator = dataMembers.iterator();

        for (int i = 0; i < currentReplicaCount; i++) {
            final Address dataMemberAddress = dataMemberIterator.next().getAddress();
            if (!excludedAddresses.contains(dataMemberAddress)) {
                replicaAddresses.add(dataMemberAddress);
            }
        }
        return replicaAddresses;
    }

    /**
     * Returns the max configured replica count.
     * When invoked for the first time, this method will fetch the
     * configuration from a cluster member.
     *
     * @return the maximum configured replica count
     */
    private int getMaxConfiguredReplicaCount() {
        if (maxConfiguredReplicaCount > 0) {
            return maxConfiguredReplicaCount;
        } else {
            final ClientMessage request = PNCounterGetConfiguredReplicaCountCodec.encodeRequest(name);
            final ClientMessage response = invoke(request);
            final PNCounterGetConfiguredReplicaCountCodec.ResponseParameters resultParameters
                    = PNCounterGetConfiguredReplicaCountCodec.decodeResponse(response);
            maxConfiguredReplicaCount = resultParameters.response;
        }
        return maxConfiguredReplicaCount;
    }

    /**
     * Updates the locally observed CRDT vector clock atomically. This method
     * is thread safe and can be called concurrently. The method will only
     * update the clock if the {@code receivedLogicalTimestamps} is higher than
     * the currently observed vector clock.
     *
     * @param receivedLogicalTimestamps logical timestamps received from a replica state read
     */
    private void updateObservedReplicaTimestamps(List<Entry<UUID, Long>> receivedLogicalTimestamps) {
        final VectorClock received = toVectorClock(receivedLogicalTimestamps);
        for (; ; ) {
            final VectorClock currentClock = this.observedClock;
            if (currentClock.isAfter(received)) {
                break;
            }
            if (OBSERVED_TIMESTAMPS_UPDATER.compareAndSet(this, currentClock, received)) {
                break;
            }
        }
    }

    /**
     * Returns the current target replica address to which this proxy is
     * sending invocations.
     */
    // public for testing purposes
    public Address getCurrentTargetReplicaAddress() {
        return currentTargetReplicaAddress;
    }
}
