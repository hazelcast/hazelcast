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

package com.hazelcast.internal.crdt.pncounter;

import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.impl.VectorClock;
import com.hazelcast.cluster.memberselector.MemberSelectors;
import com.hazelcast.core.ConsistencyLostException;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.crdt.pncounter.PNCounter;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.crdt.pncounter.operations.AddOperation;
import com.hazelcast.internal.crdt.pncounter.operations.CRDTTimestampedLong;
import com.hazelcast.internal.crdt.pncounter.operations.GetOperation;
import com.hazelcast.internal.util.ThreadLocalRandomProvider;
import com.hazelcast.logging.ILogger;
import com.hazelcast.cluster.Address;
import com.hazelcast.partition.NoDataMemberInClusterException;
import com.hazelcast.spi.impl.AbstractDistributedObject;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.InvocationBuilder;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static com.hazelcast.internal.crdt.pncounter.PNCounterService.SERVICE_NAME;

/**
 * Member proxy implementation for a {@link PNCounter}.
 */
public class PNCounterProxy extends AbstractDistributedObject<PNCounterService> implements PNCounter {
    /**
     * Atomic field updater for the observed clock field
     */
    private static final AtomicReferenceFieldUpdater<PNCounterProxy, VectorClock> OBSERVED_TIMESTAMPS_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(PNCounterProxy.class, VectorClock.class, "observedClock");
    private static final List<Address> EMPTY_ADDRESS_LIST = Collections.emptyList();
    /** The counter name */
    private final String name;
    private final ILogger logger;
    private volatile Address currentTargetReplicaAddress;
    private final Object targetSelectionMutex = new Object();
    /** Retry count for PN counter operations, if <0 then default try count is used */
    private int operationTryCount = -1;

    /**
     * The last vector clock observed by this proxy. It is used for maintaining
     * session consistency guarantees when reading from different replicas.
     */
    private volatile VectorClock observedClock;

    PNCounterProxy(String name, NodeEngine nodeEngine, PNCounterService service) {
        super(nodeEngine, service);
        this.name = name;
        this.logger = nodeEngine.getLogger(PNCounterProxy.class);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getServiceName() {
        return SERVICE_NAME;
    }

    @Override
    public long get() {
        return invoke(new GetOperation(name, observedClock));
    }

    @Override
    public long getAndAdd(long delta) {
        return invoke(new AddOperation(name, delta, true, observedClock));
    }

    @Override
    public long addAndGet(long delta) {
        return invoke(new AddOperation(name, delta, false, observedClock));
    }

    @Override
    public long getAndSubtract(long delta) {
        return invoke(new AddOperation(name, -delta, true, observedClock));
    }

    @Override
    public long subtractAndGet(long delta) {
        return invoke(new AddOperation(name, -delta, false, observedClock));
    }

    @Override
    public long decrementAndGet() {
        return invoke(new AddOperation(name, -1, false, observedClock));
    }

    @Override
    public long incrementAndGet() {
        return invoke(new AddOperation(name, 1, false, observedClock));
    }

    @Override
    public long getAndDecrement() {
        return invoke(new AddOperation(name, -1, true, observedClock));
    }

    @Override
    public long getAndIncrement() {
        return invoke(new AddOperation(name, 1, true, observedClock));
    }

    @Override
    public void reset() {
        this.observedClock = null;
    }

    /**
     * Invokes the {@code operation}, blocks and returns the result of the
     * invocation.
     * If the member on which this method is invoked is a lite member, the
     * operation will be invoked on a cluster member. The cluster member is
     * chosen randomly once the first operation is about to be invoked and if
     * we detect that the previously chosen member is no longer a data member
     * of the cluster.
     * This method may throw a {@link ConsistencyLostException}
     * if there are no reachable replicas which have observed the updates
     * previously observed by users of this proxy. This means that the session
     * guarantees are lost.
     *
     * @param operation the operation to invoke
     * @return the result of the invocation
     * @throws NoDataMemberInClusterException if the cluster does not contain any data members
     * @throws UnsupportedOperationException  if the cluster version is less than 3.10
     * @throws ConsistencyLostException       if the session guarantees have been lost
     * @see ClusterService#getClusterVersion()
     */
    private long invoke(Operation operation) {
        return invokeInternal(operation, EMPTY_ADDRESS_LIST, null);
    }

    /**
     * Invokes the {@code operation} recursively on viable replica addresses
     * until successful or the list of viable replicas is exhausted.
     * Replicas with addresses contained in the {@code excludedAddresses} are
     * skipped. If there are no viable replicas, this method will throw the
     * {@code lastException} if not {@code null} or a
     * {@link NoDataMemberInClusterException} if the {@code lastException} is
     * {@code null}.
     *
     * @param operation         the operation to invoke on a CRDT replica
     * @param excludedAddresses the addresses to exclude when choosing a replica
     *                          address, must not be {@code null}
     * @param lastException     the exception thrown from the last invocation of
     *                          the {@code operation} on a replica, may be {@code null}
     * @return the result of the operation invocation on a replica
     * @throws NoDataMemberInClusterException if there are no replicas and the
     *                                        {@code lastException} is {@code null}
     */
    private long invokeInternal(Operation operation, List<Address> excludedAddresses,
                                HazelcastException lastException) {
        final Address target = getCRDTOperationTarget(excludedAddresses);
        if (target == null) {
            throw lastException != null
                    ? lastException
                    : new NoDataMemberInClusterException(
                    "Cannot invoke operations on a CRDT because the cluster does not contain any data members");
        }
        try {
            final InvocationBuilder builder = getNodeEngine().getOperationService()
                                                             .createInvocationBuilder(SERVICE_NAME, operation, target);
            if (operationTryCount > 0) {
                builder.setTryCount(operationTryCount);
            }
            final InvocationFuture<CRDTTimestampedLong> future = builder.invoke();
            final CRDTTimestampedLong result = future.joinInternal();
            updateObservedReplicaTimestamps(result.getVectorClock());
            return result.getValue();
        } catch (HazelcastException e) {
            logger.fine("Exception occurred while invoking operation on target " + target + ", choosing different target", e);
            if (excludedAddresses == EMPTY_ADDRESS_LIST) {
                excludedAddresses = new ArrayList<Address>();
            }
            excludedAddresses.add(target);
            return invokeInternal(operation, excludedAddresses, e);
        }
    }

    /**
     * Updates the locally observed CRDT vector clock atomically. This method
     * is thread safe and can be called concurrently. The method will only
     * update the clock if the {@code receivedLogicalTimestamps} is higher than
     * the currently observed vector clock.
     *
     * @param receivedVectorClock vector clock received from a replica state read
     */
    private void updateObservedReplicaTimestamps(VectorClock receivedVectorClock) {
        for (; ; ) {
            final VectorClock currentClock = this.observedClock;
            if (currentClock != null && currentClock.isAfter(receivedVectorClock)) {
                break;
            }
            if (OBSERVED_TIMESTAMPS_UPDATER.compareAndSet(this, currentClock, receivedVectorClock)) {
                break;
            }
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
     * @param excludedAddresses the addresses to exclude from returning from
     *                          this method
     * @return a CRDT replica address or {@code null} if there are no viable
     * addresses
     */
    private Address getCRDTOperationTarget(List<Address> excludedAddresses) {
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
     * contained in the {@code excludedAddresses} list are excluded.
     * The method may return {@code null} if there are no viable addresses.
     * If the local address is a possible replica, it returns the local address,
     * otherwise it chooses a replica randomly.
     *
     * @param excludedAddresses the addresses to exclude when choosing a replica address
     * @return a CRDT replica address or {@code null} if there are no viable addresses
     */
    private Address chooseTargetReplica(List<Address> excludedAddresses) {
        final List<Address> replicaAddresses = getReplicaAddresses(excludedAddresses);
        if (replicaAddresses.isEmpty()) {
            return null;
        }

        final Address localAddress = getNodeEngine().getLocalMember().getAddress();
        if (replicaAddresses.contains(localAddress)) {
            return localAddress;
        }
        final int randomReplicaIndex = ThreadLocalRandomProvider.get().nextInt(replicaAddresses.size());
        return replicaAddresses.get(randomReplicaIndex);
    }

    /**
     * Returns the addresses of the CRDT replicas from the current state of the
     * local membership list. Addresses contained in the {@code excludedAddresses}
     * collection are excluded.
     *
     * @param excludedAddresses the addresses to exclude
     * @return list of possible CRDT replica addresses
     */
    private List<Address> getReplicaAddresses(Collection<Address> excludedAddresses) {
        final Collection<Member> dataMembers = getNodeEngine().getClusterService()
                                                              .getMembers(MemberSelectors.DATA_MEMBER_SELECTOR);
        final int maxConfiguredReplicaCount = getNodeEngine().getConfig().findPNCounterConfig(name).getReplicaCount();
        final int currentReplicaCount = Math.min(maxConfiguredReplicaCount, dataMembers.size());
        final ArrayList<Address> replicaAddresses = new ArrayList<Address>(currentReplicaCount);
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
     * Returns the current target replica address to which this proxy is
     * sending invocations.
     */
    // public for testing purposes
    public Address getCurrentTargetReplicaAddress() {
        return currentTargetReplicaAddress;
    }

    /**
     * Sets the operation retry count for PN counter operations.
     */
    // public for testing purposes
    public void setOperationTryCount(int operationTryCount) {
        this.operationTryCount = operationTryCount;
    }

    @Override
    public String toString() {
        return "PNCounter{name='" + name + "\'}";
    }
}
