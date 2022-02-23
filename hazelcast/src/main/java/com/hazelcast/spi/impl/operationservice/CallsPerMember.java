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

package com.hazelcast.spi.impl.operationservice;

import com.hazelcast.cluster.Address;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Implements {@link LiveOperations} and additionally collects the call IDs of operations on remote members
 * whose cancellation was requested.
 * <p>
 * This data-structure can be optimized to generate less litter. Instead of using an ArrayList for Long objects,
 * use an array for primitive longs. Also the lists don't need to be recreated every time the
 * {@link LiveOperationsTracker#populate(LiveOperations)} method is called; they could be recycled. This would be easy
 * to do since the {@link CallsPerMember} is used by a single thread.
 */
public final class CallsPerMember implements LiveOperations {
    private final Address localAddress;
    private final Map<Address, CategorizedCallIds> callIdsByMember = new HashMap<Address, CategorizedCallIds>();

    public CallsPerMember(Address localAddress) {
        this.localAddress = checkNotNull(localAddress, "local address can't be null");
    }

    @Override
    public void add(Address address, long callId) {
        if (callId == 0) {
            // it is an unregistered operation
            return;
        }
        if (address == null) {
            address = localAddress;
        }
        getOrCreateCallIdsForMember(address).liveOps.add(callId);
    }

    public void addOpToCancel(Address address, long callId) {
        getOrCreateCallIdsForMember(address).opsToCancel.add(callId);
    }

    public Set<Address> addresses() {
        return callIdsByMember.keySet();
    }

    public OperationControl toOpControl(Address address) {
        CategorizedCallIds callIds = callIdsByMember.get(address);
        if (callIds == null) {
            throw new IllegalArgumentException("Address not recognized as a member of this cluster: " + address);
        }
        return new OperationControl(toArray(callIds.liveOps), toArray(callIds.opsToCancel));
    }

    public void clear() {
        callIdsByMember.clear();
    }

    /**
     * Makes sure that a list of counters is created for a member.
     * <p>
     * This method exists to ensure that an operation-heartbeat is always sent, even if there are no running operations.
     *
     * @param address the address of the member.
     */
    public void ensureMember(Address address) {
        getOrCreateCallIdsForMember(address);
    }

    public CategorizedCallIds getOrCreateCallIdsForMember(Address address) {
        CategorizedCallIds callIds = callIdsByMember.get(address);
        if (callIds == null) {
            callIds = new CategorizedCallIds();
            callIdsByMember.put(address, callIds);
        }
        return callIds;
    }

    private static long[] toArray(List<Long> longs) {
        long[] array = new long[longs.size()];
        for (int k = 0; k < array.length; k++) {
            array[k] = longs.get(k);
        }
        return array;
    }

    private static final class CategorizedCallIds {
        final List<Long> liveOps = new ArrayList<Long>();
        final List<Long> opsToCancel = new ArrayList<Long>();
    }
}
