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

package com.hazelcast.spi;

import com.hazelcast.nio.Address;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Collections per member the callIds of all live operations.
 *
 * The Trace is not thread-safe and is recycled.
 *
 * This data-structure can be optimized to generate less litter. Instead of using an ArrayList for Long objects,
 * use an array for primitive longs. Also the lists don't need to be recreated every time the
 * {@link LiveOperationsTracker#populate(LiveOperations)} method is called; they could be recycled. This would be easy
 * to do since the {@link LiveOperations} is not used by a single thread.
 */
public final class LiveOperations {

    private final Address localAddress;
    private final Map<Address, List<Long>> callIdsByMember = new HashMap<Address, List<Long>>();

    public LiveOperations(Address localAddress) {
        this.localAddress = checkNotNull(localAddress, "local address can't be null");
    }

    public void add(Address address, long callId) {
        if (callId == 0) {
            // it is an unregistered operation
            return;
        }

        if (address == null) {
            address = localAddress;
        }

        List<Long> callIds = callIdsByMember.get(address);
        if (callIds == null) {
            callIds = new ArrayList<Long>();
            callIdsByMember.put(address, callIds);
        }

        callIds.add(callId);
    }

    public Set<Address> addresses() {
        return callIdsByMember.keySet();
    }

    public long[] callIds(Address address) {
        List<Long> callIdList = callIdsByMember.get(address);
        if (callIdList == null) {
            throw new IllegalArgumentException("unknown address");
        }

        long[] array = new long[callIdList.size()];
        for (int k = 0; k < array.length; k++) {
            array[k] = callIdList.get(k);
        }

        return array;
    }

    public void clear() {
        callIdsByMember.clear();
    }

    /**
     * Makes sure that a list of counters is created for a member.
     *
     * This method exists to ensure that an operation-heartbeat is always sent, even if there are no running operations.
     *
     * @param address the address of the member.
     */
    public void initMember(Address address) {
        callIdsByMember.put(address, new LinkedList<Long>());
    }
}
