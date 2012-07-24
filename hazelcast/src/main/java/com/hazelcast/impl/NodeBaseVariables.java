/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.impl;

import com.hazelcast.impl.base.Call;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Packet;
import com.hazelcast.util.Counter;
import com.hazelcast.util.SimpleBoundedQueue;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class NodeBaseVariables {
    final List<MemberImpl> lsMembers = new ArrayList<MemberImpl>(10);

    // Counter for normal/data (non-lite) members.
    // Counter is not thread-safe!
    final Counter dataMemberCount = new Counter() ;

    final Map<Address, MemberImpl> mapMembers = new HashMap<Address, MemberImpl>(200);

    final ConcurrentMap<Long, Call> mapCalls = new ConcurrentHashMap<Long, Call>(500);

    final Queue<Packet> qServiceThreadPacketCache = new SimpleBoundedQueue<Packet>(1000);

    final AtomicLong localIdGen = new AtomicLong(0);

    final Address thisAddress;

    final MemberImpl thisMember;

    NodeBaseVariables(Address thisAddress, MemberImpl thisMember) {
        this.thisAddress = thisAddress;
        this.thisMember = thisMember;
    }

}
