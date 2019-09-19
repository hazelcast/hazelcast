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

package com.hazelcast.cp.internal.datastructures.atomicref;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.IAtomicReference;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.datastructures.atomicref.proxy.RaftAtomicRefProxy;
import com.hazelcast.cp.internal.datastructures.spi.atomic.RaftAtomicValueService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.Map;
import java.util.Set;

/**
 * Contains Raft-based atomic reference instances, implements snapshotting,
 * and creates proxies
 */
public class RaftAtomicRefService extends RaftAtomicValueService<Data, RaftAtomicRef, RaftAtomicRefSnapshot> {

    /**
     * Name of the service
     */
    public static final String SERVICE_NAME = "hz:raft:atomicRefService";

    public RaftAtomicRefService(NodeEngine nodeEngine) {
        super(nodeEngine);
    }

    @Override
    protected RaftAtomicRefSnapshot newSnapshot(Map<String, Data> values, Set<String> destroyed) {
        return new RaftAtomicRefSnapshot(values, destroyed);
    }

    @Override
    protected RaftAtomicRef newAtomicValue(CPGroupId groupId, String name, Data val) {
        return new RaftAtomicRef(groupId, name, val);
    }

    @Override
    protected IAtomicReference newRaftAtomicProxy(NodeEngineImpl nodeEngine, RaftGroupId groupId, String proxyName,
            String objectNameForProxy) {
        return new RaftAtomicRefProxy(nodeEngine, groupId, proxyName, objectNameForProxy);
    }
}
