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
import com.hazelcast.cp.internal.datastructures.atomicref.proxy.AtomicRefProxy;
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
public class AtomicRefService extends RaftAtomicValueService<Data, AtomicRef, AtomicRefSnapshot> {

    /**
     * Name of the service
     */
    public static final String SERVICE_NAME = "hz:raft:atomicRefService";

    public AtomicRefService(NodeEngine nodeEngine) {
        super(nodeEngine);
    }

    @Override
    protected AtomicRefSnapshot newSnapshot(Map<String, Data> values, Set<String> destroyed) {
        return new AtomicRefSnapshot(values, destroyed);
    }

    @Override
    protected AtomicRef newAtomicValue(CPGroupId groupId, String name, Data val) {
        return new AtomicRef(groupId, name, val);
    }

    @Override
    protected IAtomicReference newRaftAtomicProxy(NodeEngineImpl nodeEngine, RaftGroupId groupId, String proxyName,
            String objectNameForProxy) {
        return new AtomicRefProxy(nodeEngine, groupId, proxyName, objectNameForProxy);
    }
}
