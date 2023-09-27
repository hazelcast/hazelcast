/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.cpmap.proxy;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPMap;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.RaftInvocationManager;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.datastructures.cpmap.CPMapService;
import com.hazelcast.cp.internal.datastructures.cpmap.operation.*;
import com.hazelcast.cp.internal.datastructures.cpmap.operation.SetOp;
import com.hazelcast.cp.internal.datastructures.spi.operation.DestroyRaftObjectOp;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;

/**
 * Server-side Raft-based proxy implementation of {@link CPMap}
 */
@SuppressWarnings("checkstyle:methodcount")
public class CPMapProxy<K, V> implements CPMap<K, V> {

    private final RaftInvocationManager invocationManager;
    private final RaftGroupId groupId;
    private final String proxyName;
    private final String objectName;

    public CPMapProxy(NodeEngine nodeEngine,
                      RaftGroupId groupId,
                      String proxyName,
                      String objectName) {
        RaftService service = nodeEngine.getService(RaftService.SERVICE_NAME);
        this.invocationManager = service.getInvocationManager();
        this.groupId = groupId;
        this.proxyName = proxyName;
        this.objectName = objectName;
    }

    @Override
    public V get(K key) {
        GetOp op = new GetOp(objectName, key);
        InternalCompletableFuture f = invocationManager.invoke(groupId, op);
        return (V) f.join();
    }

    @Override
    public void set(K key, V value) {
        SetOp op = new SetOp(objectName, key, value);
        InternalCompletableFuture f = invocationManager.invoke(groupId, op);
        f.join();
    }

    @Override
    public void remove(K key) {
        RemoveOp op = new RemoveOp(objectName, key);
        InternalCompletableFuture f = invocationManager.invoke(groupId, op);
        f.join();
    }

    @Override
    public void clear() {
        ClearOp op = new ClearOp(objectName);
        InternalCompletableFuture f = invocationManager.invoke(groupId, op);
        f.join();
    }

    @Override
    public String getPartitionKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        return proxyName;
    }

    @Override
    public String getServiceName() {
        return CPMapService.SERVICE_NAME;
    }

    @Override
    public void destroy() {
        DestroyRaftObjectOp raftOp = new DestroyRaftObjectOp(getServiceName(), objectName);
        invocationManager.invoke(groupId, raftOp).joinInternal();
    }

    public CPGroupId getGroupId() {
        return groupId;
    }
}
