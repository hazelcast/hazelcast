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

package com.hazelcast.client.cp.internal.datastructures.proxy;

import com.hazelcast.client.cp.internal.datastructures.atomiclong.RaftAtomicLongProxy;
import com.hazelcast.client.cp.internal.datastructures.atomicref.RaftAtomicRefProxy;
import com.hazelcast.client.cp.internal.datastructures.countdownlatch.RaftCountDownLatchProxy;
import com.hazelcast.client.cp.internal.datastructures.lock.RaftFencedLockProxy;
import com.hazelcast.client.cp.internal.datastructures.semaphore.RaftSessionAwareSemaphoreProxy;
import com.hazelcast.client.cp.internal.datastructures.semaphore.RaftSessionlessSemaphoreProxy;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CPGroupCreateCPGroupCodec;
import com.hazelcast.client.impl.protocol.codec.CPSemaphoreGetSemaphoreTypeCodec;
import com.hazelcast.client.impl.spi.ClientContext;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.cp.ISemaphore;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.datastructures.atomiclong.RaftAtomicLongService;
import com.hazelcast.cp.internal.datastructures.atomicref.RaftAtomicRefService;
import com.hazelcast.cp.internal.datastructures.countdownlatch.RaftCountDownLatchService;
import com.hazelcast.cp.internal.datastructures.lock.RaftLockService;
import com.hazelcast.cp.internal.datastructures.semaphore.RaftSemaphoreService;
import com.hazelcast.cp.lock.FencedLock;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.cp.internal.RaftService.getObjectNameForProxy;
import static com.hazelcast.cp.internal.RaftService.withoutDefaultGroupName;

/**
 * Creates client-side proxies of the CP data structures
 */
public class ClientRaftProxyFactory {

    private final HazelcastClientInstanceImpl client;
    private final ConcurrentMap<String, RaftFencedLockProxy> lockProxies
            = new ConcurrentHashMap<String, RaftFencedLockProxy>();
    private ClientContext context;

    public ClientRaftProxyFactory(HazelcastClientInstanceImpl client) {
        this.client = client;
    }

    public void init(ClientContext context) {
        this.context = context;
    }

    public <T extends DistributedObject> T createProxy(String serviceName, String proxyName) {
        proxyName = withoutDefaultGroupName(proxyName);
        String objectName = getObjectNameForProxy(proxyName);


        RaftGroupId groupId = getGroupId(proxyName, objectName);

        if (serviceName.equals(RaftAtomicLongService.SERVICE_NAME)) {
            return (T) new RaftAtomicLongProxy(context, groupId, proxyName, objectName);
        } else if (serviceName.equals(RaftAtomicRefService.SERVICE_NAME)) {
            return (T) new RaftAtomicRefProxy(context, groupId, proxyName, objectName);
        } else if (serviceName.equals(RaftCountDownLatchService.SERVICE_NAME)) {
            return (T) new RaftCountDownLatchProxy(context, groupId, proxyName, objectName);
        } else if (serviceName.equals(RaftLockService.SERVICE_NAME)) {
            return (T) createFencedLock(groupId, proxyName, objectName);
        } else if (serviceName.equals(RaftSemaphoreService.SERVICE_NAME)) {
            return (T) createSemaphore(groupId, proxyName, objectName);
        } else {
            throw new IllegalArgumentException();
        }
    }

    private FencedLock createFencedLock(RaftGroupId groupId, String proxyName, String objectName) {
        while (true) {
            RaftFencedLockProxy proxy = lockProxies.get(proxyName);
            if (proxy != null) {
                if (!proxy.getGroupId().equals(groupId)) {
                    lockProxies.remove(proxyName, proxy);
                } else {
                    return proxy;
                }
            }

            proxy = new RaftFencedLockProxy(context, groupId, proxyName, objectName);
            RaftFencedLockProxy existing = lockProxies.putIfAbsent(proxyName, proxy);
            if (existing == null) {
                return proxy;
            } else if (existing.getGroupId().equals(groupId)) {
                return existing;
            }

            groupId = getGroupId(proxyName, objectName);
        }
    }

    private ISemaphore createSemaphore(RaftGroupId groupId, String proxyName, String objectName) {
        ClientMessage request = CPSemaphoreGetSemaphoreTypeCodec.encodeRequest(proxyName);
        ClientMessage response = new ClientInvocation(client, request, objectName).invoke().join();
        boolean jdkCompatible = CPSemaphoreGetSemaphoreTypeCodec.decodeResponse(response).response;

        return jdkCompatible
                ? new RaftSessionlessSemaphoreProxy(context, groupId, proxyName, objectName)
                : new RaftSessionAwareSemaphoreProxy(context, groupId, proxyName, objectName);
    }

    private RaftGroupId getGroupId(String proxyName, String objectName) {
        ClientMessage request = CPGroupCreateCPGroupCodec.encodeRequest(proxyName);
        ClientMessage response = new ClientInvocation(client, request, objectName).invoke().join();
        return CPGroupCreateCPGroupCodec.decodeResponse(response).groupId;
    }

}
