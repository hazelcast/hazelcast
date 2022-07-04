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

package com.hazelcast.client.cp.internal.datastructures.proxy;

import com.hazelcast.client.cp.internal.datastructures.atomiclong.AtomicLongProxy;
import com.hazelcast.client.cp.internal.datastructures.atomicref.AtomicRefProxy;
import com.hazelcast.client.cp.internal.datastructures.countdownlatch.CountDownLatchProxy;
import com.hazelcast.client.cp.internal.datastructures.lock.FencedLockProxy;
import com.hazelcast.client.cp.internal.datastructures.semaphore.SessionAwareSemaphoreProxy;
import com.hazelcast.client.cp.internal.datastructures.semaphore.SessionlessSemaphoreProxy;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CPGroupCreateCPGroupCodec;
import com.hazelcast.client.impl.protocol.codec.SemaphoreGetSemaphoreTypeCodec;
import com.hazelcast.client.impl.spi.ClientContext;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.cp.ISemaphore;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.datastructures.atomiclong.AtomicLongService;
import com.hazelcast.cp.internal.datastructures.atomicref.AtomicRefService;
import com.hazelcast.cp.internal.datastructures.countdownlatch.CountDownLatchService;
import com.hazelcast.cp.internal.datastructures.lock.LockService;
import com.hazelcast.cp.internal.datastructures.semaphore.SemaphoreService;
import com.hazelcast.cp.lock.FencedLock;

import javax.annotation.Nonnull;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.cp.internal.RaftService.getObjectNameForProxy;
import static com.hazelcast.cp.internal.RaftService.withoutDefaultGroupName;

/**
 * Creates client-side proxies of the CP data structures
 */
public class ClientRaftProxyFactory {

    private final HazelcastClientInstanceImpl client;
    private final ConcurrentMap<String, FencedLockProxy> lockProxies
            = new ConcurrentHashMap<String, FencedLockProxy>();
    private ClientContext context;

    public ClientRaftProxyFactory(HazelcastClientInstanceImpl client) {
        this.client = client;
    }

    public void init(ClientContext context) {
        this.context = context;
    }

    public @Nonnull
    <T extends DistributedObject> T createProxy(String serviceName, String proxyName) {
        proxyName = withoutDefaultGroupName(proxyName);
        String objectName = getObjectNameForProxy(proxyName);


        RaftGroupId groupId = getGroupId(proxyName, objectName);

        if (serviceName.equals(AtomicLongService.SERVICE_NAME)) {
            return (T) new AtomicLongProxy(context, groupId, proxyName, objectName);
        } else if (serviceName.equals(AtomicRefService.SERVICE_NAME)) {
            return (T) new AtomicRefProxy(context, groupId, proxyName, objectName);
        } else if (serviceName.equals(CountDownLatchService.SERVICE_NAME)) {
            return (T) new CountDownLatchProxy(context, groupId, proxyName, objectName);
        } else if (serviceName.equals(LockService.SERVICE_NAME)) {
            return (T) createFencedLock(groupId, proxyName, objectName);
        } else if (serviceName.equals(SemaphoreService.SERVICE_NAME)) {
            return (T) createSemaphore(groupId, proxyName, objectName);
        } else {
            throw new IllegalArgumentException();
        }
    }

    private FencedLock createFencedLock(RaftGroupId groupId, String proxyName, String objectName) {
        while (true) {
            FencedLockProxy proxy = lockProxies.get(proxyName);
            if (proxy != null) {
                if (!proxy.getGroupId().equals(groupId)) {
                    lockProxies.remove(proxyName, proxy);
                } else {
                    return proxy;
                }
            }

            proxy = new FencedLockProxy(context, groupId, proxyName, objectName);
            FencedLockProxy existing = lockProxies.putIfAbsent(proxyName, proxy);
            if (existing == null) {
                return proxy;
            } else if (existing.getGroupId().equals(groupId)) {
                return existing;
            }

            groupId = getGroupId(proxyName, objectName);
        }
    }

    private ISemaphore createSemaphore(RaftGroupId groupId, String proxyName, String objectName) {
        ClientMessage request = SemaphoreGetSemaphoreTypeCodec.encodeRequest(proxyName);
        ClientMessage response = new ClientInvocation(client, request, objectName).invoke().join();
        boolean jdkCompatible = SemaphoreGetSemaphoreTypeCodec.decodeResponse(response);

        return jdkCompatible
                ? new SessionlessSemaphoreProxy(context, groupId, proxyName, objectName)
                : new SessionAwareSemaphoreProxy(context, groupId, proxyName, objectName);
    }

    private RaftGroupId getGroupId(String proxyName, String objectName) {
        ClientMessage request = CPGroupCreateCPGroupCodec.encodeRequest(proxyName);
        ClientMessage response = new ClientInvocation(client, request, objectName).invoke().joinInternal();
        return CPGroupCreateCPGroupCodec.decodeResponse(response);
    }

}
