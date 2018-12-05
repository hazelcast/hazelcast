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

package com.hazelcast.client.cp.internal.datastructures.semaphore;

import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CPGroupCreateCPGroupCodec;
import com.hazelcast.client.impl.protocol.codec.CPSemaphoreGetSemaphoreTypeCodec;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.ClientProxyFactory;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientProxyFactoryWithContext;
import com.hazelcast.cp.internal.RaftGroupId;

import static com.hazelcast.cp.internal.RaftService.getObjectNameForProxy;

/**
 * Creates client-side proxies for
 * Raft-based {@link com.hazelcast.core.ISemaphore}
 */
public class RaftSemaphoreProxyFactory extends ClientProxyFactoryWithContext implements ClientProxyFactory {

    private final HazelcastClientInstanceImpl client;

    public RaftSemaphoreProxyFactory(HazelcastClientInstanceImpl client) {
        this.client = client;
    }

    @Override
    public ClientProxy create(String proxyName, ClientContext context) {
        String objectName = getObjectNameForProxy(proxyName);
        ClientMessage request = CPGroupCreateCPGroupCodec.encodeRequest(proxyName);
        ClientMessage response = new ClientInvocation(client, request, objectName).invoke().join();
        RaftGroupId groupId = CPGroupCreateCPGroupCodec.decodeResponse(response).groupId;

        request = CPSemaphoreGetSemaphoreTypeCodec.encodeRequest(proxyName);
        response = new ClientInvocation(client, request, objectName).invoke().join();
        boolean jdkCompatible = CPSemaphoreGetSemaphoreTypeCodec.decodeResponse(response).response;

        return jdkCompatible
                ? new RaftSessionlessSemaphoreProxy(context, groupId, proxyName, objectName)
                : new RaftSessionAwareSemaphoreProxy(context, groupId, proxyName, objectName);
    }
}
