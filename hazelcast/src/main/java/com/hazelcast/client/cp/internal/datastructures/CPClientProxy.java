/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.cp.internal.datastructures;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.spi.ClientContext;
import com.hazelcast.client.impl.spi.ClientProxy;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.impl.spi.impl.ClientInvocationFuture;
import com.hazelcast.cp.internal.RaftGroupId;

import java.util.UUID;

/**
 * CP data structure client proxy that makes use of direct-to-leader operation sending where applicable.
 */
public abstract class CPClientProxy extends ClientProxy {
    protected final RaftGroupId groupId;
    // [objectName] is without the '@groupName' suffix
    protected final String objectName;

    protected CPClientProxy(String serviceName, String name, ClientContext context,
                            RaftGroupId groupId, String objectName) {
        super(serviceName, name, context);
        this.groupId = groupId;
        this.objectName = objectName;
    }

    /**
     * CP Subsystem specific functionality to invoke a {@link ClientInvocation} with
     * a target {@link UUID} provided, if available. This UUID will be the last known
     * leader of this proxy's group if available, or else a normal invocation will occur.
     *
     * @param request    the {@link ClientMessage} request to invoke
     * @param objectName the name of the object in respect to a {@link ClientInvocation}
     * @return           the {@link ClientInvocationFuture} after the request is invoked
     */
    protected ClientInvocationFuture invokeClientRequest(ClientMessage request, String objectName) {
        ClientInvocation invocation;

        // provide target UUID of last known leader if available - if direct-to-leader routing is
        //  disabled then the service will always return `null`
        UUID lastKnownLeader = getClient().getCPGroupViewService().getLastKnownLeader(groupId);
        if (lastKnownLeader != null) {
            invocation = new ClientInvocation(getClient(), request, objectName, lastKnownLeader);
        } else {
            invocation = new ClientInvocation(getClient(), request, objectName);
        }
        return invocation.invoke();
    }
}
