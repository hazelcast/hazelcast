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

package com.hazelcast.client.impl.operations;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.impl.ClientDataSerializerHook;
import com.hazelcast.client.impl.ClientEndpointManagerImpl;
import com.hazelcast.client.impl.ClientEngineImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.ClientAwareService;
import com.hazelcast.spi.UrgentSystemOperation;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;

public class ClientDisconnectionOperation extends AbstractClientOperation implements UrgentSystemOperation {

    private String clientUuid;
    private String memberUuid;

    public ClientDisconnectionOperation() {
    }

    public ClientDisconnectionOperation(String clientUuid, String memberUuid) {
        this.clientUuid = clientUuid;
        this.memberUuid = memberUuid;
    }

    @Override
    public void run() throws Exception {
        ClientEngineImpl engine = getService();
        final ClientEndpointManagerImpl endpointManager = (ClientEndpointManagerImpl) engine.getEndpointManager();
        if (engine.removeOwnershipMapping(clientUuid, memberUuid)) {
            Set<ClientEndpoint> endpoints = endpointManager.getEndpoints(clientUuid);
            for (ClientEndpoint endpoint : endpoints) {
                endpointManager
                        .removeEndpoint(endpoint, true, "ClientDisconnectionOperation: Cleanup of disconnected client resources");
            }

            NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
            nodeEngine.onClientDisconnected(clientUuid);
            Collection<ClientAwareService> services = nodeEngine.getServices(ClientAwareService.class);
            for (ClientAwareService service : services) {
                service.clientDisconnected(clientUuid);
            }
        }
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(clientUuid);
        out.writeUTF(memberUuid);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        clientUuid = in.readUTF();
        memberUuid = in.readUTF();
    }

    @Override
    public int getId() {
        return ClientDataSerializerHook.CLIENT_DISCONNECT;
    }
}
