/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.ClientEndpointManager;
import com.hazelcast.client.impl.ClientEngineImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.ClientAwareService;
import com.hazelcast.spi.UrgentSystemOperation;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;

public class ClientDisconnectionOperation extends AbstractOperation implements UrgentSystemOperation {

    private String clientUuid;

    public ClientDisconnectionOperation() {
    }

    public ClientDisconnectionOperation(String clientUuid) {
        this.clientUuid = clientUuid;
    }

    @Override
    public void run() throws Exception {
        ClientEngineImpl engine = getService();
        final ClientEndpointManager endpointManager = engine.getEndpointManager();
        Set<ClientEndpoint> endpoints = endpointManager.getEndpoints(clientUuid);
        for (ClientEndpoint endpoint : endpoints) {
            endpointManager.removeEndpoint(endpoint, true);
        }
        engine.removeOwnershipMapping(clientUuid);

        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        nodeEngine.onClientDisconnected(clientUuid);
        Collection<ClientAwareService> services = nodeEngine.getServices(ClientAwareService.class);
        for (ClientAwareService service : services) {
            service.clientDisconnected(clientUuid);
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
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        clientUuid = in.readUTF();
    }
}
