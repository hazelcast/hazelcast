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

package com.hazelcast.client;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.ClientAwareService;
import com.hazelcast.spi.UrgentSystemOperation;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;

/**
 * @author mdogan 2/11/13
 */
public class ClientDisconnectionOperation extends AbstractOperation implements UrgentSystemOperation {

    private String clientUuid;

    public ClientDisconnectionOperation() {
    }

    public ClientDisconnectionOperation(String clientUuid) {
        this.clientUuid = clientUuid;
    }

    public void run() throws Exception {
        ClientEngineImpl engine = getService();
        final Set<ClientEndpoint> endpoints = engine.getEndpoints(clientUuid);
        for (ClientEndpoint endpoint : endpoints) {
            engine.removeEndpoint(endpoint.getConnection(), true);
        }

        final NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        nodeEngine.onClientDisconnected(clientUuid);
        final Collection<ClientAwareService> services = nodeEngine.getServices(ClientAwareService.class);
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
