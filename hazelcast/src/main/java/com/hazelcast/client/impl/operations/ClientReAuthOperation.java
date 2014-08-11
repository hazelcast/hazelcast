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
import com.hazelcast.client.client.ClientPrincipal;
import com.hazelcast.client.impl.ClientEngineImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.UrgentSystemOperation;

import java.io.IOException;
import java.util.Set;

public class ClientReAuthOperation extends AbstractOperation implements UrgentSystemOperation {

    private String clientUuid;

    public ClientReAuthOperation() {
    }

    public ClientReAuthOperation(String clientUuid) {
        this.clientUuid = clientUuid;
    }

    @Override
    public void run() throws Exception {
        ClientEngineImpl service = getService();
        String memberUuid = getCallerUuid();
        ClientEngineImpl engine = getService();
        Set<ClientEndpoint> endpoints = engine.getEndpointManager().getEndpoints(clientUuid);
        for (ClientEndpoint endpoint : endpoints) {
            ClientPrincipal principal = new ClientPrincipal(clientUuid, memberUuid);
            endpoint.authenticated(principal);
        }
        service.addOwnershipMapping(clientUuid, memberUuid);
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public Object getResponse() {
        return Boolean.TRUE;
    }

    @Override
    public String getServiceName() {
        return ClientEngineImpl.SERVICE_NAME;
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
