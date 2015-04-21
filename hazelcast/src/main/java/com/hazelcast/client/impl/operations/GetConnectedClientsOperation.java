/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.impl.ClientEndpointImpl;
import com.hazelcast.client.impl.ClientEngineImpl;
import com.hazelcast.core.Client;
import com.hazelcast.core.ClientType;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.ReadonlyOperation;
import java.util.HashMap;

import java.util.Map;


public class GetConnectedClientsOperation extends AbstractOperation implements ReadonlyOperation {

    private Map<String, ClientType> clients;

    public GetConnectedClientsOperation() {
    }

    @Override
    public void run() throws Exception {
        ClientEngineImpl service = getService();
        BuildInfo buildInfo = BuildInfoProvider.getBuildInfo();
        this.clients = new HashMap<String, ClientType>();
        for (Client clientEndpoint : service.getClients()) {
            ClientEndpointImpl clientEndpointImpl = (ClientEndpointImpl) clientEndpoint;
            if (buildInfo.isEnterprise() && clientEndpointImpl.getClientType() == ClientType.JAVA) {
                this.clients.put(clientEndpointImpl.getUuid(), ClientType.JAVAENTERPRISE);
            } else {
                this.clients.put(clientEndpointImpl.getUuid(), clientEndpointImpl.getClientType());
            }
        }
    }

    @Override
    public Object getResponse() {
        return clients;
    }
}
