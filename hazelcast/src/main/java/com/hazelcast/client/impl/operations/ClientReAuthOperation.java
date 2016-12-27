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
import com.hazelcast.client.impl.ClientEngineImpl;
import com.hazelcast.client.impl.client.ClientPrincipal;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.UrgentSystemOperation;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;

import java.io.IOException;
import java.util.Set;

public class ClientReAuthOperation
        extends AbstractClientOperation
        implements UrgentSystemOperation, AllowedDuringPassiveState {

    private String clientUuid;
    private boolean noResourcesExistForClient;

    public ClientReAuthOperation() {
    }

    public ClientReAuthOperation(String clientUuid) {
        this.clientUuid = clientUuid;
    }

    @Override
    public void run() throws Exception {
        String memberUuid = getCallerUuid();
        ClientEngineImpl engine = getService();
        Set<ClientEndpoint> endpoints = engine.getEndpointManager().getEndpoints(clientUuid);
        for (ClientEndpoint endpoint : endpoints) {
            ClientPrincipal principal = new ClientPrincipal(clientUuid, memberUuid);
            endpoint.authenticated(principal);
        }
        String previousMemberUuid = engine.addOwnershipMapping(clientUuid, memberUuid);
        noResourcesExistForClient = previousMemberUuid == null;
        if (!noResourcesExistForClient) {
            // This code handles this case:
            // During a previous authentication (AuthenticationMessageTask run), ClientReAuthOperation failed at some member
            // while the ClientReAuthOperation succeeded for this member. In this case, the owner member was changed at this
            // member in the previous authentication (which we normally do not desire). Check if the resources (such as listeners)
            // exist to handle this case.
            boolean resourceExist = false;
            for (ClientEndpoint endpoint : endpoints) {
                if (endpoint.resourcesExist()) {
                    resourceExist = true;
                    break;
                }
            }
            if (!resourceExist) {
                // no resource exist for this client at the member, hence we can assume that the cleanup is done or there was no
                // registered listeners at all.
                noResourcesExistForClient = true;
            }
        }
    }

    @Override
    public boolean returnsResponse() {
        return Boolean.TRUE;
    }

    @Override
    public Object getResponse() {
        return noResourcesExistForClient;
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

    @Override
    public int getId() {
        return ClientDataSerializerHook.RE_AUTH;
    }
}
