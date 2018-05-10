/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.impl.ClientDataSerializerHook;
import com.hazelcast.client.impl.ClientEndpoint;
import com.hazelcast.client.impl.ClientEngineImpl;
import com.hazelcast.client.impl.StubAuthenticationException;
import com.hazelcast.client.impl.client.ClientPrincipal;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.ExceptionAction;
import com.hazelcast.spi.UrgentSystemOperation;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;

import java.io.IOException;
import java.util.Set;

import static com.hazelcast.spi.ExceptionAction.THROW_EXCEPTION;

public class ClientReAuthOperation
        extends AbstractClientOperation
        implements UrgentSystemOperation, AllowedDuringPassiveState {

    private String clientUuid;
    private long authCorrelationId;

    public ClientReAuthOperation() {
    }

    public ClientReAuthOperation(String clientUuid, long authCorrelationId) {
        this.clientUuid = clientUuid;
        this.authCorrelationId = authCorrelationId;
    }

    @Override
    public void run() throws Exception {
        ClientEngineImpl engine = getService();
        //Runs on {@link com.hazelcast.spi.ExecutionService.CLIENT_MANAGEMENT_EXECUTOR}
        // to work in sync with ClientDisconnectionOperation
        engine.getClientManagementExecutor().execute(new ClientReauthTask());
    }

    private boolean doRun() throws Exception {
        ILogger logger = getLogger();
        ClientEngineImpl engine = getService();
        String memberUuid = getCallerUuid();
        if (!engine.trySetLastAuthenticationCorrelationId(clientUuid, authCorrelationId)) {
            String message = "Server already processed a newer authentication from client with UUID " + clientUuid
                    + ". Not applying requested ownership change to " + memberUuid;
            logger.info(message);
            throw new StubAuthenticationException(message);
        }
        Set<ClientEndpoint> endpoints = engine.getEndpointManager().getEndpoints(clientUuid);
        for (ClientEndpoint endpoint : endpoints) {
            ClientPrincipal principal = new ClientPrincipal(clientUuid, memberUuid);
            endpoint.authenticated(principal);
        }
        String previousMemberUuid = engine.addOwnershipMapping(clientUuid, memberUuid);
        if (logger.isFineEnabled()) {
            logger.fine("Client authenticated " + clientUuid + ", owner " + memberUuid);
        }
        return previousMemberUuid == null;
    }

    @Override
    public boolean returnsResponse() {
        // This method actually returns a response.
        // Since operation needs to work on a different executor,
        // (see {@link com.hazelcast.spi.ExecutionService.CLIENT_MANAGEMENT_EXECUTOR})
        // the response is returned via ClientReAuthOperation.ClientReauthTask
        return false;
    }

    @Override
    public void logError(Throwable e) {
        if (!(e instanceof StubAuthenticationException)) {
            super.logError(e);
        }
    }

    @Override
    public ExceptionAction onInvocationException(Throwable throwable) {
        if (throwable instanceof MemberLeftException || throwable instanceof TargetNotMemberException) {
            return THROW_EXCEPTION;
        }
        return super.onInvocationException(throwable);
    }

    @Override
    public String getServiceName() {
        return ClientEngineImpl.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(clientUuid);
        out.writeLong(authCorrelationId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        clientUuid = in.readUTF();
        authCorrelationId = in.readLong();
    }

    @Override
    public int getId() {
        return ClientDataSerializerHook.RE_AUTH;
    }

    public class ClientReauthTask implements Runnable {
        @Override
        public void run() {
            try {
                sendResponse(doRun());
            } catch (Exception e) {
                sendResponse(e);
            }
        }
    }

}
