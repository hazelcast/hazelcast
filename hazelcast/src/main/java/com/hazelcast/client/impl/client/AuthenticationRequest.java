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

package com.hazelcast.client.impl.client;

import com.hazelcast.client.AuthenticationException;
import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.ClientEndpointManager;
import com.hazelcast.client.impl.ClientEngineImpl;
import com.hazelcast.client.impl.operations.ClientReAuthOperation;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.security.UsernamePasswordCredentials;
import com.hazelcast.spi.impl.SerializableCollection;
import com.hazelcast.util.UuidUtil;

import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.security.Permission;
import java.util.Collection;
import java.util.Set;

/**
 * Client Authentication Request that holds credentials
 */
public final class AuthenticationRequest extends CallableClientRequest {

    private Credentials credentials;
    private ClientPrincipal principal;
    private boolean ownerConnection;

    public AuthenticationRequest() {
    }

    public AuthenticationRequest(Credentials credentials) {
        this.credentials = credentials;
    }

    public AuthenticationRequest(Credentials credentials, ClientPrincipal principal) {
        this.credentials = credentials;
        this.principal = principal;
    }

    @Override
    public Object call() throws Exception {
        authenticate();
        return handleAuthenticated();
    }

    /**
     * Authenticates the client. An exception is thrown in case of failure.
     */
    private void authenticate() {
        ClientEngineImpl clientEngine = getService();
        if (credentials == null) {
            logAuthenticationFailure("Could not retrieve Credentials object!");
        } else if (clientEngine.getSecurityContext() != null) {
            authenticate(clientEngine.getSecurityContext());
        } else if (credentials instanceof UsernamePasswordCredentials) {
            UsernamePasswordCredentials usernamePasswordCredentials = (UsernamePasswordCredentials) credentials;
            authenticate(usernamePasswordCredentials);
        } else {
            String msg = "Hazelcast security is disabled.\nUsernamePasswordCredentials or cluster "
                    + "group-name and group-password should be used for authentication!\n"
                    + "Current credentials type is: " + credentials.getClass().getName();
            logAuthenticationFailure(msg);
        }
    }

    private void logAuthenticationSuccess() {
        ClientEngineImpl clientEngine = getService();
        Connection connection = endpoint.getConnection();
        ILogger logger = clientEngine.getLogger(getClass());
        logger.info("Received auth from " + connection + ", successfully authenticated");
    }

    private void logAuthenticationFailure(Object error) {
        ClientEngineImpl clientEngine = getService();
        Connection connection = endpoint.getConnection();
        ILogger logger = clientEngine.getLogger(getClass());

        StringBuilder message = new StringBuilder("Received auth from ").append(connection).append(", authentication failed");
        if (error instanceof String) {
            message.append(", message: ").append(message);
        }

        if (error instanceof Throwable) {
            logger.severe(message.toString(), (Throwable) error);
        } else {
            logger.warning(message.toString());
        }
    }

    private void authenticate(UsernamePasswordCredentials credentials) {
        ClientEngineImpl clientEngine = getService();
        GroupConfig groupConfig = clientEngine.getConfig().getGroupConfig();
        String nodeGroupName = groupConfig.getName();
        String nodeGroupPassword = groupConfig.getPassword();

        boolean usernameMatch = nodeGroupName.equals(credentials.getUsername());
        if (!usernameMatch) {
            String msg = "Client want to join the wrong group. "
                    + "Client has groupname: '" + credentials.getUsername()
                    + "' but cluster has groupname: '" + nodeGroupName + "'.";
            logAuthenticationFailure(msg);
            throw new AuthenticationException(msg);
        }

        boolean passwordMatch = nodeGroupPassword.equals(credentials.getPassword());
        if (!passwordMatch) {
            String msg = "Client wants to join group '" + nodeGroupName + "', but is not using the correct password.";
            logAuthenticationFailure(msg);
            throw new AuthenticationException(msg);
        }

        logAuthenticationSuccess();
    }

    private void authenticate(SecurityContext securityContext) {
        Connection connection = endpoint.getConnection();
        credentials.setEndpoint(connection.getInetAddress().getHostAddress());
        try {
            LoginContext lc = securityContext.createClientLoginContext(credentials);
            lc.login();
            endpoint.setLoginContext(lc);
            logAuthenticationSuccess();
        } catch (LoginException e) {
            logAuthenticationFailure(e);
            throw new AuthenticationException("");
        }
    }

    private Object handleAuthenticated() {
        ClientEngineImpl clientEngine = getService();

        if (ownerConnection) {
            final String uuid = getUuid();
            principal = new ClientPrincipal(uuid, clientEngine.getLocalMember().getUuid());
            reAuthLocal();
            Collection<MemberImpl> members = clientEngine.getClusterService().getMemberList();
            for (MemberImpl member : members) {
                if (!member.localMember()) {
                    ClientReAuthOperation op = new ClientReAuthOperation(uuid);
                    op.setCallerUuid(clientEngine.getLocalMember().getUuid());
                    operationService.send(op, member.getAddress());
                }
            }
        }

        endpoint.authenticated(principal, credentials, ownerConnection);
        clientEngine.getEndpointManager().registerEndpoint(endpoint);
        clientEngine.bind(endpoint);
        return new SerializableCollection(serializationService.toData(clientEngine.getThisAddress())
                , serializationService.toData(principal));
    }

    private String getUuid() {
        if (principal != null) {
            return principal.getUuid();
        }
        return UuidUtil.createClientUuid(endpoint.getConnection().getEndPoint());
    }

    private void reAuthLocal() {
        final ClientEndpointManager endpointManager = clientEngine.getEndpointManager();
        final Set<ClientEndpoint> endpoints = endpointManager.getEndpoints(principal.getUuid());
        for (ClientEndpoint endpoint : endpoints) {
            endpoint.authenticated(principal);
        }
    }

    public String getServiceName() {
        return ClientEngineImpl.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return ClientPortableHook.ID;
    }

    @Override
    public int getClassId() {
        return ClientPortableHook.AUTH;
    }

    public void setOwnerConnection(boolean ownerConnection) {
        this.ownerConnection = ownerConnection;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        writer.writePortable("credentials", (Portable) credentials);
        if (principal != null) {
            writer.writePortable("principal", principal);
        } else {
            writer.writeNullPortable("principal", ClientPortableHook.ID, ClientPortableHook.PRINCIPAL);
        }
        writer.writeBoolean("firstConnection", ownerConnection);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        credentials = (Credentials) reader.readPortable("credentials");
        principal = reader.readPortable("principal");
        ownerConnection = reader.readBoolean("firstConnection");
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }
}
