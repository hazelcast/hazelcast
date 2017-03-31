/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.task;

import com.hazelcast.client.ClientTypes;
import com.hazelcast.client.impl.ClientEndpointImpl;
import com.hazelcast.client.impl.ReAuthenticationOperationSupplier;
import com.hazelcast.client.impl.client.ClientPrincipal;
import com.hazelcast.client.impl.protocol.AuthenticationStatus;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionType;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.security.UsernamePasswordCredentials;
import com.hazelcast.spi.Operation;
import com.hazelcast.util.UuidUtil;
import com.hazelcast.util.function.Supplier;

import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.security.Permission;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static java.util.Collections.synchronizedList;

/**
 * Base authentication task
 */
public abstract class AuthenticationBaseMessageTask<P> extends AbstractMultiTargetMessageTask<P> {


    protected transient ClientPrincipal principal;
    protected transient Credentials credentials;
    protected transient byte clientSerializationVersion;
    protected transient String clientVersion;
    private final List<Member> cleanedUpMembers = synchronizedList(new ArrayList<Member>());

    public AuthenticationBaseMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Supplier<Operation> createOperationSupplier() {
        return new ReAuthenticationOperationSupplier(getUuid(), clientMessage.getCorrelationId());
    }

    @Override
    protected Object reduce(Map<Member, Object> map) throws Throwable {
        for (Map.Entry<Member, Object> entry : map.entrySet()) {
            Member member = entry.getKey();
            Object response = entry.getValue();
            if (response instanceof Throwable) {
                if (response instanceof MemberLeftException) {
                    cleanedUpMembers.add(member);
                    continue;
                }
                throw (Throwable) response;
            }
            boolean isClientDisconnectOperationRun = (Boolean) response;
            if (isClientDisconnectOperationRun) {
                cleanedUpMembers.add(member);
            }
        }
        return prepareAuthenticatedClientMessage();
    }

    @Override
    public Collection<Member> getTargets() {
        return clientEngine.getClusterService().getMembers();
    }

    @Override
    protected ClientEndpointImpl getEndpoint() {
        return new ClientEndpointImpl(clientEngine, connection);
    }

    @Override
    protected boolean isAuthenticationMessage() {
        return true;
    }

    @Override
    public void processMessage() throws Throwable {
        byte serializationServiceVersion = serializationService.getVersion();
        AuthenticationStatus authenticationStatus;
        if (clientSerializationVersion != serializationServiceVersion) {
            sendClientMessage(prepareSerializationVersionMismatchClientMessage());
            return;
        }

        authenticationStatus = authenticate();
        if (authenticationStatus == AuthenticationStatus.CREDENTIALS_FAILED) {
            sendClientMessage(prepareUnauthenticatedClientMessage());
            return;
        }

        if (!isOwnerConnection()) {
            prepareAndSendResponse(authenticationStatus);
            return;
        }
        String uuid = getUuid();
        String localMemberUUID = clientEngine.getThisUuid();
        principal = new ClientPrincipal(uuid, localMemberUUID);
        super.processMessage();
    }

    private void prepareAndSendResponse(AuthenticationStatus authenticationStatus) {
        boolean isNotMember = clientEngine.getClusterService().getMember(principal.getOwnerUuid()) == null;
        if (isNotMember) {
            logger.warning("Member having uuid " + principal.getOwnerUuid()
                    + " is not part of the cluster. Client Authentication rejected.");
            authenticationStatus = AuthenticationStatus.CREDENTIALS_FAILED;
        }

        switch (authenticationStatus) {
            case AUTHENTICATED:
                sendClientMessage(prepareAuthenticatedClientMessage());
                break;
            case CREDENTIALS_FAILED:
                sendClientMessage(prepareUnauthenticatedClientMessage());
                break;
            case SERIALIZATION_VERSION_MISMATCH:
                sendClientMessage(prepareSerializationVersionMismatchClientMessage());
                break;
            default:
                sendClientMessage(new IllegalStateException("Unsupported authentication status: " + authenticationStatus));
        }
    }

    private AuthenticationStatus authenticate() {
        ILogger logger = clientEngine.getLogger(getClass());
        AuthenticationStatus status;
        if (credentials == null) {
            status = AuthenticationStatus.CREDENTIALS_FAILED;
            logger.severe("Could not retrieve Credentials object!");
        } else if (clientEngine.getSecurityContext() != null) {
            status = authenticate(clientEngine.getSecurityContext());
        } else if (credentials instanceof UsernamePasswordCredentials) {
            UsernamePasswordCredentials usernamePasswordCredentials = (UsernamePasswordCredentials) credentials;
            status = authenticate(usernamePasswordCredentials);
        } else {
            status = AuthenticationStatus.CREDENTIALS_FAILED;
            logger.severe("Hazelcast security is disabled.\nUsernamePasswordCredentials or cluster "
                    + "group-name and group-password should be used for authentication!\n" + "Current credentials type is: "
                    + credentials.getClass().getName());
        }
        return status;
    }

    private AuthenticationStatus authenticate(SecurityContext securityContext) {
        Connection connection = endpoint.getConnection();
        credentials.setEndpoint(connection.getInetAddress().getHostAddress());
        try {
            LoginContext lc = securityContext.createClientLoginContext(credentials);
            lc.login();
            endpoint.setLoginContext(lc);
            return AuthenticationStatus.AUTHENTICATED;
        } catch (LoginException e) {
            logger.warning(e);
            return AuthenticationStatus.CREDENTIALS_FAILED;
        }
    }

    private AuthenticationStatus authenticate(UsernamePasswordCredentials credentials) {
        GroupConfig groupConfig = nodeEngine.getConfig().getGroupConfig();
        String nodeGroupName = groupConfig.getName();
        String nodeGroupPassword = groupConfig.getPassword();
        boolean usernameMatch = nodeGroupName.equals(credentials.getUsername());
        boolean passwordMatch = nodeGroupPassword.equals(credentials.getPassword());
        return usernameMatch && passwordMatch ? AuthenticationStatus.AUTHENTICATED : AuthenticationStatus.CREDENTIALS_FAILED;
    }

    private ClientMessage prepareUnauthenticatedClientMessage() {
        Connection connection = endpoint.getConnection();
        ILogger logger = clientEngine.getLogger(getClass());
        logger.warning("Received auth from " + connection + " with principal " + principal + ", authentication failed");
        byte status = AuthenticationStatus.CREDENTIALS_FAILED.getId();
        return encodeAuth(status, null, null, null, serializationService.getVersion(), null);
    }

    private ClientMessage prepareSerializationVersionMismatchClientMessage() {
        return encodeAuth(AuthenticationStatus.SERIALIZATION_VERSION_MISMATCH.getId(), null, null, null,
                serializationService.getVersion(), null);
    }

    private ClientMessage prepareAuthenticatedClientMessage() {
        Connection connection = endpoint.getConnection();
        ILogger logger = clientEngine.getLogger(getClass());

        endpoint.authenticated(principal, credentials, isOwnerConnection(), clientVersion, clientMessage.getCorrelationId());
        setConnectionType();
        logger.info("Received auth from " + connection + ", successfully authenticated" + ", principal: " + principal
                + ", owner connection: " + isOwnerConnection() + ", client version: " + clientVersion);
        if (endpointManager.registerEndpoint(endpoint)) {
            clientEngine.bind(endpoint);
        }

        final Address thisAddress = clientEngine.getThisAddress();
        byte status = AuthenticationStatus.AUTHENTICATED.getId();
        return encodeAuth(status, thisAddress, principal.getUuid(), principal.getOwnerUuid(),
                serializationService.getVersion(), cleanedUpMembers);
    }

    private void setConnectionType() {
        String type = getClientType();
        if (ClientTypes.JAVA.equals(type)) {
            connection.setType(ConnectionType.JAVA_CLIENT);
        } else if (ClientTypes.CSHARP.equals(type)) {
            connection.setType(ConnectionType.CSHARP_CLIENT);
        } else if (ClientTypes.CPP.equals(type)) {
            connection.setType(ConnectionType.CPP_CLIENT);
        } else if (ClientTypes.PYTHON.equals(type)) {
            connection.setType(ConnectionType.PYTHON_CLIENT);
        } else if (ClientTypes.RUBY.equals(type)) {
            connection.setType(ConnectionType.RUBY_CLIENT);
        } else if (ClientTypes.NODEJS.equals(type)) {
            connection.setType(ConnectionType.NODEJS_CLIENT);
        } else {
            clientEngine.getLogger(getClass()).info("Unknown client type: " + type);
            connection.setType(ConnectionType.BINARY_CLIENT);
        }
    }

    protected abstract ClientMessage encodeAuth(byte status, Address thisAddress, String uuid, String ownerUuid,
                                                byte serializationVersion, List<Member> cleanedUpMembers);

    protected abstract boolean isOwnerConnection();

    protected abstract String getClientType();

    private String getUuid() {
        if (principal != null) {
            return principal.getUuid();
        }
        return UuidUtil.createClientUuid(endpoint.getConnection().getEndPoint());
    }


    @Override
    public Permission getRequiredPermission() {
        return null;
    }
}
