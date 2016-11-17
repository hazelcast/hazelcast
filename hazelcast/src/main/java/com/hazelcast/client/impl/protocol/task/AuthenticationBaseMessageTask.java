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

package com.hazelcast.client.impl.protocol.task;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.ClientTypes;
import com.hazelcast.client.impl.ClientEndpointImpl;
import com.hazelcast.client.impl.ClientEngineImpl;
import com.hazelcast.client.impl.client.ClientPrincipal;
import com.hazelcast.client.impl.operations.ClientReAuthOperation;
import com.hazelcast.client.impl.protocol.AuthenticationStatus;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionType;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.security.UsernamePasswordCredentials;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.util.FutureUtil;
import com.hazelcast.util.UuidUtil;

import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.security.Permission;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

/**
 * Base authentication task
 */
public abstract class AuthenticationBaseMessageTask<P> extends AbstractCallableMessageTask<P> {
    private static final int REAUTHENTICATION_TIMEOUT = ClientEngineImpl.ENDPOINT_REMOVE_DELAY_SECONDS;

    protected transient ClientPrincipal principal;
    protected transient Credentials credentials;
    protected transient byte clientSerializationVersion;
    protected transient String clientVersion;

    public AuthenticationBaseMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    protected void setEndpoint() {
        if (connection.isAlive()) {
            checkExistingEndpoint();
            if (null == endpoint) {
                endpoint = new ClientEndpointImpl(clientEngine, this.connection);
            }
        } else {
            handleEndpointNotCreatedConnectionNotAlive();
        }
    }

    private void checkExistingEndpoint() {
        if (null != principal) {
            endpoint = endpointManager.getEndpoint(principal.getUuid());
            if (null != endpoint) {
                Connection previousConnection = endpoint.getConnection();
                if (null != previousConnection && !connection.equals(previousConnection)) {
                    previousConnection.close("A new authentication request from the same client with uuid " + clientUuid
                            + " is received. Closing the existing connection for this endpoint.", null);
                }
                endpoint.setConnection(connection);
            }
        }
    }

    @Override
    protected ClientEndpoint getEndpoint() {
        if (connection.isAlive()) {
            return endpoint;
        } else {
            handleEndpointNotCreatedConnectionNotAlive();
            return null;
        }
    }

    @Override
    protected boolean isAuthenticationMessage() {
        return true;
    }

    private void handleEndpointNotCreatedConnectionNotAlive() {
        logger.warning("Dropped: " + clientMessage + " -> endpoint not created for AuthenticationRequest, connection not alive");
    }

    @Override
    public Object call() {
        byte serializationServiceVersion = serializationService.getVersion();
        AuthenticationStatus authenticationStatus;
        if (clientSerializationVersion != serializationServiceVersion) {
            authenticationStatus = AuthenticationStatus.SERIALIZATION_VERSION_MISMATCH;
        } else {
            authenticationStatus = authenticate();
        }
        switch (authenticationStatus) {
            case AUTHENTICATED:
                return handleAuthenticated();
            case CREDENTIALS_FAILED:
                return handleUnauthenticated();
            case SERIALIZATION_VERSION_MISMATCH:
                return handleSerializationVersionMismatch();
            default:
                throw new IllegalStateException("Unsupported authentication status :" + authenticationStatus);
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

    private Object handleUnauthenticated() {
        Connection connection = endpoint.getConnection();
        ILogger logger = clientEngine.getLogger(getClass());
        logger.log(Level.WARNING,
                "Received auth from " + connection + " with principal " + principal + " , authentication failed");
        byte status = AuthenticationStatus.CREDENTIALS_FAILED.getId();
        return encodeAuth(status, null, null, null, serializationService.getVersion(), null);
    }

    private Object handleSerializationVersionMismatch() {
        return encodeAuth(AuthenticationStatus.SERIALIZATION_VERSION_MISMATCH.getId(), null, null, null,
                serializationService.getVersion(), null);
    }

    private ClientMessage handleAuthenticated() {
        List<Member> clientUnregisteredMembers = new ArrayList<Member>();
        if (isOwnerConnection()) {
            final String uuid = getUuid();
            final String localMemberUUID = clientEngine.getThisUuid();

            principal = new ClientPrincipal(uuid, localMemberUUID);

            boolean success = reAuthenticateWithMembers(clientUnregisteredMembers, uuid);

            if (!success) {
                byte status = AuthenticationStatus.CREDENTIALS_FAILED.getId();
                return encodeAuth(status, null, null, null, serializationService.getVersion(), null);
            }
        }

        boolean isNotMember = clientEngine.getClusterService().getMember(principal.getOwnerUuid()) == null;
        if (isNotMember) {
            byte status = AuthenticationStatus.CREDENTIALS_FAILED.getId();
            return encodeAuth(status, null, null, null, serializationService.getVersion(), null);
        }

        Connection connection = endpoint.getConnection();
        ILogger logger = clientEngine.getLogger(getClass());

        endpoint.authenticated(principal, credentials, isOwnerConnection(), clientVersion);
        setConnectionType();
        logger.log(Level.INFO, "Received auth from " + connection + ", successfully authenticated" + ", principal : " + principal
                + ", owner connection : " + isOwnerConnection() + ", client version : " + clientVersion);
        endpointManager.registerEndpoint(endpoint);
        clientEngine.bind(endpoint);

        final Address thisAddress = clientEngine.getThisAddress();
        byte status = AuthenticationStatus.AUTHENTICATED.getId();
        return encodeAuth(status, thisAddress, principal.getUuid(), principal.getOwnerUuid(), serializationService.getVersion(),
                clientUnregisteredMembers);
    }

    private boolean reAuthenticateWithMembers(final List<Member> cleanedUpMembers, final String uuid) {
        boolean success = true;

        ArrayList<OperationInfo> operationInfos = new ArrayList<OperationInfo>();
        MemberImpl localMember = clientEngine.getLocalMember();
        for (Member member : nodeEngine.getClusterService().getMembers()) {
            if (!member.localMember()) {
                ClientReAuthOperation op = new ClientReAuthOperation(uuid);
                op.setCallerUuid(localMember.getUuid());
                try {
                    InvocationBuilder invocationBuilder = nodeEngine.getOperationService()
                                                                    .createInvocationBuilder(null, op, member.getAddress());
                    invocationBuilder.setTryCount(1);
                    operationInfos.add(new OperationInfo(invocationBuilder.invoke(), member));
                } catch (Exception e) {
                    logger.warning(
                            "Failed to invoke ClientReAuthOperation of client " + uuid + " at member " + member);
                    success = false;
                    break;
                }
            }
        }

        if (success) {
            if (reAuthLocal()) {
                cleanedUpMembers.add(localMember);
            }

            if (getReAuthenticationResponses(cleanedUpMembers, operationInfos)) {
                return false;
            }
        }

        return true;
    }

    private boolean getReAuthenticationResponses(List<Member> cleanedUpMembers, List<OperationInfo> operationInfos) {
        List<Future> futures = new ArrayList<Future>();
        for (OperationInfo operationInfo : operationInfos) {
            futures.add(operationInfo.getFuture());
        }
        try {
            FutureUtil.waitWithDeadline(futures, REAUTHENTICATION_TIMEOUT, TimeUnit.SECONDS,
                    FutureUtil.RETHROW_ALL_EXCEPT_MEMBER_LEFT);
        } catch (Exception e) {
            logger.warning("Cluster reAuthentication failed.", e);
            return true;
        }

        for (OperationInfo operationInfo : operationInfos) {
            try {
                boolean clientDisconnectOperationRun = (Boolean) operationInfo.getFuture().get();
                if (clientDisconnectOperationRun) {
                    cleanedUpMembers.add(operationInfo.getMember());
                }
            } catch (MemberLeftException e) {
                // this is ok as expected
                cleanedUpMembers.add(operationInfo.getMember());
            } catch (Exception e) {
                logger.warning("Failed to get response for invocation to member:" + operationInfo.getMember(), e);
                return true;
            }
        }
        return false;
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

    private boolean reAuthLocal() {
        final Set<ClientEndpoint> endpoints = endpointManager.getEndpoints(principal.getUuid());
        for (ClientEndpoint endpoint : endpoints) {
            endpoint.authenticated(principal);
        }
        String previousOwnerUuid = clientEngine.addOwnershipMapping(principal.getUuid(), principal.getOwnerUuid());
        return previousOwnerUuid == null;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    private class OperationInfo {
        private final Member member;
        private final Future future;

        public OperationInfo(Future future, Member member) {
            this.future = future;
            this.member = member;
        }

        public Member getMember() {
            return member;
        }

        public Future getFuture() {
            return future;
        }
    }
}
