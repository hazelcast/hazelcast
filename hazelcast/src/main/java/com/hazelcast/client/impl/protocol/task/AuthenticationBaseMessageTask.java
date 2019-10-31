/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.impl.ClientTypes;
import com.hazelcast.client.impl.protocol.AuthenticationStatus;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.ConnectionType;
import com.hazelcast.cluster.Address;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.PasswordCredentials;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.security.UsernamePasswordCredentials;

import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.security.Permission;
import java.util.Set;
import java.util.UUID;

import static com.hazelcast.client.impl.protocol.AuthenticationStatus.AUTHENTICATED;
import static com.hazelcast.client.impl.protocol.AuthenticationStatus.CREDENTIALS_FAILED;
import static com.hazelcast.client.impl.protocol.AuthenticationStatus.NOT_ALLOWED_IN_CLUSTER;
import static com.hazelcast.client.impl.protocol.AuthenticationStatus.SERIALIZATION_VERSION_MISMATCH;

/**
 * Base authentication task
 */
public abstract class AuthenticationBaseMessageTask<P> extends AbstractMessageTask<P>
        implements BlockingMessageTask, UrgentMessageTask {

    protected transient UUID clientUuid;
    protected transient String clusterName;
    protected transient String clientName;
    protected transient Set<String> labels;
    protected transient Credentials credentials;
    protected transient UUID clusterId;
    protected transient int partitionCount;
    transient byte clientSerializationVersion;
    transient String clientVersion;

    AuthenticationBaseMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }


    @Override
    public int getPartitionId() {
        return -1;
    }

    @Override
    protected boolean requiresAuthentication() {
        return false;
    }

    @Override
    public void processMessage() {
        switch (authenticate()) {
            case SERIALIZATION_VERSION_MISMATCH:
                sendClientMessage(prepareSerializationVersionMismatchClientMessage());
                break;
            case NOT_ALLOWED_IN_CLUSTER:
                sendClientMessage(prepareNotAllowedInCluster());
                break;
            case CREDENTIALS_FAILED:
                sendClientMessage(prepareUnauthenticatedClientMessage());
                break;
            case AUTHENTICATED:
                if (logger.isFineEnabled()) {
                    logger.fine("Processing authentication with clientUuid " + clientUuid);
                }
                sendClientMessage(prepareAuthenticatedClientMessage());
                break;
            default:
                throw new IllegalStateException("Unhandled authentication result");
        }
    }

    @SuppressWarnings("checkstyle:returncount")
    private AuthenticationStatus authenticate() {
        if (endpoint.isAuthenticated()) {
            return AUTHENTICATED;
        } else if (clientSerializationVersion != serializationService.getVersion()) {
            return SERIALIZATION_VERSION_MISMATCH;
        } else if (credentials == null) {
            logger.severe("Could not retrieve Credentials object!");
            return CREDENTIALS_FAILED;
        } else if (partitionCount != -1 && clientEngine.getPartitionService().getPartitionCount() != partitionCount) {
            logger.warning("Received auth from " + connection + " with clientUuid " + clientUuid
                    + ",  authentication rejected because client has a different partition count. "
                    + "Partition count client expects :" + partitionCount
                    + ", Member partition count:" + clientEngine.getPartitionService().getPartitionCount());
            return NOT_ALLOWED_IN_CLUSTER;
        } else if (clusterId != null && !clientEngine.getClusterService().getClusterId().equals(clusterId)) {
            logger.warning("Received auth from " + connection + " with clientUuid " + clientUuid
                    + ",  authentication rejected because client has a different cluster id. "
                    + "Cluster Id client expects :" + clusterId
                    + ", Member partition count:" + clientEngine.getClusterService().getClusterId());
            return NOT_ALLOWED_IN_CLUSTER;
        } else if (clientEngine.getSecurityContext() != null) {
            // security is enabled, let's do full JAAS authentication
            return authenticate(clientEngine.getSecurityContext());
        } else if (credentials instanceof UsernamePasswordCredentials) {
            // security is disabled let's verify that the username and password are null and the cluster names match
            return verifyEmptyCredentialsAndClusterName((PasswordCredentials) credentials);
        } else {
            logger.severe("Hazelcast security is disabled.\n"
                    + "Null username and password values are expected.\n"
                    + "Only the cluster name is verified in this case!\n"
                    + "Current credentials type is: " + credentials.getClass().getName());
            return CREDENTIALS_FAILED;
        }
    }

    private AuthenticationStatus authenticate(SecurityContext securityContext) {
        Connection connection = endpoint.getConnection();
        try {
            LoginContext lc = securityContext.createClientLoginContext(clusterName, credentials, connection);
            lc.login();
            endpoint.setLoginContext(lc);
            return AUTHENTICATED;
        } catch (LoginException e) {
            logger.warning(e);
            return CREDENTIALS_FAILED;
        }
    }

    private AuthenticationStatus verifyEmptyCredentialsAndClusterName(PasswordCredentials passwordCredentials) {
        if (passwordCredentials.getName() != null || passwordCredentials.getPassword() != null) {
            logger.warning("Received auth from " + connection + " with clientUuid " + clientUuid
                    + ",  authentication rejected because security is disabled on the member, "
                    + "and client sends not-null username or password.");
            return CREDENTIALS_FAILED;
        }
        String nodeClusterName = nodeEngine.getConfig().getClusterName();
        boolean clusternameMatch = nodeClusterName.equals(clusterName);
        return clusternameMatch ? AUTHENTICATED : CREDENTIALS_FAILED;
    }

    private ClientMessage prepareUnauthenticatedClientMessage() {
        Connection connection = endpoint.getConnection();
        logger.warning("Received auth from " + connection + " with clientUuid " + clientUuid + ", authentication failed");
        byte status = CREDENTIALS_FAILED.getId();
        return encodeAuth(status, null, null, serializationService.getVersion(),
                clientEngine.getPartitionService().getPartitionCount(), clientEngine.getClusterService().getClusterId());
    }

    private ClientMessage prepareNotAllowedInCluster() {
        byte status = NOT_ALLOWED_IN_CLUSTER.getId();
        return encodeAuth(status, null, null, serializationService.getVersion(),
                clientEngine.getPartitionService().getPartitionCount(), clientEngine.getClusterService().getClusterId());
    }

    private ClientMessage prepareSerializationVersionMismatchClientMessage() {
        return encodeAuth(SERIALIZATION_VERSION_MISMATCH.getId(), null, null,
                serializationService.getVersion(),
                clientEngine.getPartitionService().getPartitionCount(), clientEngine.getClusterService().getClusterId());
    }

    private ClientMessage prepareAuthenticatedClientMessage() {
        Connection connection = endpoint.getConnection();

        endpoint.authenticated(clientUuid, credentials, clientVersion, clientMessage.getCorrelationId(),
                clientName, labels);
        setConnectionType();
        if (!clientEngine.bind(endpoint)) {
            return prepareNotAllowedInCluster();
        }

        logger.info("Received auth from " + connection + ", successfully authenticated" + ", clientUuid: " + clientUuid
                + ", client version: " + clientVersion);
        final Address thisAddress = clientEngine.getThisAddress();
        byte status = AUTHENTICATED.getId();
        return encodeAuth(status, thisAddress, clientUuid,
                serializationService.getVersion(),
                clientEngine.getPartitionService().getPartitionCount(), clientEngine.getClusterService().getClusterId());
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
        } else if (ClientTypes.GO.equals(type)) {
            connection.setType(ConnectionType.GO_CLIENT);
        } else {
            logger.info("Unknown client type: " + type);
            connection.setType(ConnectionType.BINARY_CLIENT);
        }
    }

    protected abstract ClientMessage encodeAuth(byte status, Address thisAddress, UUID uuid,
                                                byte serializationVersion,
                                                int partitionCount, UUID clusterId);

    protected abstract String getClientType();

    @Override
    public Permission getRequiredPermission() {
        return null;
    }
}
