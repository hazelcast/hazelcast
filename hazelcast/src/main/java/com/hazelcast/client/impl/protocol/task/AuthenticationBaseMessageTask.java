/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.auditlog.AuditlogTypeIds;
import com.hazelcast.client.impl.ClientEngineImpl;
import com.hazelcast.client.impl.ClusterViewListenerService;
import com.hazelcast.client.impl.connection.tcp.RoutingMode;
import com.hazelcast.client.impl.ClusterViewListenerService.PartitionsView;
import com.hazelcast.client.impl.protocol.AuthenticationStatus;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.cluster.Address;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.cp.CPGroupsSnapshot;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.cluster.impl.MembersView;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.server.ServerConnection;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.PasswordCredentials;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.security.UsernamePasswordCredentials;
import com.hazelcast.version.Version;

import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.security.Permission;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.hazelcast.client.impl.protocol.AuthenticationStatus.AUTHENTICATED;
import static com.hazelcast.client.impl.protocol.AuthenticationStatus.CREDENTIALS_FAILED;
import static com.hazelcast.client.impl.protocol.AuthenticationStatus.NOT_ALLOWED_IN_CLUSTER;
import static com.hazelcast.client.impl.protocol.AuthenticationStatus.SERIALIZATION_VERSION_MISMATCH;
import static com.hazelcast.client.impl.connection.tcp.KeyValuePairGenerator.createKeyValuePairs;
import static java.util.Collections.emptyMap;

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
    transient byte clientSerializationVersion;
    transient String clientVersion;
    transient byte routingMode;
    transient boolean cpDirectToLeaderRouting;

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
    protected boolean acceptOnIncompleteStart() {
        return true;
    }

    @Override
    protected boolean validateNodeStartBeforeDecode() {
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
                    logger.fine("Processing authentication with clientUuid " + clientUuid
                            + " and clientName " + clientName);
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
            logger.warning("Could not retrieve Credentials object!");
            return CREDENTIALS_FAILED;
        } else if (clientEngine.getSecurityContext() != null) {
            // security is enabled, let's do full JAAS authentication
            return authenticate(clientEngine.getSecurityContext());
        } else if (credentials instanceof UsernamePasswordCredentials userNamePasswordCredentials) {
            // security is disabled let's verify that the username and password are null and the cluster names match
            return verifyEmptyCredentialsAndClusterName(userNamePasswordCredentials);
        } else {
            logger.warning("Hazelcast security is disabled.\n"
                    + "Null username and password values are expected.\n"
                    + "Only the cluster name is verified in this case!\n"
                    + "Current credentials type is: " + credentials.getClass().getName());
            return CREDENTIALS_FAILED;
        }
    }

    private AuthenticationStatus authenticate(SecurityContext securityContext) {
        if (!verifyClusterName()) {
            return CREDENTIALS_FAILED;
        }

        Connection connection = endpoint.getConnection();
        Boolean passed = Boolean.FALSE;
        try {
            LoginContext lc = securityContext.createClientLoginContext(clusterName, credentials, connection);
            lc.login();
            endpoint.setLoginContext(lc);
            passed = Boolean.TRUE;
            return AUTHENTICATED;
        } catch (LoginException e) {
            logger.warning(e);
            return CREDENTIALS_FAILED;
        } finally {
            nodeEngine.getNode().getNodeExtension().getAuditlogService()
                    .eventBuilder(AuditlogTypeIds.AUTHENTICATION_CLIENT)
                    .message("Client connection authentication.")
                    .addParameter("connection", connection)
                    .addParameter("credentials", credentials)
                    .addParameter("passed", passed)
                    .log();
        }
    }

    private AuthenticationStatus verifyEmptyCredentialsAndClusterName(PasswordCredentials passwordCredentials) {
        if (passwordCredentials.getName() != null || passwordCredentials.getPassword() != null) {
            logger.fine("Received auth from " + connection + " with clientUuid " + clientUuid
                    + " and clientName " + clientName + ", authentication rejected because security"
                    + " is disabled on the member, and client sends not-null username or password.");
            return CREDENTIALS_FAILED;
        }

        return verifyClusterName() ? AUTHENTICATED : CREDENTIALS_FAILED;
    }

    private boolean verifyClusterName() {
        boolean skipClusterNameCheck =
            nodeEngine.getProperties().getBoolean(ClientEngineImpl.SKIP_CLUSTER_NAME_CHECK_DURING_CONNECTION);

        return skipClusterNameCheck || nodeEngine.getConfig().getClusterName().equals(clusterName);
    }

    private ClientMessage prepareUnauthenticatedClientMessage() {
        boolean failoverSupported = nodeEngine.getNode().getNodeExtension().isClientFailoverSupported();
        Connection connection = endpoint.getConnection();
        logger.fine("Received auth from %s with clientUuid %s and clientName %s, authentication failed", connection, clientUuid,
                clientName);
        byte status = CREDENTIALS_FAILED.getId();

        return encodeUnauthenticated(status, failoverSupported);
    }

    private ClientMessage encodeUnauthenticated(byte status, boolean failoverSupported) {
        return encodeAuthenticationResponse(status, null, null, (byte) -1, "", -1, null,
                failoverSupported, null, null, null, null, emptyMap());
    }

    private ClientMessage prepareNotAllowedInCluster() {
        boolean failoverSupported = nodeEngine.getNode().getNodeExtension().isClientFailoverSupported();
        byte status = NOT_ALLOWED_IN_CLUSTER.getId();
        return encodeUnauthenticated(status, failoverSupported);
    }

    private ClientMessage prepareSerializationVersionMismatchClientMessage() {
        boolean failoverSupported = nodeEngine.getNode().getNodeExtension().isClientFailoverSupported();
        return encodeUnauthenticated(SERIALIZATION_VERSION_MISMATCH.getId(), failoverSupported);
    }

    private ClientMessage prepareAuthenticatedClientMessage() {
        ServerConnection connection = endpoint.getConnection();
        setConnectionType();
        setTpcTokenToEndpoint();
        endpoint.authenticated(clientUuid, credentials, clientVersion, clientMessage.getCorrelationId(), clientName, labels,
                RoutingMode.getById(routingMode), cpDirectToLeaderRouting);
        validateNodeStart();
        final UUID clusterId = nodeEngine.getClusterService().getClusterId();
        // additional check: cluster id may be null when member has not started yet;
        // see AbstractMessageTask#acceptOnIncompleteStart
        if (clusterId == null) {
            throw new HazelcastInstanceNotActiveException("Hazelcast instance is not ready yet!");
        }
        if (!clientEngine.bind(endpoint)) {
            return prepareNotAllowedInCluster();
        }

        logger.info("Received auth from " + connection + ", successfully authenticated, clientUuid: " + clientUuid
                + ", client name: " + clientName + ", client version: " + clientVersion);
        final Address thisAddress = clientEngine.getThisAddress();
        UUID uuid = nodeEngine.getClusterService().getLocalMember().getUuid();
        byte status = AUTHENTICATED.getId();
        boolean failoverSupported = nodeEngine.getNode().getNodeExtension().isClientFailoverSupported();
        String serverVersion = getMemberBuildInfo().getVersion();
        byte[] tpcToken = endpoint.getTpcToken() != null ? endpoint.getTpcToken().getContent() : null;

        ClusterViewListenerService clusterViewListenerService = clientEngine.getClusterViewListenerService();
        MembersView membersView = clusterViewListenerService.getMembersView();
        PartitionsView partitionsView = clusterViewListenerService.getPartitionsView();
        Collection<Collection<UUID>> memberGroups = clusterViewListenerService.toMemberGroups(membersView);

        CPGroupsSnapshot cpMemberSnapshot = CPGroupsSnapshot.EMPTY;
        boolean enterprise = nodeEngine.getNode().getBuildInfo().isEnterprise();
        if (enterprise && nodeEngine.getConfig().getCPSubsystemConfig().getGroupSize() > 0) {
            // The snapshot will be empty if ADVANCED_CP license component is not present
            cpMemberSnapshot = nodeEngine.getHazelcastInstance().getCPSubsystem()
                                         .getCPSubsystemManagementService().getCurrentGroupsSnapshot();
        }

        Version clusterVersion = nodeEngine.getClusterService().getClusterVersion();
        Map<String, String> keyValuePairs = createKeyValuePairs(
                memberGroups, membersView.getVersion(), enterprise, clusterVersion, cpMemberSnapshot);

        return encodeAuthenticationResponse(status, thisAddress, uuid, serializationService.getVersion(), serverVersion,
                nodeEngine.getPartitionService().getPartitionCount(), clusterId, failoverSupported,
                nodeEngine.getTpcServerBootstrap().getClientPorts(), tpcToken, membersView, partitionsView, keyValuePairs);
    }

    private void setConnectionType() {
        connection.setConnectionType(getClientType());
    }


    protected void setTpcTokenToEndpoint() {
    }

    @SuppressWarnings("ParameterNumber")
    protected ClientMessage encodeAuthenticationResponse(byte status, Address thisAddress, UUID uuid,
                                                         byte serializationVersion, String serverVersion,
                                                         int partitionCount, UUID clusterId, boolean failoverSupported,
                                                         List<Integer> tpcPorts, byte[] tpcToken,
                                                         MembersView membersView, PartitionsView partitionsView,
                                                         Map<String, String> keyValuePairs) {
        if (membersView == null) {
            membersView = new MembersView(-1, Collections.emptyList());
        }

        if (partitionsView == null) {
            partitionsView = new PartitionsView(emptyMap(), -1);
        }

        List<Map.Entry<UUID, List<Integer>>> partitions = partitionsView.partitions().entrySet().stream().toList();

        return encodeAuthenticationResponse(status, thisAddress, uuid, serializationVersion, serverVersion, partitionCount,
                clusterId, failoverSupported, tpcPorts, tpcToken, membersView.getVersion(), membersView.getMembers(),
                partitionsView.version(), partitions, keyValuePairs);
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    protected abstract ClientMessage encodeAuthenticationResponse(byte status, Address thisAddress, UUID uuid,
                                                                  byte serializationVersion, String serverVersion,
                                                                  int partitionCount, UUID clusterId, boolean failoverSupported,
                                                                  List<Integer> tpcPorts, byte[] tpcToken, int memberListVersion,
                                                                  List<MemberInfo> members,
                                                                  int partitionsVersion,
                                                                  List<Map.Entry<UUID, List<Integer>>> partitions,
                                                                  Map<String, String> keyValuePairs);

    protected abstract String getClientType();


    @Override
    protected ClientMessage encodeResponse(Object response) {
        return (ClientMessage) response;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public String getServiceName() {
        return null;
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    @Override
    public String getMethodName() {
        return null;
    }

    @Override
    public Object[] getParameters() {
        return null;
    }
}
