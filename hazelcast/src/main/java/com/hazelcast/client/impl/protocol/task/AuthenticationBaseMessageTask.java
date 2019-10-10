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

import com.hazelcast.client.impl.ClientEndpoint;
import com.hazelcast.client.impl.ClientPartitionListenerService;
import com.hazelcast.client.impl.ClientTypes;
import com.hazelcast.client.impl.protocol.AuthenticationStatus;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientAddMembershipListenerCodec;
import com.hazelcast.cluster.InitialMembershipEvent;
import com.hazelcast.cluster.InitialMembershipListener;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MemberAttributeEvent;
import com.hazelcast.cluster.MemberAttributeOperationType;
import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.ConnectionType;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.security.UsernamePasswordCredentials;

import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.security.Permission;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;

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
                addPartitionListener();
                addMembershipListener();
                sendClientMessage(prepareAuthenticatedClientMessage());
                break;
            default:
                throw new IllegalStateException("Unhandled authentication result");
        }
    }

    private boolean addPartitionListener() {
        InternalPartitionService internalPartitionService = getService(InternalPartitionService.SERVICE_NAME);
        internalPartitionService.firstArrangement();
        final ClientPartitionListenerService service = clientEngine.getPartitionListenerService();
        service.registerPartitionListener(endpoint, clientMessage.getCorrelationId());
        endpoint.addDestroyAction(UuidUtil.newUnsecureUUID(), new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                service.deregisterPartitionListener(endpoint);
                return Boolean.TRUE;
            }
        });
        return true;
    }

    private void addMembershipListener() {
        String serviceName = ClusterServiceImpl.SERVICE_NAME;
        ClusterServiceImpl service = getService(serviceName);
        boolean advancedNetworkConfigEnabled = isAdvancedNetworkEnabled();
        UUID registrationId = service.addMembershipListener(new MembershipListenerImpl(endpoint, advancedNetworkConfigEnabled));
        endpoint.addListenerDestroyAction(serviceName, serviceName, registrationId);
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
            return authenticate(clientEngine.getSecurityContext());
        } else if (credentials instanceof UsernamePasswordCredentials) {
            UsernamePasswordCredentials usernamePasswordCredentials = (UsernamePasswordCredentials) credentials;
            return verifyClusterName(usernamePasswordCredentials);
        } else {
            logger.severe("Hazelcast security is disabled.\nUsernamePasswordCredentials or cluster-name "
                    + "should be used for authentication!\n" + "Current credentials type is: "
                    + credentials.getClass().getName());
            return CREDENTIALS_FAILED;
        }
    }

    private AuthenticationStatus authenticate(SecurityContext securityContext) {
        Connection connection = endpoint.getConnection();
        try {
            LoginContext lc = securityContext.createClientLoginContext(credentials, connection);
            lc.login();
            endpoint.setLoginContext(lc);
            return AUTHENTICATED;
        } catch (LoginException e) {
            logger.warning(e);
            return CREDENTIALS_FAILED;
        }
    }

    private AuthenticationStatus verifyClusterName(UsernamePasswordCredentials credentials) {
        String nodeClusterName = nodeEngine.getConfig().getClusterName();
        boolean usernameMatch = nodeClusterName.equals(credentials.getName());
        return usernameMatch ? AUTHENTICATED : CREDENTIALS_FAILED;
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

    private class MembershipListenerImpl
            implements InitialMembershipListener {
        private final ClientEndpoint endpoint;
        private final boolean advancedNetworkConfigEnabled;

        MembershipListenerImpl(ClientEndpoint endpoint, boolean advancedNetworkConfigEnabled) {
            this.endpoint = endpoint;
            this.advancedNetworkConfigEnabled = advancedNetworkConfigEnabled;
        }

        @Override
        public void init(InitialMembershipEvent membershipEvent) {
            ClusterService service = getService(ClusterServiceImpl.SERVICE_NAME);
            Collection<MemberImpl> members = service.getMemberImpls();
            Collection<Member> membersToSend = new ArrayList<>();
            for (MemberImpl member : members) {
                membersToSend.add(translateMemberAddress(member));
            }
            ClientMessage eventMessage = ClientAddMembershipListenerCodec.encodeMemberListEvent(membersToSend);
            sendClientMessage(endpoint.getUuid(), eventMessage);
        }

        @Override
        public void memberAdded(MembershipEvent membershipEvent) {
            if (!shouldSendEvent()) {
                return;
            }

            MemberImpl member = (MemberImpl) membershipEvent.getMember();

            ClientMessage eventMessage =
                    ClientAddMembershipListenerCodec.encodeMemberEvent(translateMemberAddress(member),
                            MembershipEvent.MEMBER_ADDED);
            sendClientMessage(endpoint.getUuid(), eventMessage);
        }

        @Override
        public void memberRemoved(MembershipEvent membershipEvent) {
            if (!shouldSendEvent()) {
                return;
            }

            MemberImpl member = (MemberImpl) membershipEvent.getMember();
            ClientMessage eventMessage = ClientAddMembershipListenerCodec.encodeMemberEvent(translateMemberAddress(member),
                    MembershipEvent.MEMBER_REMOVED);
            sendClientMessage(endpoint.getUuid(), eventMessage);
        }

        @Override
        public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {
            if (!shouldSendEvent()) {
                return;
            }

            MemberAttributeOperationType op = memberAttributeEvent.getOperationType();
            String key = memberAttributeEvent.getKey();
            String value = memberAttributeEvent.getValue() == null ? null : memberAttributeEvent.getValue().toString();
            ClientMessage eventMessage = ClientAddMembershipListenerCodec
                    .encodeMemberAttributeChangeEvent(memberAttributeEvent.getMember(), memberAttributeEvent.getMembers(), key,
                            op.getId(), value);
            sendClientMessage(endpoint.getUuid(), eventMessage);
        }

        private boolean shouldSendEvent() {
            if (!endpoint.isAlive()) {
                return false;
            }

            boolean localOnly = false;
            ClusterService clusterService = clientEngine.getClusterService();
            if (localOnly && !clusterService.isMaster()) {
                //if client registered localOnly, only master is allowed to send request
                return false;
            }
            return true;
        }

        // the member partition table that is sent out to clients must contain the addresses
        // on which cluster members listen for CLIENT protocol connections.
        // with advanced network config, we need to return Members whose getAddress method
        // returns the CLIENT server socket address
        private MemberImpl translateMemberAddress(MemberImpl member) {
            if (!advancedNetworkConfigEnabled) {
                return member;
            }

            Address clientAddress = member.getAddressMap().get(EndpointQualifier.CLIENT);

            MemberImpl result = new MemberImpl.Builder(clientAddress)
                    .version(member.getVersion())
                    .uuid(member.getUuid())
                    .localMember(member.localMember())
                    .liteMember(member.isLiteMember())
                    .memberListJoinVersion(member.getMemberListJoinVersion())
                    .attributes(member.getAttributes())
                    .build();
            return result;
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
