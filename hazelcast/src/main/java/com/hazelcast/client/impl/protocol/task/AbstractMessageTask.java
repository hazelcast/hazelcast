/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.AuthenticationException;
import com.hazelcast.client.impl.ClientBackupAwareResponse;
import com.hazelcast.client.impl.ClientEndpoint;
import com.hazelcast.client.impl.ClientEndpointImpl;
import com.hazelcast.client.impl.ClientEndpointManager;
import com.hazelcast.client.impl.ClientEngine;
import com.hazelcast.client.impl.client.SecureRequest;
import com.hazelcast.client.impl.protocol.ClientExceptionFactory;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.cluster.Address;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.ConnectionType;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.server.ServerConnection;
import com.hazelcast.logging.ILogger;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.impl.responses.NormalResponse;

import java.lang.reflect.Field;
import java.security.AccessControlException;
import java.security.Permission;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.hazelcast.internal.util.ExceptionUtil.peel;

/**
 * Base Message task.
 */
@SuppressWarnings({"checkstyle:methodcount"})
public abstract class AbstractMessageTask<P> implements MessageTask, SecureRequest {

    private static final List<Class<? extends Throwable>> NON_PEELABLE_EXCEPTIONS =
            Arrays.asList(Error.class, MemberLeftException.class);

    protected final ClientMessage clientMessage;
    protected final ServerConnection connection;
    protected final ClientEndpoint endpoint;
    protected final NodeEngineImpl nodeEngine;
    protected final InternalSerializationService serializationService;
    protected final ILogger logger;
    protected final ClientEngine clientEngine;
    protected P parameters;
    private final ClientEndpointManager endpointManager;
    private final Node node;

    protected AbstractMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        this.clientMessage = clientMessage;
        this.logger = node.getLogger(getClass());
        this.node = node;
        this.nodeEngine = node.getNodeEngine();
        this.serializationService = node.getSerializationService();
        this.connection = (ServerConnection) connection;
        this.clientEngine = node.getClientEngine();
        this.endpointManager = clientEngine.getEndpointManager();
        this.endpoint = initEndpoint();
    }

    @SuppressWarnings("unchecked")
    public <S> S getService(String serviceName) {
        return (S) node.getNodeEngine().getService(serviceName);
    }

    private ClientEndpoint initEndpoint() {
        ClientEndpoint endpoint = endpointManager.getEndpoint(connection);
        if (endpoint != null) {
            return endpoint;
        }
        return new ClientEndpointImpl(clientEngine, nodeEngine, connection);
    }

    protected abstract P decodeClientMessage(ClientMessage clientMessage);

    protected abstract ClientMessage encodeResponse(Object response);

    @Override
    public final void run() {
        try {
            Address address = connection.getRemoteAddress();
            if (isManagementTask() && !clientEngine.getManagementTasksChecker().isTrusted(address)) {
                String message = "The client address " + address + " is not allowed for management task "
                        + getClass().getName();
                logger.info(message);
                throw new AccessControlException(message);
            } else if (requiresAuthentication() && !endpoint.isAuthenticated()) {
                handleAuthenticationFailure();
            } else {
                initializeAndProcessMessage();
            }
        } catch (Throwable e) {
            handleProcessingFailure(e);
        }
    }

    protected boolean requiresAuthentication() {
        return true;
    }

    /**
     * Used to accept hot restart messages (and some other messages required for
     * client to connect) sent from MC client when node start is not complete yet.
     */
    protected boolean acceptOnIncompleteStart() {
        return false;
    }

    /**
     * Used as a workaround for calling {@link #validateNodeStart} after
     * decoding auth messages, i.e. when connection type is unknown prior
     * to decode is made.
     */
    protected boolean validateNodeStartBeforeDecode() {
        return true;
    }

    private void initializeAndProcessMessage() throws Throwable {
        if (validateNodeStartBeforeDecode()) {
            validateNodeStart();
        }
        parameters = decodeClientMessage(clientMessage);
        assert addressesDecodedWithTranslation() : formatWrongAddressInDecodedMessage();
        Credentials credentials = endpoint.getCredentials();
        interceptBefore(credentials);
        checkPermissions(endpoint);
        processMessage();
        interceptAfter(credentials);
    }

    /**
     * Throws if node start is incomplete or if the message is from a special
     * subset of messages and it's sent from MC client.
     */
    protected final void validateNodeStart() {
        boolean acceptOnIncompleteStart = acceptOnIncompleteStart()
                && ConnectionType.MC_JAVA_CLIENT.equals(endpoint.getClientType());
        if (!acceptOnIncompleteStart && !node.getNodeExtension().isStartCompleted()) {
            throw new HazelcastInstanceNotActiveException("Hazelcast instance is not ready yet!");
        }
    }

    private void handleAuthenticationFailure() {
        Exception exception;
        if (nodeEngine.isRunning()) {
            String message = "Client " + endpoint + " must authenticate before any operation.";
            logger.severe(message);
            exception = new RetryableHazelcastException(new AuthenticationException(message));
        } else {
            exception = new HazelcastInstanceNotActiveException();
        }
        sendClientMessage(exception);
        connection.close("Authentication failed. " + exception.getMessage(), null);
    }

    private void logProcessingFailure(Throwable throwable) {
        if (logger.isFinestEnabled()) {
            if (parameters == null) {
                logger.finest(throwable.getMessage(), throwable);
            } else {
                logger.finest("While executing request: " + parameters + " -> " + throwable.getMessage(), throwable);
            }
        }
    }

    protected void handleProcessingFailure(Throwable throwable) {
        logProcessingFailure(throwable);
        sendClientMessage(throwable);
    }

    private void interceptBefore(Credentials credentials) {
        final SecurityContext securityContext = clientEngine.getSecurityContext();
        final String methodName = getMethodName();
        if (securityContext != null && methodName != null) {
            final String objectType = getDistributedObjectType();
            final String objectName = getDistributedObjectName();
            securityContext.interceptBefore(credentials, objectType, objectName, methodName, getParameters());
        }
    }

    private void interceptAfter(Credentials credentials) {
        final SecurityContext securityContext = clientEngine.getSecurityContext();
        final String methodName = getMethodName();
        if (securityContext != null && methodName != null) {
            final String objectType = getDistributedObjectType();
            final String objectName = getDistributedObjectName();
            securityContext.interceptAfter(credentials, objectType, objectName, methodName);
        }
    }

    private void checkPermissions(ClientEndpoint endpoint) {
        SecurityContext securityContext = clientEngine.getSecurityContext();
        if (securityContext != null) {
            Permission permission = getRequiredPermission();
            if (permission != null) {
                securityContext.checkPermission(endpoint.getSubject(), permission);
            }
        }
    }

    protected abstract void processMessage() throws Throwable;

    protected void sendResponse(Object response) {
        try {
            int numberOfBackups = 0;
            if (response instanceof ClientBackupAwareResponse) {
                ClientBackupAwareResponse backupAwareResponse = (ClientBackupAwareResponse) response;
                response = backupAwareResponse.getResponse();
                numberOfBackups = backupAwareResponse.getNumberOfBackups();
            } else if (response instanceof NormalResponse) {
                response = ((NormalResponse) response).getValue();
            }
            ClientMessage clientMessage;
            if (response instanceof Throwable) {
                clientMessage = encodeException((Throwable) response);
            } else {
                clientMessage = encodeResponse(response);
            }
            assert numberOfBackups >= 0 && numberOfBackups < Byte.MAX_VALUE;
            clientMessage.setNumberOfBackupAcks((byte) numberOfBackups);
            sendClientMessage(clientMessage);
        } catch (Exception e) {
            handleProcessingFailure(e);
        }
    }

    protected void sendClientMessage(ClientMessage resultClientMessage) {
        resultClientMessage.setCorrelationId(clientMessage.getCorrelationId());
        //TODO framing not implemented yet, should be split into frames before writing to connection
        // PETER: There is no point in chopping it up in frames and in 1 go write all these frames because it still will
        // not allow any interleaving with operations. It will only slow down the system. Framing should be done inside
        // the io system; not outside.
        connection.write(resultClientMessage);
    }

    protected void sendClientMessage(Object key, ClientMessage resultClientMessage) {
        int partitionId = key == null ? -1 : nodeEngine.getPartitionService().getPartitionId(key);
        resultClientMessage.setPartitionId(partitionId);
        sendClientMessage(resultClientMessage);
    }

    private void sendClientMessage(Throwable throwable) {
        ClientMessage message = encodeException(throwable);
        sendClientMessage(message);
    }

    protected ClientMessage encodeException(Throwable throwable) {
        ClientExceptionFactory exceptionFactory = clientEngine.getExceptionFactory();
        return exceptionFactory.createExceptionMessage(peelIfNeeded(throwable));
    }

    public abstract String getServiceName();

    @Override
    public String getDistributedObjectType() {
        return getServiceName();
    }

    @Override
    public abstract String getDistributedObjectName();

    @Override
    public abstract String getMethodName();

    @Override
    public abstract Object[] getParameters();

    protected final BuildInfo getMemberBuildInfo() {
        return node.getBuildInfo();
    }

    protected boolean isAdvancedNetworkEnabled() {
        return node.getConfig().getAdvancedNetworkConfig().isEnabled();
    }

    final boolean addressesDecodedWithTranslation() {
        if (!isAdvancedNetworkEnabled()) {
            return true;
        }
        if (parameters == null) {
            return true;
        }
        Class<Address> addressClass = Address.class;
        Field[] fields = parameters.getClass().getDeclaredFields();
        Set<Address> addresses = new HashSet<Address>();
        try {
            for (Field field : fields) {
                if (addressClass.isAssignableFrom(field.getType())) {
                    addresses.add((Address) field.get(parameters));
                }
            }
        } catch (IllegalAccessException e) {
            logger.info("Could not reflectively access parameter fields", e);
        }
        if (!addresses.isEmpty()) {
            Collection<Address> allMemberAddresses = node.clusterService.getMemberAddresses();
            for (Address address : addresses) {
                if (!allMemberAddresses.contains(address)) {
                    return false;
                }
            }
        }
        return true;
    }

    private String formatWrongAddressInDecodedMessage() {
        return "Decoded message of type " + parameters.getClass() + " contains untranslated addresses. "
                + "Use ClientEngine.memberAddressOf to translate addresses while decoding this client message.";
    }

    protected Throwable peelIfNeeded(Throwable t) {
        if (t == null) {
            return null;
        }

        for (Class<? extends Throwable> clazz : NON_PEELABLE_EXCEPTIONS) {
            if (clazz.isAssignableFrom(t.getClass())) {
                return t;
            }
        }
        //We are passing our own message factory, because we don't want checked exceptions to be wrapped to HazelcastException
        return peel(t, null, null, (throwable, message) -> throwable);
    }


    /**
     * The default implementation returns false. Child classes which implement a logic related to a management operation should
     * override it and return true so the proper access control mechanism is used.
     */
    @Override
    public boolean isManagementTask() {
        return false;
    }

}
