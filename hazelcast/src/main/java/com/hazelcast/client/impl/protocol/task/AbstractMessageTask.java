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

package com.hazelcast.client.impl.protocol.task;

import com.hazelcast.client.AuthenticationException;
import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.ClientEndpointManager;
import com.hazelcast.client.impl.ClientEngineImpl;
import com.hazelcast.client.impl.client.SecureRequest;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes;
import com.hazelcast.client.impl.protocol.parameters.ExceptionResultParameters;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.security.Permission;
import java.util.logging.Level;

/**
 * Base Message task
 */
public abstract class AbstractMessageTask<P>
        implements MessageTask, SecureRequest {

    protected final P parameters;
    protected final ClientMessage clientMessage;

    protected final Connection connection;
    protected final ClientEndpoint endpoint;
    protected final NodeEngineImpl nodeEngine;
    protected final SerializationService serializationService;
    protected final ILogger logger;
    protected final ClientEndpointManager endpointManager;
    protected final ClientEngineImpl clientEngine;

    private final Node node;

    protected AbstractMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        this.clientMessage = clientMessage;
        this.logger = node.getLogger(getClass());
        this.node = node;
        this.nodeEngine = node.nodeEngine;
        this.serializationService = node.getSerializationService();
        this.connection = connection;
        this.parameters = decodeClientMessage(clientMessage);
        this.clientEngine = node.clientEngine;
        this.endpointManager = clientEngine.getEndpointManager();
        this.endpoint = getEndpoint();
    }

    @SuppressWarnings("unchecked")
    public <S> S getService(String serviceName) {
        return (S) node.nodeEngine.getService(serviceName);
    }

    protected ClientEndpoint getEndpoint() {
        return endpointManager.getEndpoint(connection);
    }

    protected abstract P decodeClientMessage(ClientMessage clientMessage);

    protected abstract ClientMessage encodeResponse(Object response);

    @Override
    public int getPartitionId() {
        return clientMessage.getPartitionId();
    }

    @Override
    public void run() {
        try {
            if (endpoint == null) {
                handleMissingEndpoint();
            } else if (isAuthenticationMessage()) {
                initializeAndProcessMessage();
            } else if (!endpoint.isAuthenticated()) {
                handleAuthenticationFailure();
            } else {
                initializeAndProcessMessage();
            }

        } catch (Throwable e) {
            logProcessingFailure(e);
            handleProcessingFailure(e);
        }
    }

    protected boolean isAuthenticationMessage() {
        return false;
    }

    private void initializeAndProcessMessage() {
        if (!node.joined()) {
            throw new HazelcastInstanceNotActiveException("Hazelcast instance is not ready yet!");
        }
        Credentials credentials = endpoint.getCredentials();
        interceptBefore(credentials);
        checkPermissions(endpoint);
        processMessage();
    }

    private void handleAuthenticationFailure() {
        Exception exception;
        if (nodeEngine.isActive()) {
            String message = "Client " + endpoint + " must authenticate before any operation.";
            logger.severe(message);
            exception = new AuthenticationException(message);
        } else {
            exception = new HazelcastInstanceNotActiveException();
        }
        endpoint.sendResponse(exception, clientMessage.getCorrelationId());
        endpointManager.removeEndpoint(endpoint);
    }

    private void handleMissingEndpoint() {
        if (connection.isAlive()) {
            logger.severe("Dropping: " + parameters + " -> no endpoint found for live connection.");
        } else {
            if (logger.isFinestEnabled()) {
                logger.finest("Dropping: " + parameters + " -> no endpoint found for dead connection.");
            }
        }
    }

    private void logProcessingFailure(Throwable throwable) {
        Level level = nodeEngine.isActive() ? Level.SEVERE : Level.FINEST;
        if (logger.isLoggable(level)) {
            if (parameters == null) {
                logger.log(level, throwable.getMessage(), throwable);
            } else {
                logger.log(level, "While executing request: " + parameters + " -> " + throwable.getMessage(), throwable);
            }
        }
    }

    private void handleProcessingFailure(Throwable throwable) {
        if (parameters != null && endpoint != null) {
            sendClientMessage(throwable);
        }
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

    private ClientMessage createExceptionMessage(Throwable throwable) {
        int errorCode = ClientProtocolErrorCodes.getErrorCode(throwable);
        String message = throwable.getMessage();
        StackTraceElement[] stackTrace = throwable.getStackTrace();
        return ExceptionResultParameters.encode(errorCode, message, stackTrace);
    }

    protected abstract void processMessage();

    protected void sendResponse(Object response) {
        ClientMessage clientMessage = encodeResponse(response);
        sendClientMessage(clientMessage);
    }

    protected void sendClientMessage(ClientMessage resultClientMessage) {
        resultClientMessage.setCorrelationId(clientMessage.getCorrelationId());
        resultClientMessage.addFlag(ClientMessage.BEGIN_AND_END_FLAGS);
        resultClientMessage.setVersion(ClientMessage.VERSION);
        final Connection connection = endpoint.getConnection();
        //TODO framing not implemented yet, should be split into frames before writing to connection
        connection.write(resultClientMessage);
    }

    protected void sendClientMessage(Object key, ClientMessage resultClientMessage) {
        int partitionId = key == null ? -1 : nodeEngine.getPartitionService().getPartitionId(key);
        resultClientMessage.setPartitionId(partitionId);
        sendClientMessage(resultClientMessage);
    }

    protected void sendClientMessage(Throwable throwable) {
        ClientMessage exception = createExceptionMessage(throwable);
        sendClientMessage(exception);
    }

    public abstract String getServiceName();

    public String getDistributedObjectType() {
        return getServiceName();
    }

    @Override
    public abstract String getDistributedObjectName();

    @Override
    public abstract String getMethodName();

    @Override
    public abstract Object[] getParameters();
}
