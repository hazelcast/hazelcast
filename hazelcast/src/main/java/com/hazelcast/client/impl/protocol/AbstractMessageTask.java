package com.hazelcast.client.impl.protocol;

import com.hazelcast.client.ClientEndpointManager;
import com.hazelcast.client.ClientEngine;
import com.hazelcast.client.impl.ClientEndpointImpl;
import com.hazelcast.client.impl.ClientEngineImpl;
import com.hazelcast.client.impl.client.SecureRequest;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.security.Permission;
import java.util.logging.Level;

/**
 * Base Message task
 */
public abstract class AbstractMessageTask<P>
        implements PartitionSpecificRunnable, SecureRequest {

    protected final P parameters;
    protected final ClientMessage clientMessage;

    protected final Connection connection;
    protected final ClientEndpointImpl endpoint;
    protected final NodeEngineImpl nodeEngine;
    protected final SerializationService serializationService;
    protected final ILogger logger;
    protected final ClientEndpointManager endpointManager;
    protected final ClientEngineImpl clientEngine;

    private final Node node;

    protected AbstractMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        this.clientMessage = clientMessage;
        this.logger = node.getLogger(ClientEngine.class);
        this.node = node;
        this.nodeEngine = node.nodeEngine;
        this.serializationService = node.getSerializationService();
        this.connection = connection;
        this.parameters = decodeClientMessage(clientMessage);
        this.clientEngine = node.clientEngine;
        this.endpointManager = clientEngine.getEndpointManager();
        this.endpoint = getEndpoint();
    }

    protected ClientEndpointImpl getEndpoint() {
        return (ClientEndpointImpl) endpointManager.getEndpoint(connection);
    }

    protected abstract P decodeClientMessage(ClientMessage clientMessage);

    @Override
    public int getPartitionId() {
        return clientMessage.getPartitionId();
    }

    @Override
    public void run() {
        try {
            if (endpoint == null) {
                handleMissingEndpoint();
                return;
            }
            //process message
            if (!node.joined()) {
                throw new HazelcastInstanceNotActiveException("Hazelcast instance is not ready yet!");
            }
            final Credentials credentials = endpoint.getCredentials();
            interceptBefore(credentials);
            checkPermissions(endpoint);
            processMessage();
            interceptAfter(credentials);

        } catch (Throwable e) {
            handleProcessingFailure(e);
        }
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

    private void handleProcessingFailure(Throwable throwable) {
        Level level = nodeEngine.isActive() ? Level.SEVERE : Level.FINEST;
        if (logger.isLoggable(level)) {
            if (parameters == null) {
                logger.log(level, throwable.getMessage(), throwable);
            } else {
                logger.log(level, "While executing request: " + parameters + " -> " + throwable.getMessage(), throwable);
            }
        }
        if (parameters != null && endpoint != null) {
            ClientMessage exception = createExceptionMessage(throwable);
            exception.setCorrelationId(clientMessage.getCorrelationId());
            endpoint.sendClientMessage(exception);
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

    private void checkPermissions(ClientEndpointImpl endpoint) {
        SecurityContext securityContext = clientEngine.getSecurityContext();
        if (securityContext != null) {
            Permission permission = getRequiredPermission();
            if (permission != null) {
                securityContext.checkPermission(endpoint.getSubject(), permission);
            }
        }
    }

    protected ClientMessage createExceptionMessage(Throwable throwable) {
        String className = throwable.getClass().getName();
        String message = throwable.getMessage();
        StringWriter sw = new StringWriter();
        throwable.printStackTrace(new PrintWriter(sw));
        final ClientMessage parameters = ExceptionResultParameters.encode(className, message, sw.toString());
        return parameters;
    }

    protected abstract void processMessage();

    @Override
    public String getDistributedObjectType() {
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
