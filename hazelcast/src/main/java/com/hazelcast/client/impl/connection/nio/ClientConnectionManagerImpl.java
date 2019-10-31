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

package com.hazelcast.client.impl.connection.nio;

import com.hazelcast.client.AuthenticationException;
import com.hazelcast.client.ClientNotAllowedInClusterException;
import com.hazelcast.client.HazelcastClientNotActiveException;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.impl.ClientTypes;
import com.hazelcast.client.impl.clientside.CandidateClusterContext;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.connection.AddressProvider;
import com.hazelcast.client.impl.connection.ClientConnectionManager;
import com.hazelcast.client.impl.connection.ClientConnectionStrategy;
import com.hazelcast.client.impl.protocol.AuthenticationStatus;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientAuthenticationCodec;
import com.hazelcast.client.impl.protocol.codec.ClientAuthenticationCustomCodec;
import com.hazelcast.client.impl.protocol.codec.ClientIsFailoverSupportedCodec;
import com.hazelcast.client.impl.spi.ClientExecutionService;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.impl.spi.impl.ClientInvocationFuture;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.ChannelErrorHandler;
import com.hazelcast.internal.networking.ChannelInitializerProvider;
import com.hazelcast.internal.networking.nio.NioNetworking;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.ConnectionListener;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.internal.util.AddressUtil;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.cluster.Address;
import com.hazelcast.nio.SocketInterceptor;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.PasswordCredentials;
import com.hazelcast.security.TokenCredentials;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.hazelcast.spi.properties.HazelcastProperties;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static com.hazelcast.client.properties.ClientProperty.IO_BALANCER_INTERVAL_SECONDS;
import static com.hazelcast.client.properties.ClientProperty.IO_INPUT_THREAD_COUNT;
import static com.hazelcast.client.properties.ClientProperty.IO_OUTPUT_THREAD_COUNT;
import static com.hazelcast.client.properties.ClientProperty.IO_WRITE_THROUGH_ENABLED;
import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Implementation of {@link ClientConnectionManager}.
 */
@SuppressWarnings("checkstyle:classdataabstractioncoupling")
public class ClientConnectionManagerImpl implements ClientConnectionManager {

    private static final int DEFAULT_SMART_CLIENT_THREAD_COUNT = 3;

    protected final AtomicInteger connectionIdGen = new AtomicInteger();

    protected volatile boolean alive;
    private final ILogger logger;
    private final int connectionTimeoutMillis;
    private final HazelcastClientInstanceImpl client;
    private final ClientExecutionService executionService;
    private final InetSocketAddressCache inetSocketAddressCache = new InetSocketAddressCache();
    private final ConcurrentMap<InetSocketAddress, ClientConnection> activeConnections
            = new ConcurrentHashMap<InetSocketAddress, ClientConnection>();
    private final ConcurrentMap<InetSocketAddress, AuthenticationFuture> connectionsInProgress =
            new ConcurrentHashMap<InetSocketAddress, AuthenticationFuture>();
    private final Collection<ConnectionListener> connectionListeners = new CopyOnWriteArrayList<ConnectionListener>();
    private final NioNetworking networking;
    private final HeartbeatManager heartbeat;
    private final long authenticationTimeout;
    private final ClientConnectionStrategy connectionStrategy;
    private final UUID clientUuid;
    // accessed only in synchronized block
    private final LinkedList<Integer> outboundPorts = new LinkedList<Integer>();
    private final Set<String> labels;
    private final int outboundPortCount;
    private final boolean failoverConfigProvided;
    private volatile Credentials lastCredentials;
    private volatile int clusterPartitionCount = -1;
    private volatile UUID clusterId;
    private volatile CandidateClusterContext currentClusterContext;

    public ClientConnectionManagerImpl(HazelcastClientInstanceImpl client) {
        this.client = client;
        this.labels = Collections.unmodifiableSet(client.getClientConfig().getLabels());
        this.logger = client.getLoggingService().getLogger(ClientConnectionManager.class);
        ClientNetworkConfig networkConfig = client.getClientConfig().getNetworkConfig();
        this.clientUuid = UuidUtil.newUnsecureUUID();
        final int connTimeout = networkConfig.getConnectionTimeout();
        this.connectionTimeoutMillis = connTimeout == 0 ? Integer.MAX_VALUE : connTimeout;
        this.executionService = client.getClientExecutionService();
        this.networking = initNetworking(client);
        this.connectionStrategy = client.getConnectionStrategy();
        this.outboundPorts.addAll(getOutboundPorts(networkConfig));
        this.outboundPortCount = outboundPorts.size();
        this.heartbeat = new HeartbeatManager(this, client);
        this.authenticationTimeout = heartbeat.getHeartbeatTimeout();
        this.failoverConfigProvided = client.getFailoverConfig() != null;
    }

    private Collection<Integer> getOutboundPorts(ClientNetworkConfig networkConfig) {
        Collection<Integer> outboundPorts = networkConfig.getOutboundPorts();
        Collection<String> outboundPortDefinitions = networkConfig.getOutboundPortDefinitions();
        return AddressUtil.getOutboundPorts(outboundPorts, outboundPortDefinitions);
    }

    public NioNetworking getNetworking() {
        return networking;
    }

    protected NioNetworking initNetworking(final HazelcastClientInstanceImpl client) {
        HazelcastProperties properties = client.getProperties();

        ClientNetworkConfig networkConfig = client.getClientConfig().getNetworkConfig();
        boolean smartClient = networkConfig == null || networkConfig.isSmartRouting();

        int configuredInputThreads = properties.getInteger(IO_INPUT_THREAD_COUNT);
        int configuredOutputThreads = properties.getInteger(IO_OUTPUT_THREAD_COUNT);

        int inputThreads;
        if (configuredInputThreads == -1) {
            inputThreads = smartClient ? DEFAULT_SMART_CLIENT_THREAD_COUNT : 1;
        } else {
            inputThreads = configuredInputThreads;
        }

        int outputThreads;
        if (configuredOutputThreads == -1) {
            outputThreads = smartClient ? DEFAULT_SMART_CLIENT_THREAD_COUNT : 1;
        } else {
            outputThreads = configuredOutputThreads;
        }

        return new NioNetworking(
                new NioNetworking.Context()
                        .loggingService(client.getLoggingService())
                        .metricsRegistry(client.getMetricsRegistry())
                        .threadNamePrefix(client.getName())
                        .errorHandler(new ClientConnectionChannelErrorHandler())
                        .inputThreadCount(inputThreads)
                        .outputThreadCount(outputThreads)
                        .balancerIntervalSeconds(properties.getInteger(IO_BALANCER_INTERVAL_SECONDS))
                        .writeThroughEnabled(properties.getBoolean(IO_WRITE_THROUGH_ENABLED))
                        .concurrencyDetection(client.getConcurrencyDetection()));
    }

    public ClientConnectionStrategy getConnectionStrategy() {
        return connectionStrategy;
    }

    @Override
    public Collection<ClientConnection> getActiveConnections() {
        return activeConnections.values();
    }

    @Override
    public boolean isAlive() {
        return alive;
    }

    public synchronized void start() {
        if (alive) {
            return;
        }
        alive = true;
        startNetworking();

        heartbeat.start();
    }

    protected void startNetworking() {
        networking.restart();
    }

    public synchronized void shutdown() {
        if (!alive) {
            return;
        }
        alive = false;

        for (Connection connection : activeConnections.values()) {
            connection.close("Hazelcast client is shutting down", null);
        }

        stopNetworking();
        connectionListeners.clear();
        heartbeat.shutdown();
        if (currentClusterContext != null) {
            currentClusterContext.destroy();
        }
    }

    @Override
    public UUID getClientUuid() {
        return clientUuid;
    }

    protected void stopNetworking() {
        networking.shutdown();
    }

    @Override
    public Connection getActiveConnection(Address target) {
        if (target == null) {
            return null;
        }
        return activeConnections.get(inetSocketAddressCache.get(target));
    }

    @Override
    public Connection getOrConnect(Address address) throws IOException {
        try {
            ClientConnection connection = (ClientConnection) getConnection(address);
            if (connection != null) {
                return connection;
            }
            AuthenticationFuture future = triggerConnect(address);
            return future.get();
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    @Override
    public Connection getOrTriggerConnect(Address target) throws IOException {
        Connection connection = getConnection(target);
        if (connection != null) {
            return connection;
        }
        triggerConnect(target);
        return null;
    }

    private Connection getConnection(Address target) throws IOException {
        checkAllowed(target);
        if (target == null) {
            throw new IllegalStateException("Address can not be null");
        }

        return activeConnections.get(inetSocketAddressCache.get(target));
    }

    private void checkAllowed(Address target) throws IOException {
        if (!alive) {
            throw new HazelcastClientNotActiveException("ConnectionManager is not active!");
        }
        connectionStrategy.beforeGetConnection(target);
    }


    private AuthenticationFuture triggerConnect(Address target) {
        connectionStrategy.beforeOpenConnection(target);

        AuthenticationFuture future = new AuthenticationFuture();
        AuthenticationFuture oldFuture = connectionsInProgress.putIfAbsent(inetSocketAddressCache.get(target), future);
        if (oldFuture == null) {
            executionService.execute(new InitConnectionTask(target, future));
            return future;
        }
        return oldFuture;
    }

    private void fireConnectionAddedEvent(ClientConnection connection) {
        for (ConnectionListener connectionListener : connectionListeners) {
            connectionListener.connectionAdded(connection);
        }
        connectionStrategy.onConnect(connection);
    }

    private void fireConnectionRemovedEvent(ClientConnection connection) {
        for (ConnectionListener listener : connectionListeners) {
            listener.connectionRemoved(connection);
        }
        connectionStrategy.onDisconnect(connection);
    }

    private boolean useAnyOutboundPort() {
        return outboundPortCount == 0;
    }

    private int acquireOutboundPort() {
        if (outboundPortCount == 0) {
            return 0;
        }
        synchronized (outboundPorts) {
            final Integer port = outboundPorts.removeFirst();
            outboundPorts.addLast(port);
            return port;
        }
    }

    private void bindSocketToPort(Socket socket) throws IOException {
        if (useAnyOutboundPort()) {
            SocketAddress socketAddress = new InetSocketAddress(0);
            socket.bind(socketAddress);
        } else {
            int retryCount = outboundPortCount * 2;
            IOException ex = null;
            for (int i = 0; i < retryCount; i++) {
                int port = acquireOutboundPort();
                if (port == 0) {
                    // fast-path for ephemeral range - no need to bind
                    return;
                }
                SocketAddress socketAddress = new InetSocketAddress(port);
                try {
                    socket.bind(socketAddress);
                    return;
                } catch (IOException e) {
                    ex = e;
                    logger.finest("Could not bind port[ " + port + "]: " + e.getMessage());
                }
            }
            throw ex;
        }
    }

    protected ClientConnection createSocketConnection(final Address remoteAddress) throws IOException {
        SocketChannel socketChannel = null;
        try {
            socketChannel = SocketChannel.open();
            Socket socket = socketChannel.socket();

            bindSocketToPort(socket);

            ChannelInitializerProvider channelInitializer = currentClusterContext.getChannelInitializerProvider();
            Channel channel = networking.register(null, channelInitializer, socketChannel, true);
            channel.connect(inetSocketAddressCache.get(remoteAddress), connectionTimeoutMillis);

            ClientConnection connection
                    = new ClientConnection(client, connectionIdGen.incrementAndGet(), channel);

            socketChannel.configureBlocking(true);
            SocketInterceptor socketInterceptor = currentClusterContext.getSocketInterceptor();
            if (socketInterceptor != null) {
                socketInterceptor.onConnect(socket);
            }

            channel.start();
            return connection;
        } catch (Exception e) {
            closeResource(socketChannel);
            throw rethrow(e, IOException.class);
        }
    }

    void onClose(Connection connection) {
        removeFromActiveConnections((ClientConnection) connection);
    }

    private void removeFromActiveConnections(ClientConnection connection) {
        Address endpoint = connection.getEndPoint();

        if (endpoint == null) {
            if (logger.isFinestEnabled()) {
                logger.finest("Destroying " + connection + ", but it has end-point set to null "
                        + "-> not removing it from a connection map");
            }
            return;
        }
        if (activeConnections.remove(inetSocketAddressCache.get(endpoint), connection)) {
            logger.info("Removed connection to endpoint: " + endpoint + ", connection: " + connection);
            fireConnectionRemovedEvent(connection);
        } else {
            if (logger.isFinestEnabled()) {
                logger.finest("Destroying a connection, but there is no mapping " + endpoint + " -> " + connection
                        + " in the connection map.");
            }
        }
    }

    @Override
    public void addConnectionListener(ConnectionListener connectionListener) {
        connectionListeners.add(connectionListener);
    }

    public Credentials getLastCredentials() {
        return lastCredentials;
    }

    @Override
    public void setCandidateClusterContext(CandidateClusterContext context) {
        if (currentClusterContext == null) {
            context.start();
        }
        currentClusterContext = context;
    }

    public void beforeClusterSwitch(CandidateClusterContext context) {
        for (ClientConnection activeConnection : activeConnections.values()) {
            activeConnection.close(null, new TargetDisconnectedException("Closing since client is switching cluster"));
        }

        if (currentClusterContext != null) {
            currentClusterContext.destroy();
        }
        clusterId = null;
        inetSocketAddressCache.clear();
        currentClusterContext = context;
        currentClusterContext.start();
    }

    private class TimeoutAuthenticationTask implements Runnable {

        private final ClientInvocationFuture future;

        TimeoutAuthenticationTask(ClientInvocationFuture future) {
            this.future = future;
        }

        @Override
        public void run() {
            if (future.isDone()) {
                return;
            }
            future.complete(new TimeoutException("Authentication response did not come back in "
                    + authenticationTimeout + " millis"));
        }
    }

    private class ClientConnectionChannelErrorHandler implements ChannelErrorHandler {
        @Override
        public void onError(Channel channel, Throwable cause) {
            if (channel == null) {
                logger.severe(cause);
            } else {
                if (cause instanceof OutOfMemoryError) {
                    logger.severe(cause);
                }

                Connection connection = (Connection) channel.attributeMap().get(ClientConnection.class);
                if (cause instanceof EOFException) {
                    connection.close("Connection closed by the other side", cause);
                } else {
                    connection.close("Exception in " + connection + ", thread=" + Thread.currentThread().getName(), cause);
                }
            }
        }
    }

    private class InitConnectionTask implements Runnable {

        private final Address target;
        private final AuthenticationFuture future;

        InitConnectionTask(Address target, AuthenticationFuture future) {
            this.target = target;
            this.future = future;
        }

        @Override
        public void run() {
            ClientConnection connection;
            try {
                connection = getConnection();
            } catch (Exception e) {
                logger.finest(e);
                future.onFailure(e);
                connectionsInProgress.remove(inetSocketAddressCache.get(target));
                return;
            }

            try {
                authenticateAsync(connection);
            } catch (Exception e) {
                future.onFailure(e);
                connection.close("Failed to authenticate connection", e);
                connectionsInProgress.remove(inetSocketAddressCache.get(target));
            }
        }

        private void authenticateAsync(ClientConnection connection) {
            ClientMessage clientMessage = encodeAuthenticationRequest();
            ClientInvocation clientInvocation = new ClientInvocation(client, clientMessage, null, connection);
            ClientInvocationFuture invocationFuture = clientInvocation.invokeUrgent();

            ClientInvocationFuture failoverFuture = null;
            if (failoverConfigProvided) {
                ClientMessage isFailoverSupportedMessage = ClientIsFailoverSupportedCodec.encodeRequest();
                failoverFuture = new ClientInvocation(client, isFailoverSupportedMessage, null, connection).invoke();
            }

            ScheduledFuture timeoutTaskFuture = executionService.schedule(
                    new TimeoutAuthenticationTask(invocationFuture), authenticationTimeout, MILLISECONDS);
            AuthCallback callback = new AuthCallback(connection, target, future, timeoutTaskFuture, failoverFuture);
            invocationFuture.whenCompleteAsync(callback);
        }

        private ClientMessage encodeAuthenticationRequest() {
            InternalSerializationService ss = client.getSerializationService();
            byte serializationVersion = ss.getVersion();

            Credentials credentials = currentClusterContext.getCredentialsFactory().newCredentials();
            lastCredentials = credentials;

            UUID resolvedClusterId = null;
            if (failoverConfigProvided) {
                resolvedClusterId = clusterId;
            }

            String clusterName = currentClusterContext.getClusterName();
            if (credentials instanceof PasswordCredentials) {
                PasswordCredentials cr = (PasswordCredentials) credentials;
                return ClientAuthenticationCodec.encodeRequest(clusterName, cr.getName(),
                        cr.getPassword(), clientUuid, ClientTypes.JAVA, serializationVersion,
                        BuildInfoProvider.getBuildInfo().getVersion(), client.getName(), labels, clusterPartitionCount,
                        resolvedClusterId);
            } else {
                Data data;
                if (credentials instanceof TokenCredentials) {
                    data = new HeapData(((TokenCredentials) credentials).getToken());
                } else {
                    data = ss.toData(credentials);
                }
                return ClientAuthenticationCustomCodec.encodeRequest(clusterName, data,
                        clientUuid, ClientTypes.JAVA, serializationVersion, BuildInfoProvider.getBuildInfo().getVersion(),
                        client.getName(), labels, clusterPartitionCount, resolvedClusterId);
            }
        }

        private ClientConnection getConnection() throws IOException {
            ClientConnection connection = activeConnections.get(inetSocketAddressCache.get(target));
            if (connection != null) {
                return connection;
            }
            AddressProvider addressProvider = currentClusterContext.getAddressProvider();
            Address address = null;
            try {
                address = addressProvider.translate(target);
            } catch (Exception e) {
                logger.warning("Failed to translate address " + target + " via address provider " + e.getMessage());
            }
            if (address == null) {
                throw new NullPointerException("Address Provider " + addressProvider.getClass()
                        + " could not translate address " + target);
            }
            return createSocketConnection(address);
        }
    }

    private class AuthCallback implements BiConsumer<ClientMessage, Throwable> {
        private final ClientConnection connection;
        private final Address target;
        private final AuthenticationFuture future;
        private final ScheduledFuture timeoutTaskFuture;
        private final ClientInvocationFuture isFailoverFuture;

        AuthCallback(ClientConnection connection, Address target,
                     AuthenticationFuture future, ScheduledFuture timeoutTaskFuture, ClientInvocationFuture isFailoverFuture) {
            this.connection = connection;
            this.target = target;
            this.future = future;
            this.timeoutTaskFuture = timeoutTaskFuture;
            this.isFailoverFuture = isFailoverFuture;
        }

        @Override
        public void accept(ClientMessage response, Throwable throwable) {
            if (throwable == null) {
                onResponse(response);
            } else {
                onFailure(throwable);
            }
        }

        public void onResponse(ClientMessage response) {
            timeoutTaskFuture.cancel(true);
            ClientAuthenticationCodec.ResponseParameters result;
            try {
                result = ClientAuthenticationCodec.decodeResponse(response);
            } catch (HazelcastException e) {
                onFailure(e);
                return;
            }

            AuthenticationStatus authenticationStatus = AuthenticationStatus.getById(result.status);
            switch (authenticationStatus) {
                case AUTHENTICATED:
                    if (!checkFailoverSupportIfNeeded(result)) {
                        return;
                    }
                    handleSuccessResult(result);
                    onAuthenticated();
                    future.onSuccess(connection);
                    break;
                case CREDENTIALS_FAILED:
                    onFailure(new AuthenticationException("Invalid credentials!"));
                    break;
                case NOT_ALLOWED_IN_CLUSTER:
                    onFailure(new ClientNotAllowedInClusterException("Client is not allowed in the cluster"));
                    break;
                default:
                    onFailure(new AuthenticationException("Authentication status code not supported. status: "
                            + authenticationStatus));
            }
        }

        private void handleSuccessResult(ClientAuthenticationCodec.ResponseParameters result) {
            clusterPartitionCount = result.partitionCount;
            clusterId = result.clusterId;
            connection.setConnectedServerVersion(result.serverHazelcastVersion);

            connection.setRemoteEndpoint(result.address);
        }

        private boolean checkFailoverSupportIfNeeded(ClientAuthenticationCodec.ResponseParameters result) {
            boolean isFailoverAskedToCluster = isFailoverFuture != null;
            if (!isFailoverAskedToCluster) {
                return true;
            }

            if (BuildInfo.calculateVersion(result.serverHazelcastVersion) < BuildInfo.calculateVersion("3.12")) {
                //this means that server is too old and failover not supported
                //IllegalStateException will cause client to give up trying on this cluster
                onFailure(new ClientNotAllowedInClusterException("Cluster does not support failover. "
                        + "This feature is available in Hazelcast Enterprise with version 3.12 and after"));
                return false;
            }

            try {
                boolean isAllowed = ClientIsFailoverSupportedCodec.decodeResponse(isFailoverFuture.get()).response;
                if (!isAllowed) {
                    //in this path server is new but not enterprise.
                    //IllegalStateException will cause client to give up trying on this cluster
                    onFailure(new ClientNotAllowedInClusterException("Cluster does not support failover. "
                            + "This feature is available in Hazelcast Enterprise"));
                    return false;
                }
                return true;
            } catch (Exception e) {
                //server version is 3.12 or newer, but an exception occured while getting the isFailover supported info
                //Exception is delegated without wrapping `IllegalStateException` to so that we will continue
                // trying same connection
                onFailure(e);
                return false;
            }
        }

        private void onAuthenticated() {
            Address memberAddress = connection.getEndPoint();
            ClientConnection oldConnection = activeConnections.put(inetSocketAddressCache.get(memberAddress), connection);
            if (oldConnection == null) {
                if (logger.isFinestEnabled()) {
                    logger.finest("Authentication succeeded for " + connection
                            + " and there was no old connection to this end-point");
                }
                fireConnectionAddedEvent(connection);
            } else {
                if (logger.isFinestEnabled()) {
                    logger.finest("Re-authentication succeeded for " + connection);
                }
                if (!connection.equals(oldConnection)) {
                    logger.severe("The address that client is connected from does not match with the member address. "
                            + " This setup is illegal and will cause inconsistent behaviour "
                            + "Address that client uses : " + target + ", member address : " + memberAddress);
                }
            }

            connectionsInProgress.remove(inetSocketAddressCache.get(target));
            logger.info("Authenticated with server " + memberAddress + ", server version:" + connection
                    .getConnectedServerVersion() + " Local address: " + connection.getLocalSocketAddress());

            // Check if connection is closed by remote before authentication complete, if that is the case
            // we need to remove it back from active connections.
            // Race description from https://github.com/hazelcast/hazelcast/pull/8832.(A little bit changed)
            // - open a connection client -> member
            // - send auth message
            // - receive auth reply -> reply processing is offloaded to an executor. Did not start to run yet.
            // - member closes the connection -> the connection is trying to removed from map
            //                                                     but it was not there to begin with
            // - the executor start processing the auth reply -> it put the connection to the connection map.
            // - we end up with a closed connection in activeConnections map
            if (!connection.isAlive()) {
                removeFromActiveConnections(connection);
            }
        }

        public void onFailure(Throwable cause) {
            timeoutTaskFuture.cancel(true);
            if (logger.isFinestEnabled()) {
                logger.finest("Authentication of " + connection + " failed.", cause);
            }
            connection.close(null, cause);
            connectionsInProgress.remove(inetSocketAddressCache.get(target));
            future.onFailure(cause);
        }
    }

    private static class InetSocketAddressCache {

        private final ConcurrentMap<Address, InetSocketAddress> cache = new ConcurrentHashMap<Address, InetSocketAddress>();

        private InetSocketAddress get(Address target) {
            try {
                InetSocketAddress resolvedAddress = cache.get(target);
                if (resolvedAddress != null) {
                    return resolvedAddress;
                }
                InetSocketAddress newResolvedAddress = new InetSocketAddress(target.getInetAddress(), target.getPort());
                InetSocketAddress prevAddress = cache.putIfAbsent(target, newResolvedAddress);
                if (prevAddress != null) {
                    return prevAddress;
                }
                return newResolvedAddress;
            } catch (UnknownHostException e) {
                throw rethrow(e);
            }
        }

        //only called when there is a cluster switch
        public void clear() {
            cache.clear();
        }
    }
}
