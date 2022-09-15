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

package com.hazelcast.internal.server.tcp;

import com.hazelcast.auditlog.AuditlogTypeIds;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.impl.OutOfMemoryErrorDispatcher;
import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.nio.SelectorMode;
import com.hazelcast.internal.server.ServerContext;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.logging.ILogger;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_DISCRIMINATOR_THREAD;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_METRIC_ACCEPTOR_EVENT_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_METRIC_ACCEPTOR_EXCEPTION_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_METRIC_ACCEPTOR_IDLE_TIME_MILLIS;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_METRIC_ACCEPTOR_SELECTOR_RECREATE_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_PREFIX_ACCEPTOR;
import static com.hazelcast.internal.metrics.ProbeLevel.DEBUG;
import static com.hazelcast.internal.metrics.ProbeUnit.MS;
import static com.hazelcast.internal.networking.nio.SelectorMode.SELECT_WITH_FIX;
import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.internal.util.ThreadUtil.createThreadPoolName;
import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;
import static java.lang.Math.max;
import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.currentThread;
import static java.nio.channels.SelectionKey.OP_ACCEPT;
import static java.util.Collections.newSetFromMap;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Contains the logic for accepting {@link TcpServerConnection}s.
 *
 * The {@link TcpServerAcceptor} and {@link TcpServerConnector} are 2 sides of the same coin. The
 * {@link TcpServerConnector} take care of the 'client' side of a connection and the {@link TcpServerAcceptor}
 * is the 'server' side of a connection (each connection has a client and server-side)
 */
public class TcpServerAcceptor implements DynamicMetricsProvider {
    private static final long SHUTDOWN_TIMEOUT_MILLIS = SECONDS.toMillis(10);
    private static final long SELECT_TIMEOUT_MILLIS = SECONDS.toMillis(60);
    private static final int SELECT_IDLE_COUNT_THRESHOLD = 10;

    private final ServerSocketRegistry registry;
    private final TcpServer server;
    private final ILogger logger;
    private final ServerContext serverContext;
    @Probe(name = TCP_METRIC_ACCEPTOR_EVENT_COUNT, level = DEBUG)
    private final SwCounter eventCount = newSwCounter();
    @Probe(name = TCP_METRIC_ACCEPTOR_EXCEPTION_COUNT, level = DEBUG)
    private final SwCounter exceptionCount = newSwCounter();
    // count number of times the selector was recreated (if selectWorkaround is enabled)
    @Probe(name = TCP_METRIC_ACCEPTOR_SELECTOR_RECREATE_COUNT, level = DEBUG)
    private final SwCounter selectorRecreateCount = newSwCounter();
    private final AcceptorIOThread acceptorThread;
    // last time select returned
    private volatile long lastSelectTimeMs;

    // When true, enables workaround for bug occurring when SelectorImpl.select returns immediately
    // with no channels selected, resulting in 100% CPU usage while doing no progress.
    // See issue: https://github.com/hazelcast/hazelcast/issues/7943
    private final boolean selectorWorkaround = (SelectorMode.getConfiguredValue() == SELECT_WITH_FIX);

    private volatile boolean stop;
    private volatile Selector selector;

    private final Set<SelectionKey> selectionKeys = newSetFromMap(new ConcurrentHashMap<>());

    TcpServerAcceptor(ServerSocketRegistry registry, TcpServer server, ServerContext serverContext) {
        this.registry = registry;
        this.server = server;
        this.serverContext = server.getContext();
        this.logger = serverContext.getLoggingService().getLogger(getClass());
        this.acceptorThread = new AcceptorIOThread();
    }

    /**
     * A probe that measure how long this {@link TcpServerAcceptor} has not received any events.
     *
     * @return the idle time in ms.
     */
    @Probe(name = TCP_METRIC_ACCEPTOR_IDLE_TIME_MILLIS, unit = MS, level = DEBUG)
    private long idleTimeMillis() {
        return max(currentTimeMillis() - lastSelectTimeMs, 0);
    }

    public TcpServerAcceptor start() {
        acceptorThread.start();
        return this;
    }

    public synchronized void shutdown() {
        if (stop) {
            return;
        }

        logger.finest("Shutting down SocketAcceptor thread.");
        stop = true;
        Selector sel = selector;
        if (sel != null) {
            sel.wakeup();
        }

        try {
            acceptorThread.join(SHUTDOWN_TIMEOUT_MILLIS);
        } catch (InterruptedException e) {
            currentThread().interrupt();
            logger.finest(e);
        }
    }

    @Override
    public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext context) {
        context.collect(descriptor.withPrefix(TCP_PREFIX_ACCEPTOR)
                .withDiscriminator(TCP_DISCRIMINATOR_THREAD, acceptorThread.getName()), this);
    }

    private final class AcceptorIOThread extends Thread {

        private AcceptorIOThread() {
            super(createThreadPoolName(serverContext.getHazelcastName(), "IO") + "Acceptor");
        }

        @Override
        public void run() {
            if (logger.isFinestEnabled()) {
                logger.finest("Starting TcpIpAcceptor on " + registry);
            }

            try {
                selector = Selector.open();
                for (ServerSocketRegistry.Pair entry : registry) {
                    ServerSocketChannel serverSocketChannel = entry.getChannel();

                    serverSocketChannel.configureBlocking(false);
                    SelectionKey selectionKey = serverSocketChannel.register(selector, OP_ACCEPT);
                    selectionKey.attach(entry);
                    selectionKeys.add(selectionKey);
                }
                if (selectorWorkaround) {
                    acceptLoopWithSelectorFix();
                } else {
                    acceptLoop();
                }
            } catch (OutOfMemoryError e) {
                OutOfMemoryErrorDispatcher.onOutOfMemory(e);
            } catch (Throwable e) {
                logger.severe(e.getClass().getName() + ": " + e.getMessage(), e);
            } finally {
                closeResource(selector);
            }
        }

        private void acceptLoop() throws IOException {
            while (!stop) {
                // block until new connection or interruption.
                int keyCount = selector.select();
                if (isInterrupted()) {
                    break;
                }
                if (keyCount == 0) {
                    continue;
                }
                Iterator<SelectionKey> it = selector.selectedKeys().iterator();
                handleSelectionKeys(it);
            }
        }

        private void acceptLoopWithSelectorFix() throws IOException {
            int idleCount = 0;
            while (!stop) {
                // block with a timeout until new connection or interruption.
                long before = currentTimeMillis();
                int keyCount = selector.select(SELECT_TIMEOUT_MILLIS);
                if (isInterrupted()) {
                    break;
                }
                if (keyCount == 0) {
                    long selectTimeTaken = currentTimeMillis() - before;
                    idleCount = selectTimeTaken < SELECT_TIMEOUT_MILLIS ? idleCount + 1 : 0;
                    // select unblocked before timing out with no keys selected --> bug detected
                    if (idleCount > SELECT_IDLE_COUNT_THRESHOLD) {
                        // rebuild the selector
                        rebuildSelector();
                        idleCount = 0;
                    }
                    continue;
                }
                idleCount = 0;
                Iterator<SelectionKey> it = selector.selectedKeys().iterator();
                handleSelectionKeys(it);
            }
        }

        private void rebuildSelector() throws IOException {
            selectorRecreateCount.inc();
            // cancel existing selection key, register new one on the new selector
            for (SelectionKey key : selectionKeys) {
                key.cancel();
            }
            selectionKeys.clear();
            closeResource(selector);
            Selector newSelector = Selector.open();
            selector = newSelector;
            for (ServerSocketRegistry.Pair entry : registry) {
                ServerSocketChannel serverSocketChannel = entry.getChannel();

                SelectionKey selectionKey = serverSocketChannel.register(newSelector, OP_ACCEPT);
                selectionKey.attach(entry);
                selectionKeys.add(selectionKey);
            }
        }

        private void handleSelectionKeys(Iterator<SelectionKey> it) {
            lastSelectTimeMs = currentTimeMillis();
            while (it.hasNext()) {
                SelectionKey sk = it.next();
                it.remove();
                // of course it is acceptable!
                if (sk.isValid() && sk.isAcceptable()) {
                    eventCount.inc();
                    ServerSocketRegistry.Pair attachment = (ServerSocketRegistry.Pair) sk.attachment();
                    ServerSocketChannel serverSocketChannel = attachment.getChannel();
                    try {
                        SocketChannel socketChannel = serverSocketChannel.accept();
                        if (socketChannel == null) {
                            continue;
                        }
                        newConnection(attachment.getQualifier(), socketChannel);
                    } catch (Exception e) {
                        handleAcceptException(serverSocketChannel, e);
                    }
                }
            }
        }

        private void handleAcceptException(ServerSocketChannel serverSocketChannel, Exception e) {
            exceptionCount.inc();

            if (e instanceof ClosedChannelException && !server.isLive()) {
                // ClosedChannelException
                // or AsynchronousCloseException
                // or ClosedByInterruptException
                logger.finest("Terminating socket acceptor thread...", e);
            } else {
                logger.severe("Unexpected error while accepting connection! "
                        + e.getClass().getName() + ": " + e.getMessage());
                try {
                    serverSocketChannel.close();
                } catch (Exception ex) {
                    logger.finest("Closing server socket failed", ex);
                }
                serverContext.onFatalError(e);
            }
        }

        private void newConnection(final EndpointQualifier qualifier, SocketChannel socketChannel) throws IOException {
            TcpServerConnectionManager connectionManager = server.getConnectionManager(qualifier);
            Channel channel = connectionManager.newChannel(socketChannel, false);

            if (logger.isFineEnabled()) {
                logger.fine("Accepting socket connection from " + channel.socket().getRemoteSocketAddress());
            }
            serverContext.getAuditLogService()
                .eventBuilder(AuditlogTypeIds.NETWORK_CONNECT)
                .message("New connection accepted.")
                .addParameter("qualifier", qualifier)
                .addParameter("remoteAddress", socketChannel.getRemoteAddress())
                .log();
            if (serverContext.isSocketInterceptorEnabled(qualifier)) {
                serverContext.submitAsync(() -> newConnection0(connectionManager, channel));
            } else {
                newConnection0(connectionManager, channel);
            }
        }

        private void newConnection0(TcpServerConnectionManager connectionManager, Channel channel) {
            try {
                serverContext.interceptSocket(connectionManager.getEndpointQualifier(), channel.socket(), true);
                connectionManager.newConnection(channel, null, true);
            } catch (Exception e) {
                exceptionCount.inc();
                logger.warning(e.getClass().getName() + ": " + e.getMessage(), e);
                closeResource(channel);
            }
        }
    }
}
