/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.connection.tcp;

import com.hazelcast.client.config.ClientIcmpPingConfig;
import com.hazelcast.client.impl.connection.ClientConnection;
import com.hazelcast.client.impl.spi.impl.ClientExecutionServiceImpl;
import com.hazelcast.internal.cluster.fd.PingFailureDetector;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.ConnectionListener;
import com.hazelcast.internal.util.ICMPHelper;

import java.io.IOException;
import java.net.ConnectException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static com.hazelcast.internal.util.EmptyStatement.ignore;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Client icmp based ping manager
 * Responsible for configuration handling and
 * scheduling related tasks
 */
public class ClientICMPManager implements ConnectionListener {

    private static final long MIN_ICMP_INTERVAL_MILLIS = SECONDS.toMillis(1);
    private final ClientExecutionServiceImpl clientExecutionService;
    private final TcpClientConnectionManager clientConnectionManager;
    private final HeartbeatManager heartbeatManager;
    private final ILogger logger;
    private final PingFailureDetector<Connection> icmpFailureDetector;
    private final boolean icmpEnabled;
    private final int icmpTtl;
    private final int icmpTimeoutMillis;
    private final int icmpIntervalMillis;

    public ClientICMPManager(ClientIcmpPingConfig clientIcmpPingConfig, ClientExecutionServiceImpl clientExecutionService,
                             LoggingService loggingService, TcpClientConnectionManager clientConnectionManager,
                             HeartbeatManager heartbeatManager) {
        this.clientExecutionService = clientExecutionService;
        this.clientConnectionManager = clientConnectionManager;
        this.heartbeatManager = heartbeatManager;
        this.logger = loggingService.getLogger(ClientICMPManager.class);
        this.icmpTtl = clientIcmpPingConfig.getTtl();
        this.icmpTimeoutMillis = clientIcmpPingConfig.getTimeoutMilliseconds();
        this.icmpIntervalMillis = clientIcmpPingConfig.getIntervalMilliseconds();
        this.icmpEnabled = clientIcmpPingConfig.isEnabled();
        int icmpMaxAttempts = clientIcmpPingConfig.getMaxAttempts();

        if (icmpTimeoutMillis > icmpIntervalMillis) {
            throw new IllegalStateException("ICMP timeout is set to a value greater than the ICMP interval, "
                    + "this is not allowed.");
        }

        if (icmpIntervalMillis < MIN_ICMP_INTERVAL_MILLIS) {
            throw new IllegalStateException("ICMP interval is set to a value less than the min allowed, "
                    + MIN_ICMP_INTERVAL_MILLIS + "ms");
        }

        if (icmpEnabled) {
            if (clientIcmpPingConfig.isEchoFailFastOnStartup()) {
                echoFailFast();
            }
            this.icmpFailureDetector = new PingFailureDetector<>(icmpMaxAttempts);
        } else {
            this.icmpFailureDetector = null;
        }
    }

    private void echoFailFast() {
        logger.info("Checking that ICMP failure-detector is permitted. Attempting to create a raw-socket using JNI.");

        if (!ICMPHelper.isRawSocketPermitted()) {
            throw new IllegalStateException("ICMP failure-detector can't be used in this environment. "
                    + "Check Hazelcast Documentation Chapter on the Ping Failure Detector for supported platforms "
                    + "and how to enable this capability for your operating system");
        }
        logger.info("ICMP failure-detector is supported, enabling.");
    }

    public void start() {
        if (!icmpEnabled) {
            return;
        }

        clientConnectionManager.addConnectionListener(this);
        clientExecutionService.scheduleWithRepetition(new Runnable() {
            public void run() {
                for (final ClientConnection connection : clientConnectionManager.getActiveConnections()) {
                    try {
                        clientExecutionService.execute(new PeriodicPingTask(connection));
                    } catch (Throwable e) {
                        logger.severe(e);
                    }
                }
            }
        }, icmpIntervalMillis, icmpIntervalMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void connectionAdded(Connection connection) {

    }

    @Override
    public void connectionRemoved(Connection connection) {
        if (icmpEnabled) {
            icmpFailureDetector.remove(connection);
        }
    }

    public void shutdown() {
        if (icmpEnabled) {
            icmpFailureDetector.reset();
        }
    }

    private class PeriodicPingTask implements Runnable {

        final ClientConnection connection;

        PeriodicPingTask(ClientConnection connection) {
            this.connection = connection;
        }

        boolean doPing(Address address, Level level)
                throws IOException {
            try {
                if (address.getInetAddress().isReachable(null, icmpTtl, icmpTimeoutMillis)) {
                    String msg = format("%s is pinged successfully", address);
                    logger.log(level, msg);
                    return true;
                }
            } catch (ConnectException ignored) {
                // no route to host, means we cannot connect anymore
                ignore(ignored);
            }
            return false;
        }

        public void run() {
            try {
                Address address = connection.getRemoteAddress();
                logger.fine(format("will ping %s", address));
                if (doPing(address, Level.FINE)) {
                    icmpFailureDetector.heartbeat(connection);
                    return;
                }

                icmpFailureDetector.logAttempt(connection);

                // host not reachable
                String reason = format("Could not ping %s", address);
                logger.warning(reason);

                if (!icmpFailureDetector.isAlive(connection)) {
                    heartbeatManager.onHeartbeatStopped(connection, "ICMP ping time out");
                }
            } catch (Throwable ignored) {
                ignore(ignored);
            } finally {
                //because ping and connection removal runs concurrently,
                //it could be the case that we created an entry for a dead connection.
                //if connection closed while we are pinging, remove the related entry
                if (!connection.isAlive()) {
                    icmpFailureDetector.remove(connection);
                }
            }
        }
    }
}
