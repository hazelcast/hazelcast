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

package com.hazelcast.client.impl.connection.tcp;

import com.hazelcast.client.config.ClientIcmpPingConfig;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.cluster.fd.PingFailureDetector;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.util.ICMPHelper;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.hazelcast.spi.impl.executionservice.TaskScheduler;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.util.EmptyStatement.ignore;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Client icmp based ping manager
 * Periodically at each `icmpIntervalMillis` checks if the address is reachable.
 * If the address can not be reached for `icmpTimeoutMillis` at each attempt, counts that as a failure attempt.
 * After the configured count `icmpMaxAttempts`, closes the connection with {@link TargetDisconnectedException}
 */
public final class ClientICMPManager {

    private static final long MIN_ICMP_INTERVAL_MILLIS = SECONDS.toMillis(1);

    private ClientICMPManager() {
    }

    public static void start(ClientIcmpPingConfig clientIcmpPingConfig,
                             TaskScheduler taskScheduler,
                             ILogger logger,
                             Collection<Connection> unmodifiableActiveConnections) {
        if (!clientIcmpPingConfig.isEnabled()) {
            return;
        }

        if (clientIcmpPingConfig.isEchoFailFastOnStartup()) {
            echoFailFast(logger);
        }

        int icmpTtl = clientIcmpPingConfig.getTtl();
        int icmpTimeoutMillis = clientIcmpPingConfig.getTimeoutMilliseconds();
        int icmpIntervalMillis = clientIcmpPingConfig.getIntervalMilliseconds();
        int icmpMaxAttempts = clientIcmpPingConfig.getMaxAttempts();

        if (icmpTimeoutMillis > icmpIntervalMillis) {
            throw new IllegalStateException("ICMP timeout is set to a value greater than the ICMP interval, "
                    + "this is not allowed.");
        }

        if (icmpIntervalMillis < MIN_ICMP_INTERVAL_MILLIS) {
            throw new IllegalStateException("ICMP interval is set to a value less than the min allowed, "
                    + MIN_ICMP_INTERVAL_MILLIS + "ms");
        }

        PingFailureDetector<Connection> failureDetector = new PingFailureDetector<>(icmpMaxAttempts);
        taskScheduler.scheduleWithRepetition(() -> {
            failureDetector.retainAttemptsForAliveEndpoints(unmodifiableActiveConnections);
            for (Connection connection : unmodifiableActiveConnections) {
                // we don't want an isReachable call to an address stopping us to check other addresses.
                // so we run each check in its own thread
                taskScheduler.execute(() -> ping(logger, failureDetector, connection, icmpTtl, icmpTimeoutMillis));
            }
        }, icmpIntervalMillis, icmpIntervalMillis, TimeUnit.MILLISECONDS);
    }

    private static void echoFailFast(ILogger logger) {
        logger.info("Checking that ICMP failure-detector is permitted. Attempting to create a raw-socket using JNI.");

        if (!ICMPHelper.isRawSocketPermitted()) {
            throw new IllegalStateException("ICMP failure-detector can't be used in this environment. "
                    + "Check Hazelcast Documentation Chapter on the Ping Failure Detector for supported platforms "
                    + "and how to enable this capability for your operating system");
        }
        logger.info("ICMP failure-detector is supported, enabling.");
    }

    private static boolean isReachable(ILogger logger, int icmpTtl, int icmpTimeoutMillis, Address address) {
        try {
            if (address.getInetAddress().isReachable(null, icmpTtl, icmpTimeoutMillis)) {
                logger.fine(format("%s is pinged successfully", address));
                return true;
            }
        } catch (IOException ignored) {
            // no route to host, means we cannot connect anymore
            ignore(ignored);
        }
        return false;
    }

    private static void ping(ILogger logger, PingFailureDetector<Connection> failureDetector, Connection connection,
                             int icmpTtl, int icmpTimeoutMillis) {
        Address address = connection.getRemoteAddress();
        logger.fine(format("will ping %s", address));
        if (isReachable(logger, icmpTtl, icmpTimeoutMillis, address)) {
            failureDetector.heartbeat(connection);
            return;
        }
        failureDetector.logAttempt(connection);
        logger.warning(format("Could not ping %s", address));
        if (!failureDetector.isAlive(connection)) {
            connection.close("ICMP ping time out",
                    new TargetDisconnectedException("ICMP ping time out to connection " + connection));
        }
    }
}
