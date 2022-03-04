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

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.server.ServerContext;
import com.hazelcast.logging.ILogger;

import static java.lang.System.currentTimeMillis;

class TcpServerConnectionErrorHandler {

    private final ILogger logger;
    private final ServerContext serverContext;
    private final Address endpointAddress;
    private final long minInterval;
    private final int maxFaults;
    private int faults;
    private long lastFaultTime;

    TcpServerConnectionErrorHandler(ServerContext serverContext, Address endpointAddress) {
        this.endpointAddress = endpointAddress;
        this.serverContext = serverContext;
        this.minInterval = serverContext.getConnectionMonitorInterval();
        this.maxFaults = serverContext.getConnectionMonitorMaxFaults();
        this.logger = serverContext.getLoggingService().getLogger(getClass());
    }

    synchronized void onError(Throwable cause) {
        String errorMessage = "An error occurred on connection to " + endpointAddress + getCauseDescription(cause);
        logger.finest(errorMessage);
        final long now = currentTimeMillis();
        final long last = lastFaultTime;
        if (now - last > minInterval) {
            if (faults++ >= maxFaults) {
                String removeEndpointMessage = "Removing connection to endpoint " + endpointAddress + getCauseDescription(cause);
                logger.warning(removeEndpointMessage);
                serverContext.removeEndpoint(endpointAddress);
            }
            lastFaultTime = now;
        }
    }

    synchronized TcpServerConnectionErrorHandler reset() {
        String resetMessage = "Resetting connection monitor for endpoint " + endpointAddress;
        logger.finest(resetMessage);
        faults = 0;
        lastFaultTime = 0L;
        return this;
    }

    private String getCauseDescription(Throwable cause) {
        StringBuilder s = new StringBuilder(" Cause => ");
        if (cause != null) {
            s.append(cause.getClass().getName()).append(" {").append(cause.getMessage()).append("}");
        } else {
            s.append("Unknown");
        }
        return s.append(", Error-Count: ").append(faults + 1).toString();
    }
}
