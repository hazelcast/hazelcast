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

package com.hazelcast.internal.nio.tcp;

import com.hazelcast.logging.ILogger;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.IOService;

import static java.lang.System.currentTimeMillis;

public class TcpIpConnectionErrorHandler {

    private final ILogger logger;
    private final IOService ioService;
    private final Address endPoint;
    private final long minInterval;
    private final int maxFaults;
    private int faults;
    private long lastFaultTime;

    TcpIpConnectionErrorHandler(TcpIpEndpointManager endpointManager, Address endPoint) {
        this.endPoint = endPoint;
        this.ioService = endpointManager.getNetworkingService().getIoService();
        this.minInterval = ioService.getConnectionMonitorInterval();
        this.maxFaults = ioService.getConnectionMonitorMaxFaults();
        this.logger = ioService.getLoggingService().getLogger(getClass());
    }

    public Address getEndPoint() {
        return endPoint;
    }

    public synchronized void onError(Throwable t) {
        String errorMessage = "An error occurred on connection to " + endPoint + getCauseDescription(t);
        logger.finest(errorMessage);
        final long now = currentTimeMillis();
        final long last = lastFaultTime;
        if (now - last > minInterval) {
            if (faults++ >= maxFaults) {
                String removeEndpointMessage = "Removing connection to endpoint " + endPoint + getCauseDescription(t);
                logger.warning(removeEndpointMessage);
                ioService.removeEndpoint(endPoint);
            }
            lastFaultTime = now;
        }
    }

    public synchronized void reset() {
        String resetMessage = "Resetting connection monitor for endpoint " + endPoint;
        logger.finest(resetMessage);
        faults = 0;
        lastFaultTime = 0L;
    }

    private String getCauseDescription(Throwable t) {
        StringBuilder s = new StringBuilder(" Cause => ");
        if (t != null) {
            s.append(t.getClass().getName()).append(" {").append(t.getMessage()).append("}");
        } else {
            s.append("Unknown");
        }
        return s.append(", Error-Count: ").append(faults + 1).toString();
    }
}
