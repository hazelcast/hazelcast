/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.tcp;

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.IOService;
import com.hazelcast.util.Clock;

public class TcpIpConnectionMonitor {

    final ILogger logger;
    final TcpIpConnectionManager connectionManager;
    final IOService ioService;
    final Address endPoint;
    final long minInterval;
    final int maxFaults;
    int faults;
    long lastFaultTime;

    public TcpIpConnectionMonitor(TcpIpConnectionManager connectionManager, Address endPoint) {
        this.connectionManager = connectionManager;
        this.endPoint = endPoint;
        this.ioService = connectionManager.getIOHandler();
        this.minInterval = ioService.getConnectionMonitorInterval();
        this.maxFaults = ioService.getConnectionMonitorMaxFaults();
        this.logger = ioService.getLogger(getClass().getName());
    }

    public Address getEndPoint() {
        return endPoint;
    }

    public synchronized void onError(Throwable t) {
        String errorMessage = "An error occurred on connection to " + endPoint + getCauseDescription(t);
        logger.finest(errorMessage);
        ioService.getSystemLogService().logConnection(errorMessage);
        final long now = Clock.currentTimeMillis();
        final long last = lastFaultTime;
        if (now - last > minInterval) {
            if (faults++ >= maxFaults) {
                String removeEndpointMessage = "Removing connection to endpoint " + endPoint + getCauseDescription(t);
                logger.warning(removeEndpointMessage);
                ioService.getSystemLogService().logConnection(removeEndpointMessage);
                ioService.removeEndpoint(endPoint);
            }
            lastFaultTime = now;
        }
    }

    public synchronized void reset() {
        String resetMessage = "Resetting connection monitor for endpoint " + endPoint;
        logger.finest(resetMessage);
        ioService.getSystemLogService().logConnection(resetMessage);
        faults = 0;
        lastFaultTime = 0L;
    }

    private /*synchronized*/ String getCauseDescription(Throwable t) {
        StringBuilder s = new StringBuilder(" Cause => ");
        if (t != null) {
            s.append(t.getClass().getName()).append(" {").append(t.getMessage()).append("}");
        } else {
            s.append("Unknown");
        }
        return s.append(", Error-Count: ").append(faults + 1).toString();
    }
}
