/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.nio;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.logging.Level;

class ConnectionMonitor {

    final static ILogger logger = Logger.getLogger(ConnectionMonitor.class.getName());

    final ConnectionManager connectionManager;
    final IOService ioService;
    final Address endPoint;
    final long minInterval;
    final int maxFaults;
    int faults = 0;
    long lastFaultTime = 0L;

    public ConnectionMonitor(ConnectionManager connectionManager, Address endPoint) {
        super();
        this.connectionManager = connectionManager;
        this.endPoint = endPoint;
        this.ioService = connectionManager.getIOHandler();
        this.minInterval = ioService.getConnectionMonitorInterval();
        this.maxFaults = ioService.getConnectionMonitorMaxFaults();
    }

    public Address getEndPoint() {
        return endPoint;
    }

    public synchronized void onError(Throwable t) {
        logger.log(Level.FINEST, "An error occured on connection to " + endPoint + getCauseDescription(t));
        final long now = System.currentTimeMillis();
        final long last = lastFaultTime;
        if (now - last > minInterval) {
            if ((++faults) >= maxFaults) {
                logger.log(Level.WARNING, "Removing connection to endpoint " + endPoint + getCauseDescription(t));
                ioService.removeEndpoint(endPoint);
            }
            lastFaultTime = now;
        }
    }

    public synchronized void reset() {
        logger.log(Level.FINEST, "Resetting connection monitor for endpoint " + endPoint);
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
        return s.append(", Error-Count: ").append(faults).toString();
    }
}
