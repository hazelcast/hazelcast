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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.OperationTracingService;
import com.hazelcast.spi.impl.operationexecutor.OperationExecutor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static com.hazelcast.nio.Packet.HEADER_OP;
import static com.hazelcast.nio.Packet.HEADER_OP_HEARTBEAT;
import static com.hazelcast.nio.Packet.HEADER_URGENT;
import static com.hazelcast.util.EmptyStatement.ignore;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * The OperationHeartbeatReporter is a mechanism that prevents operations from getting stuck indefinitely if something goes
 * wrong while sending, executing or returning the result.
 *
 * The mechanism contains 2 parts:
 * - on the executing side each member will send periodically a packet with all calls id's of operations that are running.
 * - on the invoking side each member check if it has received a signal that the operation is still running. If no signal
 * is received for a certain amount of time, the operation is aborted with an operation timeout exception.
 *
 * The actual monitoring is done in the InvocationRegistry. The only thing the OperationHeartbeatReporter does is for every callId
 * it calls the {@link InvocationRegistry#notifyStillRunning(long)}.
 */
class OperationHeartbeatReporter {

    // todo: we need to come up with a smart default
    private static final int DELAY_MS = 1000;

    private final SerializationService serializationService;
    private final InvocationRegistry invocationRegistry;
    private final OperationExecutor operationExecutor;
    private final NodeEngineImpl nodeEngine;
    private final ScanThread scanThread;
    private final BlockingQueue<Object> queue = new LinkedBlockingQueue<Object>();
    private final ILogger logger;

    OperationHeartbeatReporter(NodeEngineImpl nodeEngine,
                               HazelcastThreadGroup hazelcastThreadGroup,
                               SerializationService serializationService,
                               InvocationRegistry invocationRegistry,
                               OperationExecutor operationExecutor,
                               ILogger logger) {
        this.nodeEngine = nodeEngine;
        this.logger = logger;
        this.serializationService = serializationService;
        this.invocationRegistry = invocationRegistry;
        this.operationExecutor = operationExecutor;
        this.scanThread = new ScanThread(hazelcastThreadGroup);
        this.scanThread.start();
    }

//    private int delayMs() {
//        // by default every 15 seconds an update is send to all members.
//        GroupProperties groupProperties = nodeEngine.getNode().getGroupProperties();
//        int callTimeoutMs = groupProperties.getInteger(GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS);
//        return callTimeoutMs / 4;
//    }

    public void shutdown() {
        scanThread.shutdown();
    }

    public void handle(Packet packet) {
        queue.offer(packet);
    }

    private final class ScanThread extends Thread {
        private volatile boolean stop;

        public ScanThread(HazelcastThreadGroup hzThreadGroup) {
            super(hzThreadGroup.getInternalThreadGroup(),
                    hzThreadGroup.getThreadNamePrefix("OperationDeadMansSwitchThread"));
        }

        public void shutdown() {
            stop = true;
            interrupt();
        }

        @Override
        public void run() {
            try {
                while (!stop) {
                    receiveCallIds();
                    sendCallIds();
                }
            } catch (InterruptedException e) {
                ignore(e);
            } catch (Throwable t) {
                logger.severe(t);
            }

            logger.finest(getName() + " has completed");
        }

        private void receiveCallIds() throws InterruptedException {
            long remainMs = DELAY_MS;

            do {
                long startMs = currentTimeMillis();
                Object task = queue.poll(remainMs, MILLISECONDS);
                remainMs -= currentTimeMillis() - startMs;

                if (task != null) {
                    long[] callIds;
                    if (task instanceof Packet) {
                        callIds = serializationService.toObject(task);
                    } else {
                        callIds = (long[]) task;
                    }

                    for (Long callId : callIds) {
                        invocationRegistry.notifyStillRunning(callId);
                    }
                }
            } while (remainMs > 0);
        }

        private void sendCallIds() {
            Map<Address, List<Long>> results = scan();

            ConnectionManager connectionManager = nodeEngine.getNode().getConnectionManager();
            for (Map.Entry<Address, List<Long>> entry : results.entrySet()) {
                Address address = entry.getKey();
                if (address == null) {
                    // it can be that address is null; some operations are directly send without an invocation
                    continue;
                }

                List<Long> callIdList = entry.getValue();

                long[] callIdArray = new long[callIdList.size()];
                for (int k = 0; k < callIdArray.length; k++) {
                    callIdArray[k] = callIdList.get(k);
                }

                if (address.equals(nodeEngine.getThisAddress())) {
                    queue.offer(callIdArray);
                } else {
                    Packet packet = new Packet(serializationService.toBytes(callIdArray))
                            .setHeader(HEADER_OP)
                            .setHeader(HEADER_OP_HEARTBEAT)
                            .setHeader(HEADER_URGENT);
                    Connection connection = connectionManager.getOrConnect(address);
                    if (connection != null) {
                        connection.write(packet);
                    }
                }
            }
        }

        private Map<Address, List<Long>> scan() {
            // this map can be recycled. Also instead of using a list with Long, we could use an array backed data-structure.
            // which also can be recycled.
            Map<Address, List<Long>> results = new HashMap<Address, List<Long>>();

            // todo: in the future it would be nice if we don't need to explicitly deal with executor or wait-notify service.
            // would be simpler if we can ask for any service implementing OperationTracingService.

            operationExecutor.scan(results);

            for (OperationTracingService tracingService : nodeEngine.getServices(OperationTracingService.class)) {
                tracingService.scan(results);
            }

            nodeEngine.getWaitNotifyService().scan(results);

            return results;
        }
    }
}
