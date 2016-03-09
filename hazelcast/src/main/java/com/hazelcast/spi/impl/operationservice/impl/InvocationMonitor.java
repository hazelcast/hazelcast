/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.MemberLeftException;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.OperationTracingService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.PacketHandler;
import com.hazelcast.spi.impl.operationexecutor.OperationHostileThread;
import com.hazelcast.spi.impl.servicemanager.ServiceManager;
import com.hazelcast.util.Clock;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.logging.Level;

import static com.hazelcast.instance.GroupProperty.OPERATION_BACKUP_TIMEOUT_MILLIS;
import static com.hazelcast.instance.GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS;
import static com.hazelcast.instance.GroupProperty.SLOW_INVOCATION_DETECTOR_THRESHOLD_MILLIS;
import static com.hazelcast.instance.OutOfMemoryErrorDispatcher.inspectOutputMemoryError;
import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;
import static com.hazelcast.nio.Packet.FLAG_OP;
import static com.hazelcast.nio.Packet.FLAG_OP_CONTROL;
import static com.hazelcast.nio.Packet.FLAG_URGENT;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.logging.Level.FINE;
import static java.util.logging.Level.INFO;

/**
 * The InvocationMonitor monitors all pending invocations and determines if there are any problems like timeouts. It uses the
 * {@link InvocationRegistry} to access the pending invocations.
 * <p/>
 * An experimental feature to support debugging is the slow invocation detector. So it can log any invocation that takes
 * more than x seconds. See {@link GroupProperty#SLOW_INVOCATION_DETECTOR_THRESHOLD_MILLIS} for more information.
 */
public class InvocationMonitor implements PacketHandler {

    private static final long ON_MEMBER_LEFT_DELAY_MILLIS = 1111;
    private static final int HEARTBEAT_CALL_TIMEOUT_RATIO = 4;

    private final NodeEngineImpl nodeEngine;
    private final InternalSerializationService serializationService;
    private final ServiceManager serviceManager;
    private final InvocationRegistry invocationRegistry;
    private final ILogger logger;
    private final ScheduledExecutorService scheduler;
    @Probe(name = "backupTimeouts", level = MANDATORY)
    private final SwCounter backupTimeoutsCount = newSwCounter();
    @Probe(name = "normalTimeouts", level = MANDATORY)
    private final SwCounter normalTimeoutsCount = newSwCounter();
    @Probe
    private final long backupTimeoutMillis;
    @Probe
    private final long invocationTimeoutMillis;
    @Probe
    private final long slowInvocationThresholdMillis;
    @Probe
    private final long heartbeatBroadcastPeriodMillis;
    @Probe
    private final long invocationScanPeriodMillis = SECONDS.toMillis(1);
    @Probe
    private volatile long heartbeatPacketsReceived;
    @Probe
    private volatile long heartbeatPacketsSend;

    public InvocationMonitor(InvocationRegistry invocationRegistry, ILogger logger, NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.serializationService = (InternalSerializationService) nodeEngine.getSerializationService();
        this.serviceManager = nodeEngine.getServiceManager();
        this.invocationRegistry = invocationRegistry;
        this.logger = logger;

        this.backupTimeoutMillis = nodeEngine.getGroupProperties().getMillis(OPERATION_BACKUP_TIMEOUT_MILLIS);
        this.invocationTimeoutMillis = invocationTimeoutMillis();
        this.heartbeatBroadcastPeriodMillis = heartbeatBroadcastPeriodMillis();
        this.slowInvocationThresholdMillis = slowInvocationThresholdMillis();

        this.scheduler = newScheduler();

        nodeEngine.getMetricsRegistry().scanAndRegister(this, "operation.invocations");
    }

    private ScheduledExecutorService newScheduler() {
        // the scheduler is configured with a single thread.
        ScheduledExecutorService scheduler = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new InvocationMonitorThread(r, nodeEngine.getNode().getHazelcastThreadGroup());
                thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                    @Override
                    public void uncaughtException(Thread t, Throwable e) {
                        logger.severe(e);
                    }
                });
                return thread;
            }
        });
        scheduler.scheduleAtFixedRate(new MonitorInvocationsTask(), 0, invocationScanPeriodMillis, MILLISECONDS);
        scheduler.scheduleAtFixedRate(new BroadcastOperationHeartbeatsTask(), 0, heartbeatBroadcastPeriodMillis, MILLISECONDS);
        return scheduler;
    }

    private long invocationTimeoutMillis() {
        // the heartbeat timeout is 2x the call timeout. This is comparable to the timeout mechanism used in 3.6

        GroupProperties props = nodeEngine.getGroupProperties();
        long heartbeatTimeoutMillis = 2 * props.getMillis(OPERATION_CALL_TIMEOUT_MILLIS);

        if (logger.isFinestEnabled()) {
            logger.finest("Operation invocation timeout is " + heartbeatTimeoutMillis + " ms");
        }

        return heartbeatTimeoutMillis;
    }

    private long heartbeatBroadcastPeriodMillis() {
        // The heartbeat period is configured to be 1/4 of the call timeout. So with default settings, every 15 seconds,
        // every member in the cluster, will notify every other member in the cluster about all calls that are pending.
        // This is done quite efficiently; imagine at any given moment one node has 1000 concurrent calls pending on another
        // node; then every 15 seconds an 8 KByte packet is send to the other member.
        int callTimeoutMs = nodeEngine.getGroupProperties().getInteger(OPERATION_CALL_TIMEOUT_MILLIS);
        long periodMs = Math.max(SECONDS.toMillis(1), callTimeoutMs / HEARTBEAT_CALL_TIMEOUT_RATIO);

        if (logger.isFinestEnabled()) {
            logger.finest("Operation heartbeat period is " + periodMs + " ms");
        }

        return periodMs;
    }

    private long slowInvocationThresholdMillis() {
        GroupProperties props = nodeEngine.getGroupProperties();
        long thresholdMs = props.getMillis(SLOW_INVOCATION_DETECTOR_THRESHOLD_MILLIS);
        if (thresholdMs > -1) {
            logger.info("Slow invocation detector enabled, using threshold: " + thresholdMs + " ms");
        }
        return thresholdMs;
    }

    public void shutdown() {
        scheduler.shutdown();
    }

    public void awaitTermination(long timeoutMillis) throws InterruptedException {
        scheduler.awaitTermination(timeoutMillis, MILLISECONDS);
    }

    public void onMemberLeft(MemberImpl member) {
        // postpone notifying invocations since real response may arrive in the mean time.
        scheduler.schedule(new OnMemberLeftTask(member), ON_MEMBER_LEFT_DELAY_MILLIS, MILLISECONDS);
    }

    @Override
    public void handle(Packet packet) {
        scheduler.execute(new ProcessOperationHeartbeatsTask(packet));
    }

    /**
     * The MonitorTask iterates over all pending invocations and sees what needs to be done
     * <p/>
     * But it should also check if a 'is still running' check needs to be done. This removed complexity from
     * the invocation.waitForResponse which is too complicated too understand.
     * <p/>
     */
    private final class MonitorInvocationsTask implements Runnable {

        @Override
        public void run() {
            if (logger.isFinestEnabled()) {
                logger.finest("Scanning all invocations");
            }

            if (invocationRegistry.size() == 0) {
                return;
            }

            long now = Clock.currentTimeMillis();
            int backupTimeouts = 0;
            int normalTimeouts = 0;
            int invocationCount = 0;

            Set<Map.Entry<Long, Invocation>> invocations = invocationRegistry.entrySet();
            Iterator<Map.Entry<Long, Invocation>> iterator = invocations.iterator();
            while (iterator.hasNext()) {
                invocationCount++;
                Map.Entry<Long, Invocation> entry = iterator.next();
                Long callId = entry.getKey();
                Invocation inv = entry.getValue();

                if (duplicate(inv, callId, iterator)) {
                    continue;
                }

                detectSlowInvocation(now, inv);

                try {
                    if (inv.detectAndHandleTimeout(invocationTimeoutMillis)) {
                        normalTimeouts++;
                    } else if (inv.detectAndHandleBackupTimeout(backupTimeoutMillis)) {
                        backupTimeouts++;
                    }
                } catch (Throwable t) {
                    inspectOutputMemoryError(t);
                    logger.severe("Failed to check invocation:" + inv, t);
                }
            }

            backupTimeoutsCount.inc(backupTimeouts);
            normalTimeoutsCount.inc(normalTimeouts);
            log(invocationCount, backupTimeouts, normalTimeouts);
        }

        /**
         * The reason for the following if check is a workaround for the problem explained below.
         *
         * Problematic scenario :
         * If an invocation with callId 1 is retried twice for any reason,
         * two new innovations created and registered to invocation registry with callId’s 2 and 3 respectively.
         * Both new invocations are sharing the same operation
         * When one of the new invocations, say the one with callId 2 finishes, it de-registers itself from the
         * invocation registry.
         * When doing the de-registration it sets the shared operation’s callId to 0.
         * After that when the invocation with the callId 3 completes, it tries to de-register itself from
         * invocation registry
         * but fails to do so since the invocation callId and the callId on the operation is not matching anymore
         * When InvocationMonitor thread kicks in, it sees that there is an invocation in the registry,
         * and asks whether invocation is finished or not.
         * Even if the remote node replies with invocation is timed out,
         * It can’t be de-registered from the registry because of aforementioned non-matching callId scenario.
         *
         * Workaround:
         * When InvocationMonitor kicks in, it will do a check for invocations that are completed
         * but their callId's are not matching with their operations. If any invocation found for that type,
         * it is removed from the invocation registry.
         */
        private boolean duplicate(Invocation inv, long callId, Iterator iterator) {
            if (callId != inv.op.getCallId() && inv.future.isDone()) {
                iterator.remove();
                return true;
            }

            return false;
        }

        private void detectSlowInvocation(long now, Invocation invocation) {
            if (slowInvocationThresholdMillis > 0) {
                long durationMs = now - invocation.op.getInvocationTime();
                if (durationMs > slowInvocationThresholdMillis) {
                    logger.info("Slow invocation: duration=" + durationMs + " ms, operation="
                            + invocation.op.getClass().getName() + " inv:" + invocation);
                }
            }
        }

        private void log(int invocationCount, int backupTimeouts, int invocationTimeouts) {
            Level logLevel = null;
            if (backupTimeouts > 0 || invocationTimeouts > 0) {
                logLevel = INFO;
            } else if (logger.isFineEnabled()) {
                logLevel = FINE;
            }

            if (logLevel != null) {
                logger.log(logLevel, "Invocations:" + invocationCount
                        + " timeouts:" + invocationTimeouts
                        + " backup-timeouts:" + backupTimeouts);
            }
        }
    }

    private final class OnMemberLeftTask implements Runnable {
        private final MemberImpl leftMember;

        public OnMemberLeftTask(MemberImpl leftMember) {
            this.leftMember = leftMember;
        }

        @Override
        public void run() {
            for (Invocation invocation : invocationRegistry) {
                if (hasMemberLeft(invocation)) {
                    invocation.notifyError(new MemberLeftException(leftMember));
                }
            }
        }

        private boolean hasMemberLeft(Invocation invocation) {
            MemberImpl targetMember = invocation.targetMember;
            if (targetMember == null) {
                Address invTarget = invocation.invTarget;
                return leftMember.getAddress().equals(invTarget);
            } else {
                return leftMember.getUuid().equals(targetMember.getUuid());
            }
        }
    }

    private final class ProcessOperationHeartbeatsTask implements Runnable {
        // either a packet or a long-array.
        private Object callIds;

        public ProcessOperationHeartbeatsTask(Object callIds) {
            this.callIds = callIds;
        }

        @Override
        public void run() {
            heartbeatPacketsReceived++;
            for (long callId : (long[]) serializationService.toObject(callIds)) {
                invocationRegistry.updateHeartbeat(callId);
            }
        }
    }

    private final class BroadcastOperationHeartbeatsTask implements Runnable {
        @Override
        public void run() {
            if (logger.isFinestEnabled()) {
                logger.finest("Broadcasting operation heartbeats");
            }

            Map<Address, List<Long>> results = scan();

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

                sendHeartbeats(address, callIdArray);
            }
        }

        private void sendHeartbeats(Address address, long[] callIds) {
            heartbeatPacketsSend++;

            if (address.equals(nodeEngine.getThisAddress())) {
                scheduler.execute(new ProcessOperationHeartbeatsTask(callIds));
            } else {
                Packet packet = new Packet(serializationService.toBytes(callIds))
                        .setAllFlags(FLAG_OP | FLAG_OP_CONTROL | FLAG_URGENT);
                nodeEngine.getNode().getConnectionManager().transmit(packet, address);
            }
        }

        private Map<Address, List<Long>> scan() {

            // this map can be recycled. Also instead of using a list with Long, we could use an array backed data-structure.
            // which also can be recycled.
            Map<Address, List<Long>> results = new HashMap<Address, List<Long>>();

            for (OperationTracingService tracingService : serviceManager.getServices(OperationTracingService.class)) {
                tracingService.scan(results);
            }

            return results;
        }
    }

    /**
     * This class needs to implement the {@link OperationHostileThread} interface to make sure that the OperationExecutor
     * is not going to schedule any operations on this thread due to retry.
     */
    private static final class InvocationMonitorThread extends Thread implements OperationHostileThread {
        private InvocationMonitorThread(Runnable task, HazelcastThreadGroup hzThreadGroup) {
            super(hzThreadGroup.getInternalThreadGroup(), task, hzThreadGroup.getThreadNamePrefix("InvocationMonitorThread"));
        }
    }
}
