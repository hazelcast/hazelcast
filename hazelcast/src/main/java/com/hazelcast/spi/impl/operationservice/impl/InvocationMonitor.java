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

import com.hazelcast.core.Member;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.metrics.MetricsProvider;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.internal.connection.Packet;
import com.hazelcast.spi.LiveOperations;
import com.hazelcast.spi.LiveOperationsTracker;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.PacketHandler;
import com.hazelcast.spi.impl.operationexecutor.OperationHostileThread;
import com.hazelcast.spi.impl.servicemanager.ServiceManager;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.util.Clock;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

import static com.hazelcast.instance.OutOfMemoryErrorDispatcher.inspectOutOfMemoryError;
import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;
import static com.hazelcast.internal.connection.Packet.FLAG_OP;
import static com.hazelcast.internal.connection.Packet.FLAG_OP_CONTROL;
import static com.hazelcast.internal.connection.Packet.FLAG_URGENT;
import static com.hazelcast.spi.properties.GroupProperty.OPERATION_BACKUP_TIMEOUT_MILLIS;
import static com.hazelcast.spi.properties.GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.logging.Level.FINE;
import static java.util.logging.Level.INFO;

/**
 * The InvocationMonitor monitors all pending invocations and determines if there are any problems like timeouts. It uses the
 * {@link InvocationRegistry} to access the pending invocations.
 *
 * The {@link InvocationMonitor} sends Operation heartbeats to the other member informing them about if the operation is still
 * alive. Also if no operations are running, it will still send a period packet to each member. This is a different system than
 * the regular heartbeats, but it has similar characteristics. The reason the packet is always send is for debugging purposes.
 */
class InvocationMonitor implements PacketHandler, MetricsProvider {

    private static final long ON_MEMBER_LEFT_DELAY_MILLIS = 1111;
    private static final int HEARTBEAT_CALL_TIMEOUT_RATIO = 4;
    private static final long MAX_DELAY_MILLIS = SECONDS.toMillis(10);

    private final NodeEngineImpl nodeEngine;
    private final InternalSerializationService serializationService;
    private final ServiceManager serviceManager;
    private final InvocationRegistry invocationRegistry;
    private final ILogger logger;
    private final ScheduledExecutorService scheduler;
    private final Address thisAddress;
    private final ConcurrentMap<Address, AtomicLong> lastHeartbeatPerMember = new ConcurrentHashMap<Address, AtomicLong>();

    @Probe(name = "backupTimeouts", level = MANDATORY)
    private final SwCounter backupTimeoutsCount = newSwCounter();
    @Probe(name = "normalTimeouts", level = MANDATORY)
    private final SwCounter normalTimeoutsCount = newSwCounter();
    @Probe
    private final SwCounter heartbeatPacketsReceived = newSwCounter();
    @Probe
    private final SwCounter heartbeatPacketsSend = newSwCounter();
    @Probe
    private final SwCounter delayedExecutionCount = newSwCounter();
    @Probe
    private final long backupTimeoutMillis;
    @Probe
    private final long invocationTimeoutMillis;
    @Probe
    private final long heartbeatBroadcastPeriodMillis;
    @Probe
    private final long invocationScanPeriodMillis = SECONDS.toMillis(1);

    //todo: we need to get rid of the nodeEngine dependency
    InvocationMonitor(NodeEngineImpl nodeEngine,
                      Address thisAddress,
                      HazelcastThreadGroup threadGroup,
                      HazelcastProperties properties,
                      InvocationRegistry invocationRegistry,
                      ILogger logger,
                      InternalSerializationService serializationService,
                      ServiceManager serviceManager) {
        this.nodeEngine = nodeEngine;
        this.thisAddress = thisAddress;
        this.serializationService = serializationService;
        this.serviceManager = serviceManager;
        this.invocationRegistry = invocationRegistry;
        this.logger = logger;
        this.backupTimeoutMillis = backupTimeoutMillis(properties);
        this.invocationTimeoutMillis = invocationTimeoutMillis(properties);
        this.heartbeatBroadcastPeriodMillis = heartbeatBroadcastPeriodMillis(properties);
        this.scheduler = newScheduler(threadGroup);
    }

    @Override
    public void provideMetrics(MetricsRegistry metricsRegistry) {
        metricsRegistry.scanAndRegister(this, "operation.invocations");
    }

    private ScheduledExecutorService newScheduler(final HazelcastThreadGroup threadGroup) {
        // the scheduler is configured with a single thread; so prevent concurrency problems.
        return new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new InvocationMonitorThread(r, threadGroup);
            }
        });
    }

    private long invocationTimeoutMillis(HazelcastProperties properties) {
        long heartbeatTimeoutMillis = properties.getMillis(OPERATION_CALL_TIMEOUT_MILLIS);
        if (logger.isFinestEnabled()) {
            logger.finest("Operation invocation timeout is " + heartbeatTimeoutMillis + " ms");
        }

        return heartbeatTimeoutMillis;
    }

    private long backupTimeoutMillis(HazelcastProperties properties) {
        long backupTimeoutMillis = properties.getMillis(OPERATION_BACKUP_TIMEOUT_MILLIS);

        if (logger.isFinestEnabled()) {
            logger.finest("Operation backup timeout is " + backupTimeoutMillis + " ms");
        }

        return backupTimeoutMillis;
    }

    private long heartbeatBroadcastPeriodMillis(HazelcastProperties properties) {
        // The heartbeat period is configured to be 1/4 of the call timeout. So with default settings, every 15 seconds,
        // every member in the cluster, will notify every other member in the cluster about all calls that are pending.
        // This is done quite efficiently; imagine at any given moment one node has 1000 concurrent calls pending on another
        // node; then every 15 seconds an 8 KByte packet is send to the other member.
        // Another advantage is that multiple heartbeat timeouts are allowed to get lost; without leading to premature
        // abortion of the invocation.
        int callTimeoutMs = properties.getInteger(OPERATION_CALL_TIMEOUT_MILLIS);
        long periodMs = Math.max(SECONDS.toMillis(1), callTimeoutMs / HEARTBEAT_CALL_TIMEOUT_RATIO);

        if (logger.isFinestEnabled()) {
            logger.finest("Operation heartbeat period is " + periodMs + " ms");
        }

        return periodMs;
    }

    void onMemberLeft(MemberImpl member) {
        // postpone notifying invocations since real response may arrive in the mean time.
        scheduler.schedule(new OnMemberLeftTask(member), ON_MEMBER_LEFT_DELAY_MILLIS, MILLISECONDS);
    }

    @Override
    public void handle(Packet packet) {
        scheduler.execute(new ProcessOperationHeartbeatsTask(packet));
    }

    public void start() {
        MonitorInvocationsTask monitorInvocationsTask = new MonitorInvocationsTask(invocationScanPeriodMillis);
        scheduler.scheduleAtFixedRate(
                monitorInvocationsTask, 0, monitorInvocationsTask.periodMillis, MILLISECONDS);

        BroadcastOperationHeartbeatsTask broadcastOperationHeartbeatsTask
                = new BroadcastOperationHeartbeatsTask(heartbeatBroadcastPeriodMillis);
        scheduler.scheduleAtFixedRate(
                broadcastOperationHeartbeatsTask, 0, broadcastOperationHeartbeatsTask.periodMillis, MILLISECONDS);
    }

    public void shutdown() {
        scheduler.shutdown();
    }

    public void awaitTermination(long timeoutMillis) throws InterruptedException {
        scheduler.awaitTermination(timeoutMillis, MILLISECONDS);
    }

    long getLastMemberHeartbeatMillis(Address memberAddress) {
        if (memberAddress == null) {
            return 0;
        }

        AtomicLong heartbeat = lastHeartbeatPerMember.get(memberAddress);
        return heartbeat == null ? 0 : heartbeat.get();
    }

    private abstract class MonitorTask implements Runnable {
        @Override
        public void run() {
            try {
                run0();
            } catch (Throwable t) {
                inspectOutOfMemoryError(t);
                logger.severe(t);
            }
        }

        protected abstract void run0();
    }

    abstract class FixedRateMonitorTask implements Runnable {
        final long periodMillis;
        private long expectedNextMillis = System.currentTimeMillis();

        FixedRateMonitorTask(long periodMillis) {
            this.periodMillis = periodMillis;
        }

        @Override
        public void run() {
            long currentTimeMillis = System.currentTimeMillis();

            try {
                if (expectedNextMillis + MAX_DELAY_MILLIS < currentTimeMillis) {
                    logger.warning(getClass().getSimpleName() + " delayed " + (currentTimeMillis - expectedNextMillis) + " ms");
                    delayedExecutionCount.inc();
                }

                run0();
            } catch (Throwable t) {
                // we want to catch the exception; if we don't and the executor runs into it, it will abort the task.
                inspectOutOfMemoryError(t);
                logger.severe(t);
            } finally {
                expectedNextMillis = currentTimeMillis + periodMillis;
            }
        }

        protected abstract void run0();
    }

    /**
     * The MonitorTask iterates over all pending invocations and sees what needs to be done. Currently its tasks are:
     * - getting rid of duplicates
     * - checking for heartbeat timeout
     * - checking for backup timeout
     *
     * In the future additional checks can be added here like checking if a retry is needed etc.
     */
    private final class MonitorInvocationsTask extends FixedRateMonitorTask {
        private MonitorInvocationsTask(long periodMillis) {
            super(periodMillis);
        }

        @Override
        public void run0() {
            if (logger.isFinestEnabled()) {
                logger.finest("Scanning all invocations");
            }

            if (invocationRegistry.size() == 0) {
                return;
            }

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

                try {
                    if (inv.detectAndHandleTimeout(invocationTimeoutMillis)) {
                        normalTimeouts++;
                    } else if (inv.detectAndHandleBackupTimeout(backupTimeoutMillis)) {
                        backupTimeouts++;
                    }
                } catch (Throwable t) {
                    inspectOutOfMemoryError(t);
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

    private final class OnMemberLeftTask extends MonitorTask {
        private final MemberImpl leftMember;

        private OnMemberLeftTask(MemberImpl leftMember) {
            this.leftMember = leftMember;
        }

        @Override
        public void run0() {
            lastHeartbeatPerMember.remove(leftMember.getAddress());

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

    private final class ProcessOperationHeartbeatsTask extends MonitorTask {
        // either a packet or a long-array.
        private Object callIds;

        private ProcessOperationHeartbeatsTask(Object callIds) {
            this.callIds = callIds;
        }

        @Override
        public void run0() {
            heartbeatPacketsReceived.inc();
            long timeMillis = Clock.currentTimeMillis();

            updateMemberHeartbeat(timeMillis);

            for (long callId : (long[]) serializationService.toObject(callIds)) {
                updateHeartbeat(callId, timeMillis);
            }
        }

        private void updateMemberHeartbeat(long timeMillis) {
            Address address = callIds instanceof Packet ? ((Packet) callIds).getConn().getEndPoint() : thisAddress;
            AtomicLong lastMemberHeartbeat = lastHeartbeatPerMember.get(address);
            if (lastMemberHeartbeat == null) {
                lastMemberHeartbeat = new AtomicLong();
                lastHeartbeatPerMember.put(address, lastMemberHeartbeat);
            }

            lastMemberHeartbeat.set(timeMillis);
        }

        private void updateHeartbeat(long callId, long timeMillis) {
            Invocation invocation = invocationRegistry.get(callId);
            if (invocation == null) {
                // the invocation doesn't exist anymore, so we are done.
                return;
            }

            invocation.lastHeartbeatMillis = timeMillis;
        }
    }

    private final class BroadcastOperationHeartbeatsTask extends FixedRateMonitorTask {
        private final LiveOperations liveOperations = new LiveOperations(thisAddress);

        private BroadcastOperationHeartbeatsTask(long periodMillis) {
            super(periodMillis);
        }

        @Override
        public void run0() {
            LiveOperations result = populate();
            Set<Address> addresses = result.addresses();

            if (logger.isFinestEnabled()) {
                logger.finest("Broadcasting operation heartbeats to: " + addresses.size() + " members");
            }

            for (Address address : addresses) {
                sendHeartbeats(address, result.callIds(address));
            }
        }

        private LiveOperations populate() {
            liveOperations.clear();

            ClusterService clusterService = nodeEngine.getClusterService();
            liveOperations.initMember(thisAddress);
            for (Member member : clusterService.getMembers()) {
                liveOperations.initMember(member.getAddress());
            }

            for (LiveOperationsTracker tracker : serviceManager.getServices(LiveOperationsTracker.class)) {
                tracker.populate(liveOperations);
            }

            return liveOperations;
        }

        private void sendHeartbeats(Address address, long[] callIds) {
            heartbeatPacketsSend.inc();

            if (address.equals(thisAddress)) {
                scheduler.execute(new ProcessOperationHeartbeatsTask(callIds));
            } else {
                Packet packet = new Packet(serializationService.toBytes(callIds))
                        .setAllFlags(FLAG_OP | FLAG_OP_CONTROL | FLAG_URGENT);
                nodeEngine.getNode().getConnectionManager().transmit(packet, address);
            }
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
