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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.StaticMetricsProvider;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.services.CanCancelOperations;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationexecutor.OperationHostileThread;
import com.hazelcast.spi.impl.operationservice.CallsPerMember;
import com.hazelcast.spi.impl.operationservice.LiveOperationsTracker;
import com.hazelcast.spi.impl.operationservice.OperationControl;
import com.hazelcast.spi.impl.servicemanager.ServiceManager;
import com.hazelcast.spi.properties.HazelcastProperties;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.logging.Level;

import static com.hazelcast.instance.EndpointQualifier.MEMBER;
import static com.hazelcast.instance.impl.OutOfMemoryErrorDispatcher.inspectOutOfMemoryError;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OPERATION_METRIC_INVOCATION_MONITOR_BACKUP_TIMEOUTS;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OPERATION_METRIC_INVOCATION_MONITOR_BACKUP_TIMEOUT_MILLIS;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OPERATION_METRIC_INVOCATION_MONITOR_DELAYED_EXECUTION_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OPERATION_METRIC_INVOCATION_MONITOR_HEARTBEAT_BROADCAST_PERIOD_MILLIS;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OPERATION_METRIC_INVOCATION_MONITOR_HEARTBEAT_PACKETS_RECEIVED;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OPERATION_METRIC_INVOCATION_MONITOR_HEARTBEAT_PACKETS_SENT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OPERATION_METRIC_INVOCATION_MONITOR_INVOCATION_SCAN_PERIOD_MILLIS;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OPERATION_METRIC_INVOCATION_MONITOR_INVOCATION_TIMEOUT_MILLIS;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OPERATION_METRIC_INVOCATION_MONITOR_NORMAL_TIMEOUTS;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OPERATION_PREFIX_INVOCATIONS;
import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.internal.metrics.ProbeUnit.MS;
import static com.hazelcast.internal.nio.Packet.FLAG_OP_CONTROL;
import static com.hazelcast.internal.nio.Packet.FLAG_URGENT;
import static com.hazelcast.internal.util.ThreadUtil.createThreadName;
import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;
import static com.hazelcast.spi.properties.ClusterProperty.OPERATION_BACKUP_TIMEOUT_MILLIS;
import static com.hazelcast.spi.properties.ClusterProperty.OPERATION_CALL_TIMEOUT_MILLIS;
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
public class InvocationMonitor implements Consumer<Packet>, StaticMetricsProvider {

    private static final int HEARTBEAT_CALL_TIMEOUT_RATIO = 4;
    private static final long MAX_DELAY_MILLIS = SECONDS.toMillis(10);

    private final NodeEngineImpl nodeEngine;
    private final InternalSerializationService serializationService;
    private final ServiceManager serviceManager;
    private final InvocationRegistry invocationRegistry;
    private final ILogger logger;
    private final ScheduledExecutorService scheduler;
    private final Address thisAddress;
    private final ConcurrentMap<Address, AtomicLong> heartbeatPerMember = new ConcurrentHashMap<>();

    @Probe(name = OPERATION_METRIC_INVOCATION_MONITOR_BACKUP_TIMEOUTS, level = MANDATORY)
    private final SwCounter backupTimeoutsCount = newSwCounter();
    @Probe(name = OPERATION_METRIC_INVOCATION_MONITOR_NORMAL_TIMEOUTS, level = MANDATORY)
    private final SwCounter normalTimeoutsCount = newSwCounter();
    @Probe(name = OPERATION_METRIC_INVOCATION_MONITOR_HEARTBEAT_PACKETS_RECEIVED)
    private final SwCounter heartbeatPacketsReceived = newSwCounter();
    @Probe(name = OPERATION_METRIC_INVOCATION_MONITOR_HEARTBEAT_PACKETS_SENT)
    private final SwCounter heartbeatPacketsSent = newSwCounter();
    @Probe(name = OPERATION_METRIC_INVOCATION_MONITOR_DELAYED_EXECUTION_COUNT)
    private final SwCounter delayedExecutionCount = newSwCounter();
    @Probe(name = OPERATION_METRIC_INVOCATION_MONITOR_BACKUP_TIMEOUT_MILLIS, unit = MS)
    private final long backupTimeoutMillis;
    @Probe(name = OPERATION_METRIC_INVOCATION_MONITOR_INVOCATION_TIMEOUT_MILLIS, unit = MS)
    private final long invocationTimeoutMillis;
    @Probe(name = OPERATION_METRIC_INVOCATION_MONITOR_HEARTBEAT_BROADCAST_PERIOD_MILLIS, unit = MS)
    private final long heartbeatBroadcastPeriodMillis;
    @Probe(name = OPERATION_METRIC_INVOCATION_MONITOR_INVOCATION_SCAN_PERIOD_MILLIS, unit = MS)
    private final long invocationScanPeriodMillis = SECONDS.toMillis(1);

    //todo: we need to get rid of the nodeEngine dependency
    InvocationMonitor(NodeEngineImpl nodeEngine,
                      Address thisAddress,
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
        this.scheduler = newScheduler(nodeEngine.getHazelcastInstance().getName());
    }

    // Only accessed by diagnostics.
    public ConcurrentMap<Address, AtomicLong> getHeartbeatPerMember() {
        return heartbeatPerMember;
    }

    // Only accessed by diagnostics.
    public long getHeartbeatBroadcastPeriodMillis() {
        return heartbeatBroadcastPeriodMillis;
    }

    @Override
    public void provideStaticMetrics(MetricsRegistry registry) {
        registry.registerStaticMetrics(this, OPERATION_PREFIX_INVOCATIONS);
    }

    private static ScheduledExecutorService newScheduler(final String hzName) {
        // the scheduler is configured with a single thread; so prevent concurrency problems.
        return new ScheduledThreadPoolExecutor(1, r -> new InvocationMonitorThread(r, hzName));
    }

    private long invocationTimeoutMillis(HazelcastProperties properties) {
        long invocationTimeoutMillis = properties.getMillis(OPERATION_CALL_TIMEOUT_MILLIS);
        if (logger.isFinestEnabled()) {
            logger.finest("Operation invocation timeout is " + invocationTimeoutMillis + " ms");
        }

        return invocationTimeoutMillis;
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
        // Member list version at the time of member removal. Since version is read after member removal,
        // this is guaranteed to be greater than version in invocations whose target was left member.
        int memberListVersion = nodeEngine.getClusterService().getMemberListVersion();
        // postpone notifying invocations since real response may arrive in the meantime.
        scheduler.execute(new OnMemberLeftTask(member, memberListVersion));
    }

    /**
     * Cleans up heartbeats and fails invocations for the given endpoint.
     *
     * @param endpoint the endpoint that has left
     */
    void onEndpointLeft(Address endpoint) {
        scheduler.execute(new OnEndpointLeftTask(endpoint));
    }

    void execute(Runnable runnable) {
        scheduler.execute(runnable);
    }

    void schedule(Runnable command, long delayMillis) {
        scheduler.schedule(command, delayMillis, MILLISECONDS);
    }

    @Override
    public void accept(Packet packet) {
        scheduler.execute(new ProcessOperationControlTask(packet));
    }

    public void start() {
        MonitorInvocationsTask monitorInvocationsTask = new MonitorInvocationsTask(invocationScanPeriodMillis);
        scheduler.scheduleAtFixedRate(
                monitorInvocationsTask, 0, monitorInvocationsTask.periodMillis, MILLISECONDS);

        BroadcastOperationControlTask broadcastOperationControlTask
                = new BroadcastOperationControlTask(heartbeatBroadcastPeriodMillis);
        scheduler.scheduleAtFixedRate(
                broadcastOperationControlTask, 0, broadcastOperationControlTask.periodMillis, MILLISECONDS);
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

        AtomicLong heartbeat = heartbeatPerMember.get(memberAddress);
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

            for (Invocation inv : invocationRegistry) {
                invocationCount++;
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

    /**
     * Task for cleaning up heartbeats and failing invocations for an endpoint which has left.
     */
    private final class OnEndpointLeftTask extends MonitorTask {
        private final Address endpoint;

        private OnEndpointLeftTask(Address endpoint) {
            this.endpoint = endpoint;
        }

        @Override
        public void run0() {
            heartbeatPerMember.remove(endpoint);

            for (Invocation invocation : invocationRegistry) {
                if (endpoint.equals(invocation.getTargetAddress())) {
                    invocation.notifyError(new MemberLeftException("Endpoint " + endpoint + " has left"));
                }
            }
        }
    }

    private final class OnMemberLeftTask extends MonitorTask {
        private final MemberImpl leftMember;
        private final int memberListVersion;

        private OnMemberLeftTask(MemberImpl leftMember, int memberListVersion) {
            this.leftMember = leftMember;
            this.memberListVersion = memberListVersion;
        }

        @Override
        public void run0() {
            heartbeatPerMember.remove(leftMember.getAddress());

            for (Invocation invocation : invocationRegistry) {
                if (hasTargetLeft(invocation)) {
                    onTargetLoss(invocation);
                } else {
                    onPotentialBackupLoss(invocation);
                }
            }
        }

        private boolean hasTargetLeft(Invocation invocation) {
            Member targetMember = invocation.getTargetMember();
            if (targetMember == null) {
                Address invTarget = invocation.getTargetAddress();
                return leftMember.getAddress().equals(invTarget);
            } else {
                return leftMember.getUuid().equals(targetMember.getUuid());
            }
        }

        private void onTargetLoss(Invocation invocation) {
            // Notify only if invocation's target is left member and invocation's member-list-version
            // is lower than member-list-version at time of member removal.
            //
            // Comparison of invocation's target and left member is done using member UUID.
            // Normally Hazelcast does not support crash-recover, a left member cannot rejoin
            // with the same UUID. Hence UUID comparison is enough.
            //
            // But Hot-Restart breaks this limitation and when Hot-Restart is enabled a member
            // can restore its UUID and it's allowed to rejoin when cluster state is FROZEN or PASSIVE.
            //
            // That's why another ordering property is needed. Invocation keeps member-list-version before
            // operation is submitted to the target. If a member restarts with the same identity (UUID),
            // by comparing member-list-version during member removal with the invocation's member-list-version
            // we can determine whether invocation is submitted before member left or after restart.
            if (invocation.getMemberListVersion() < memberListVersion) {
                invocation.notifyError(new MemberLeftException(leftMember));
            }
        }

        private void onPotentialBackupLoss(Invocation invocation) {
            invocation.notifyBackupComplete();
        }
    }

    private final class ProcessOperationControlTask extends MonitorTask {
        // either OperationControl or Packet that contains it
        private final Object payload;
        private final Address sender;

        ProcessOperationControlTask(OperationControl payload) {
            this.payload = payload;
            this.sender = thisAddress;
        }

        ProcessOperationControlTask(Packet payload) {
            this.payload = payload;
            this.sender = payload.getConn().getRemoteAddress();
        }

        @Override
        public void run0() {
            heartbeatPacketsReceived.inc();
            long nowMillis = Clock.currentTimeMillis();
            updateMemberHeartbeat(nowMillis);
            final OperationControl opControl = serializationService.toObject(payload);
            for (long callId : opControl.runningOperations()) {
                updateHeartbeat(callId, nowMillis);
            }
            for (CanCancelOperations service : serviceManager.getServices(CanCancelOperations.class)) {
                final long[] opsToCancel = opControl.operationsToCancel();
                for (int i = 0; i < opsToCancel.length; i++) {
                    if (opsToCancel[i] != -1 && service.cancelOperation(sender, opsToCancel[i])) {
                        opsToCancel[i] = -1;
                    }
                }
            }
        }

        private void updateMemberHeartbeat(long nowMillis) {
            AtomicLong heartbeat = heartbeatPerMember.get(sender);
            if (heartbeat == null) {
                heartbeat = new AtomicLong(nowMillis);
                heartbeatPerMember.put(sender, heartbeat);
                return;
            }

            heartbeat.set(nowMillis);
        }

        private void updateHeartbeat(long callId, long nowMillis) {
            Invocation invocation = invocationRegistry.get(callId);
            if (invocation == null) {
                // the invocation doesn't exist anymore, so we are done.
                return;
            }

            invocation.lastHeartbeatMillis = nowMillis;
        }
    }

    private final class BroadcastOperationControlTask extends FixedRateMonitorTask {
        private final CallsPerMember calls = new CallsPerMember(thisAddress);

        private BroadcastOperationControlTask(long periodMillis) {
            super(periodMillis);
        }

        @Override
        public void run0() {
            CallsPerMember calls = populate();
            Set<Address> addresses = calls.addresses();
            if (logger.isFinestEnabled()) {
                logger.finest("Broadcasting operation control packets to: " + addresses.size() + " members");
            }
            for (Address address : addresses) {
                sendOpControlPacket(address, calls.toOpControl(address));
            }
        }

        private CallsPerMember populate() {
            calls.clear();

            ClusterService clusterService = nodeEngine.getClusterService();
            calls.ensureMember(thisAddress);
            for (Member member : clusterService.getMembers()) {
                calls.ensureMember(member.getAddress());
            }
            for (LiveOperationsTracker tracker : serviceManager.getServices(LiveOperationsTracker.class)) {
                tracker.populate(calls);
            }
            for (Invocation invocation : invocationRegistry) {
                if (invocation.future.isCancelled()) {
                    calls.addOpToCancel(invocation.getTargetAddress(), invocation.op.getCallId());
                }
            }
            return calls;
        }

        private void sendOpControlPacket(Address address, OperationControl opControl) {
            heartbeatPacketsSent.inc();

            if (address.equals(thisAddress)) {
                scheduler.execute(new ProcessOperationControlTask(opControl));
            } else {
                Packet packet = new Packet(serializationService.toBytes(opControl))
                        .setPacketType(Packet.Type.OPERATION)
                        .raiseFlags(FLAG_OP_CONTROL | FLAG_URGENT);
                nodeEngine.getNode().getServer().getConnectionManager(MEMBER).transmit(packet, address);
            }
        }
    }

    /**
     * This class needs to implement the {@link OperationHostileThread} interface to make sure that the OperationExecutor
     * is not going to schedule any operations on this thread due to retry.
     */
    private static final class InvocationMonitorThread extends Thread implements OperationHostileThread {
        private InvocationMonitorThread(Runnable task, String hzName) {
            super(task, createThreadName(hzName, "InvocationMonitorThread"));
        }
    }
}
