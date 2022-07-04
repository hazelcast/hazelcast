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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.config.IcmpFailureDetectorConfig;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.cluster.fd.ClusterFailureDetector;
import com.hazelcast.internal.cluster.fd.ClusterFailureDetectorType;
import com.hazelcast.internal.cluster.fd.DeadlineClusterFailureDetector;
import com.hazelcast.internal.cluster.fd.PhiAccrualClusterFailureDetector;
import com.hazelcast.internal.cluster.fd.PingFailureDetector;
import com.hazelcast.internal.cluster.impl.operations.ExplicitSuspicionOp;
import com.hazelcast.internal.cluster.impl.operations.HeartbeatComplaintOp;
import com.hazelcast.internal.cluster.impl.operations.HeartbeatOp;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.ICMPHelper;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.splitbrainprotection.impl.SplitBrainProtectionServiceImpl;

import java.io.IOException;
import java.net.ConnectException;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.logging.Level;

import static com.hazelcast.cluster.memberselector.MemberSelectors.NON_LOCAL_MEMBER_SELECTOR;
import static com.hazelcast.config.ConfigAccessor.getActiveMemberNetworkConfig;
import static com.hazelcast.instance.EndpointQualifier.MEMBER;
import static com.hazelcast.internal.cluster.impl.ClusterServiceImpl.CLUSTER_EXECUTOR_NAME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CLUSTER_METRIC_HEARTBEAT_MANAGER_LAST_HEARTBEAT;
import static com.hazelcast.internal.metrics.ProbeUnit.MS;
import static com.hazelcast.internal.util.EmptyStatement.ignore;
import static com.hazelcast.internal.util.StringUtil.timeToString;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toSet;

/**
 * ClusterHeartbeatManager manages the heartbeat sending and receiving
 * process of a node.
 * <p>
 * It periodically sends heartbeat to the other nodes and stores heartbeat timestamps
 * per node when a heartbeat is received from other nodes. If enabled and required, it can send
 * ping packets (an ICMP ping or an echo packet depending on the environment and settings).
 * <p>
 * If it detects a member is not live anymore, that member is kicked out of cluster.
 */
public class ClusterHeartbeatManager {

    private static final long CLOCK_JUMP_THRESHOLD = MINUTES.toMillis(2);
    private static final int HEART_BEAT_INTERVAL_FACTOR = 10;
    private static final int MAX_PING_RETRY_COUNT = 5;
    private static final long MIN_ICMP_INTERVAL_MILLIS = SECONDS.toMillis(1);
    private static final int DEFAULT_ICMP_TIMEOUT_MILLIS = 1000;
    private static final int DEFAULT_ICMP_INTERVAL_MILLIS = 1000;

    private final ILogger logger;
    private final Lock clusterServiceLock;
    private final Node node;
    private final NodeEngineImpl nodeEngine;
    private final ClusterServiceImpl clusterService;
    private final ClusterClockImpl clusterClock;

    private final ClusterFailureDetector heartbeatFailureDetector;
    private final PingFailureDetector<Member> icmpFailureDetector;

    private final long maxNoHeartbeatMillis;
    private final long heartbeatIntervalMillis;
    private final long legacyIcmpCheckThresholdMillis;
    private final boolean icmpEnabled;
    private final boolean icmpParallelMode;
    private final int icmpTtl;
    private final int icmpTimeoutMillis;
    private final int icmpIntervalMillis;
    private final int icmpMaxAttempts;

    @Probe(name = CLUSTER_METRIC_HEARTBEAT_MANAGER_LAST_HEARTBEAT, unit = MS)
    private volatile long lastHeartbeat;
    private volatile long lastClusterTimeDiff;

    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity", "checkstyle:executablestatementcount"})
    ClusterHeartbeatManager(Node node, ClusterServiceImpl clusterService, Lock lock) {
        this.node = node;
        this.clusterService = clusterService;
        this.nodeEngine = node.getNodeEngine();
        clusterClock = clusterService.getClusterClock();
        logger = node.getLogger(getClass());
        clusterServiceLock = lock;

        HazelcastProperties hazelcastProperties = node.getProperties();
        maxNoHeartbeatMillis = hazelcastProperties.getMillis(ClusterProperty.MAX_NO_HEARTBEAT_SECONDS);

        heartbeatIntervalMillis = getHeartbeatInterval(hazelcastProperties);
        legacyIcmpCheckThresholdMillis = heartbeatIntervalMillis * HEART_BEAT_INTERVAL_FACTOR;

        IcmpFailureDetectorConfig icmpConfig
                = getActiveMemberNetworkConfig(node.config).getIcmpFailureDetectorConfig();

        this.icmpTtl = icmpConfig != null ? icmpConfig.getTtl() : 0;
        this.icmpTimeoutMillis = icmpConfig != null ? icmpConfig.getTimeoutMilliseconds() : DEFAULT_ICMP_TIMEOUT_MILLIS;
        this.icmpIntervalMillis = icmpConfig != null ? icmpConfig.getIntervalMilliseconds() : DEFAULT_ICMP_INTERVAL_MILLIS;
        this.icmpMaxAttempts = icmpConfig != null ? icmpConfig.getMaxAttempts() : 3;
        this.icmpEnabled = icmpConfig != null && icmpConfig.isEnabled();
        this.icmpParallelMode = icmpEnabled && icmpConfig.isParallelMode();

        if (icmpTimeoutMillis > icmpIntervalMillis) {
            throw new IllegalStateException("ICMP timeout is set to a value greater than the ICMP interval, "
                    + "this is not allowed.");
        }

        if (icmpIntervalMillis < MIN_ICMP_INTERVAL_MILLIS) {
            throw new IllegalStateException("ICMP interval is set to a value less than the min allowed, "
                    + MIN_ICMP_INTERVAL_MILLIS + "ms");
        }

        this.icmpFailureDetector = createIcmpFailureDetectorIfNeeded();
        heartbeatFailureDetector = createHeartbeatFailureDetector(hazelcastProperties);
    }

    private PingFailureDetector createIcmpFailureDetectorIfNeeded() {
        IcmpFailureDetectorConfig icmpFailureDetectorConfig
                = getActiveMemberNetworkConfig(node.config).getIcmpFailureDetectorConfig();

        boolean icmpEchoFailFast = icmpFailureDetectorConfig == null || icmpFailureDetectorConfig.isFailFastOnStartup();

        if (icmpParallelMode) {
            if (icmpEchoFailFast) {
                logger.info("Checking that ICMP failure-detector is permitted. Attempting to create a raw-socket using JNI.");

                if (!ICMPHelper.isRawSocketPermitted()) {
                    throw new IllegalStateException("ICMP failure-detector can't be used in this environment. "
                            + "Check Hazelcast Documentation Chapter on the Ping Failure Detector for supported platforms "
                            + "and how to enable this capability for your operating system");
                }
                logger.info("ICMP failure-detector is supported, enabling.");
            }

            return new PingFailureDetector(icmpMaxAttempts);
        }

        return null;
    }

    private ClusterFailureDetector createHeartbeatFailureDetector(HazelcastProperties properties) {
        String type = properties.getString(ClusterProperty.HEARTBEAT_FAILURE_DETECTOR_TYPE);
        ClusterFailureDetectorType fdType = ClusterFailureDetectorType.of(type);
        switch (fdType) {
            case DEADLINE:
                return new DeadlineClusterFailureDetector(maxNoHeartbeatMillis);
            case PHI_ACCRUAL:
                int defaultValue = Integer.parseInt(ClusterProperty.MAX_NO_HEARTBEAT_SECONDS.getDefaultValue());
                if (maxNoHeartbeatMillis == TimeUnit.SECONDS.toMillis(defaultValue)) {
                    logger.warning("When using Phi-Accrual Failure Detector, please consider using a lower '"
                            + ClusterProperty.MAX_NO_HEARTBEAT_SECONDS.getName() + "' value. Current is: "
                            + defaultValue + " seconds.");
                }
                return new PhiAccrualClusterFailureDetector(maxNoHeartbeatMillis, heartbeatIntervalMillis, properties);
            default:
                throw new IllegalArgumentException("Unknown failure detector type: " + type);
        }
    }

    public long getHeartbeatIntervalMillis() {
        return heartbeatIntervalMillis;
    }

    public long getLastHeartbeatTime(Member member) {
        return heartbeatFailureDetector.lastHeartbeat(member);
    }

    private static long getHeartbeatInterval(HazelcastProperties hazelcastProperties) {
        long heartbeatInterval = hazelcastProperties.getMillis(ClusterProperty.HEARTBEAT_INTERVAL_SECONDS);
        return heartbeatInterval > 0 ? heartbeatInterval : TimeUnit.SECONDS.toMillis(1);
    }

    /**
     * Initializes the {@link ClusterHeartbeatManager}. It will schedule the
     * heartbeat operation to the {@link #getHeartbeatInterval(HazelcastProperties)} interval.
     */
    void init() {
        ExecutionService executionService = nodeEngine.getExecutionService();

        executionService.scheduleWithRepetition(CLUSTER_EXECUTOR_NAME, this::heartbeat,
                heartbeatIntervalMillis, heartbeatIntervalMillis, TimeUnit.MILLISECONDS);

        if (icmpParallelMode) {
            startPeriodicPinger();
        }
    }

    public void handleHeartbeat(MembersViewMetadata senderMembersViewMetadata, UUID receiverUuid, long timestamp,
                                Collection<MemberInfo> suspectedMembers) {
        Address senderAddress = senderMembersViewMetadata.getMemberAddress();
        try {
            long timeout = Math.min(TimeUnit.SECONDS.toMillis(1), heartbeatIntervalMillis / 2);
            if (!clusterServiceLock.tryLock(timeout, MILLISECONDS)) {
                logger.warning("Cannot handle heartbeat from " + senderAddress + ", could not acquire lock in time.");
                return;
            }
        } catch (InterruptedException e) {
            logger.warning("Cannot handle heartbeat from " + senderAddress + ", thread interrupted.");
            Thread.currentThread().interrupt();
            return;
        }

        try {
            if (!clusterService.isJoined()) {
                if (clusterService.getThisUuid().equals(receiverUuid)) {
                    logger.fine("Ignoring heartbeat of sender: " + senderMembersViewMetadata + ", because node is not joined!");
                } else {
                    // we know that sender version is 3.9 so we send explicit suspicion back even if we are not joined...
                    logger.fine("Sending explicit suspicion to " + senderAddress + " for heartbeat " + senderMembersViewMetadata
                            + ", because this node has received an invalid heartbeat before it joins to the cluster");
                    OperationService operationService = nodeEngine.getOperationService();
                    Operation op = new ExplicitSuspicionOp(senderMembersViewMetadata);
                    operationService.send(op, senderAddress);
                }
                return;
            }

            MembershipManager membershipManager = clusterService.getMembershipManager();
            MemberImpl member = membershipManager.getMember(senderAddress, senderMembersViewMetadata.getMemberUuid());
            if (member != null) {
                if (clusterService.getThisUuid().equals(receiverUuid)) {
                    if (onHeartbeat(member, timestamp)) {
                        // local timestamp is used on purpose here
                        membershipManager.handleReceivedSuspectedMembers(member, clusterClock.getClusterTime(), suspectedMembers);
                    }

                    return;
                }

                logger.warning("Local UUID mismatch on received heartbeat. local UUID: " + clusterService.getThisUuid()
                        + " received UUID: " + receiverUuid + " with " + senderMembersViewMetadata);
            }

            onInvalidHeartbeat(senderMembersViewMetadata);
        } finally {
            clusterServiceLock.unlock();
        }
    }

    private void onInvalidHeartbeat(MembersViewMetadata senderMembersViewMetadata) {
        Address senderAddress = senderMembersViewMetadata.getMemberAddress();

        if (clusterService.isMaster()) {
            if (!clusterService.getClusterJoinManager().isMastershipClaimInProgress()) {
                logger.fine("Sending explicit suspicion to " + senderAddress + " for heartbeat "
                        + senderMembersViewMetadata + ", because it is not a member of this cluster"
                        + " or its heartbeat cannot be validated!");
                clusterService.sendExplicitSuspicion(senderMembersViewMetadata);
            }
        } else {
            MemberImpl master = clusterService.getMember(clusterService.getMasterAddress());
            if (clusterService.getMembershipManager().isMemberSuspected(master)) {
                logger.fine("Not sending heartbeat complaint for " + senderMembersViewMetadata
                        + " to suspected master: " + master.getAddress());
                return;
            }

            logger.fine("Sending heartbeat complaint to master " + master.getAddress() + " for heartbeat "
                    + senderMembersViewMetadata + ", because it is not a member of this cluster"
                    + " or its heartbeat cannot be validated!");
            sendHeartbeatComplaintToMaster(senderMembersViewMetadata);
        }
    }

    private void sendHeartbeatComplaintToMaster(MembersViewMetadata senderMembersViewMetadata) {
        if (clusterService.isMaster()) {
            logger.warning("Cannot send heartbeat complaint for " + senderMembersViewMetadata + " to itself.");
            return;
        }

        Address masterAddress = clusterService.getMasterAddress();
        if (masterAddress == null) {
            logger.fine("Cannot send heartbeat complaint for " + senderMembersViewMetadata.getMemberAddress()
                    + ", master address is not set.");
            return;
        }

        MembersViewMetadata localMembersViewMetadata = clusterService.getMembershipManager().createLocalMembersViewMetadata();
        Operation op = new HeartbeatComplaintOp(localMembersViewMetadata, senderMembersViewMetadata);
        OperationService operationService = nodeEngine.getOperationService();
        operationService.send(op, masterAddress);
    }

    public void handleHeartbeatComplaint(MembersViewMetadata receiverMVMetadata, MembersViewMetadata senderMVMetadata) {
        clusterServiceLock.lock();
        try {
            if (!clusterService.isJoined()) {
                logger.warning("Ignoring heartbeat complaint of receiver: " + receiverMVMetadata
                        + " and sender: " + senderMVMetadata + " because not joined!");
                return;
            }

            MembershipManager membershipManager = clusterService.getMembershipManager();
            ClusterJoinManager clusterJoinManager = clusterService.getClusterJoinManager();

            if (!clusterService.isMaster()) {
                logger.warning("Ignoring heartbeat complaint of receiver: " + receiverMVMetadata
                        + " for sender: " + senderMVMetadata + " because this node is not master");
                return;
            } else if (clusterJoinManager.isMastershipClaimInProgress()) {
                logger.fine("Ignoring heartbeat complaint of receiver: " + receiverMVMetadata
                        + " for sender: " + senderMVMetadata + " because mastership claim process is ongoing");
                return;
            } else if (senderMVMetadata.getMemberAddress().equals(receiverMVMetadata.getMemberAddress())) {
                logger.warning("Ignoring heartbeat complaint of receiver: " + receiverMVMetadata
                        + " for sender: " + senderMVMetadata + " because they are same member");
                return;
            }

            if (membershipManager.validateMembersViewMetadata(senderMVMetadata)) {
                if (membershipManager.validateMembersViewMetadata(receiverMVMetadata)) {
                    logger.fine("Sending latest member list to " + senderMVMetadata.getMemberAddress()
                            + " and " + receiverMVMetadata.getMemberAddress() + " after heartbeat complaint.");
                    membershipManager.sendMemberListToMember(senderMVMetadata.getMemberAddress());
                    membershipManager.sendMemberListToMember(receiverMVMetadata.getMemberAddress());
                } else {
                    logger.fine("Complainer " + receiverMVMetadata.getMemberAddress() + " will explicitly suspect from "
                            + node.getThisAddress() + " and " + senderMVMetadata.getMemberAddress());
                    clusterService.sendExplicitSuspicion(receiverMVMetadata);
                    clusterService.sendExplicitSuspicionTrigger(senderMVMetadata.getMemberAddress(), receiverMVMetadata);
                }
            } else if (membershipManager.validateMembersViewMetadata(receiverMVMetadata)) {
                logger.fine("Complainee " + senderMVMetadata.getMemberAddress() + " will explicitly suspect from "
                        + node.getThisAddress() + " and " + receiverMVMetadata.getMemberAddress());
                clusterService.sendExplicitSuspicion(senderMVMetadata);
                clusterService.sendExplicitSuspicionTrigger(receiverMVMetadata.getMemberAddress(), senderMVMetadata);
            } else {
                logger.fine("Both complainer " + receiverMVMetadata.getMemberAddress()
                        + " and complainee " + senderMVMetadata.getMemberAddress()
                        + " will explicitly suspect from " + node.getThisAddress());
                clusterService.sendExplicitSuspicion(senderMVMetadata);
                clusterService.sendExplicitSuspicion(receiverMVMetadata);
            }
        } finally {
            clusterServiceLock.unlock();
        }
    }

    /**
     * Accepts the heartbeat message from {@code member} created at {@code timestamp}. The timestamp must be
     * related to the cluster clock, not the local clock. The heartbeat is ignored if the duration between
     * {@code timestamp} and the current cluster time is more than {@link ClusterProperty#MAX_NO_HEARTBEAT_SECONDS}/2.
     * If the sending node is the master, this node will also calculate and set the cluster clock diff.
     *
     * @param member    the member sending the heartbeat
     * @param timestamp the timestamp when the heartbeat was created
     * @return          true if the heartbeat is processed, false if it is ignored.
     */
    public boolean onHeartbeat(MemberImpl member, long timestamp) {
        if (member == null) {
            return false;
        }
        long clusterTime = clusterClock.getClusterTime();
        if (logger.isFineEnabled()) {
            logger.fine(format("Received heartbeat from %s (now: %s, timestamp: %s)",
                    member, timeToString(clusterTime), timeToString(timestamp)));
        }

        if (clusterTime - timestamp > maxNoHeartbeatMillis / 2) {
            logger.warning(format("Ignoring heartbeat from %s since it is expired (now: %s, timestamp: %s)", member,
                    timeToString(clusterTime), timeToString(timestamp)));
            return false;
        }

        if (isMaster(member)) {
            clusterClock.setMasterTime(timestamp);
        }
        heartbeatFailureDetector.heartbeat(member, clusterClock.getClusterTime());

        MembershipManager membershipManager = clusterService.getMembershipManager();
        membershipManager.clearMemberSuspicion(member, "Valid heartbeat");
        nodeEngine.getSplitBrainProtectionService().onHeartbeat(member, timestamp);

        return true;
    }

    /**
     * Send heartbeats and calculate clock drift. This method is expected to be called periodically because it calculates
     * the clock drift based on the expected and actual invocation period.
     */
    void heartbeat() {
        if (!clusterService.isJoined()) {
            return;
        }

        checkClockDrift(heartbeatIntervalMillis);

        final long clusterTime = clusterClock.getClusterTime();
        if (clusterService.isMaster()) {
            heartbeatWhenMaster(clusterTime);
        } else {
            heartbeatWhenSlave(clusterTime);
        }
    }

    /**
     * Checks the elapsed time from the last local heartbeat and compares it to the expected {@code intervalMillis}.
     * The method will correct a number of clocks and timestamps based on this difference:
     * <ul>
     * <li>
     * set the local cluster time diff if the absolute diff is larger than {@link #CLOCK_JUMP_THRESHOLD} and
     * the change in diff from the previous and current value is less than {@link #CLOCK_JUMP_THRESHOLD}.
     * In the case that the diff change is larger than the threshold, we assume that the current clock diff is not
     * from any local cause but that this node received a heartbeat message from the master, setting the cluster clock diff.
     * </li>
     * <li>
     * Reset the heartbeat timestamps if the absolute diff is greater or equal to
     * {@link ClusterProperty#MAX_NO_HEARTBEAT_SECONDS}/2
     * </li>
     * </ul>
     *
     * @param intervalMillis the expected elapsed interval of the cluster clock
     */
    private void checkClockDrift(long intervalMillis) {
        long now = Clock.currentTimeMillis();
        // compensate for any abrupt jumps forward in the system clock
        if (lastHeartbeat != 0L) {
            long clockJump = now - lastHeartbeat - intervalMillis;
            long absoluteClockJump = Math.abs(clockJump);

            if (absoluteClockJump > CLOCK_JUMP_THRESHOLD) {
                logger.info(format("System clock apparently jumped from %s to %s since last heartbeat (%+d ms)",
                        timeToString(lastHeartbeat), timeToString(now), clockJump));

                // We only set cluster clock, if clock jumps more than threshold.
                // If the last cluster-time diff we've seen is significantly different than what we read now,
                // that means, it's already adjusted by master heartbeat. Then don't update the cluster time again.
                long currentClusterTimeDiff = clusterClock.getClusterTimeDiff();
                if (Math.abs(lastClusterTimeDiff - currentClusterTimeDiff) < CLOCK_JUMP_THRESHOLD) {
                    // adjust cluster clock due to clock drift
                    clusterClock.setClusterTimeDiff(currentClusterTimeDiff - clockJump);
                }
            }

            if (absoluteClockJump >= maxNoHeartbeatMillis / 2) {
                logger.warning(format("Resetting heartbeat timestamps because of huge system clock jump!"
                        + " Clock-Jump: %d ms, Heartbeat-Timeout: %d ms", clockJump, maxNoHeartbeatMillis));
                resetHeartbeats();
            }
        }
        lastClusterTimeDiff = clusterClock.getClusterTimeDiff();
        lastHeartbeat = now;
    }

    /**
     * Sends heartbeat to each of the cluster members.
     * Checks whether a member has failed to send a heartbeat in time
     * (see {@link #maxNoHeartbeatMillis})
     * and removes that member from the cluster.
     * <p></p>
     * This method is only called on the master member.
     *
     * @param now the current cluster clock time
     */
    private void heartbeatWhenMaster(long now) {
        for (Member member : clusterService.getMembers(NON_LOCAL_MEMBER_SELECTOR)) {
            try {
                logIfConnectionToEndpointIsMissing(now, member);
                if (suspectMemberIfNotHeartBeating(now, member)) {
                    continue;
                }

                pingMemberIfRequired(now, member);
                sendHeartbeat(member);
            } catch (Throwable t) {
                logger.severe(t);
            }
        }

        clusterService.getMembershipManager().checkPartialDisconnectivity(now);
    }

    /**
     * Removes the {@code member} if it has not sent any heartbeats in {@link ClusterProperty#MAX_NO_HEARTBEAT_SECONDS}.
     * If it has not sent any heartbeats in {@link #HEART_BEAT_INTERVAL_FACTOR} heartbeat intervals, it will log a warning.
     *
     * @param now    the current cluster clock time
     * @param member the member which needs to be checked
     * @return if the member has been removed
     */
    private boolean suspectMemberIfNotHeartBeating(long now, Member member) {
        if (clusterService.getMembershipManager().isMemberSuspected((MemberImpl) member)) {
            return true;
        }

        long lastHeartbeat = heartbeatFailureDetector.lastHeartbeat(member);
        if (!heartbeatFailureDetector.isAlive(member, now)) {
            double suspicionLevel = heartbeatFailureDetector.suspicionLevel(member, now);
            String reason = format("Suspecting %s because it has not sent any heartbeats since %s."
                            + " Now: %s, heartbeat timeout: %d ms, suspicion level: %.2f",
                    member, timeToString(lastHeartbeat), timeToString(now), maxNoHeartbeatMillis, suspicionLevel);
            logger.warning(reason);
            clusterService.suspectMember(member, reason, true);
            return true;
        }
        if (logger.isFineEnabled() && (now - lastHeartbeat) > heartbeatIntervalMillis * HEART_BEAT_INTERVAL_FACTOR) {
            double suspicionLevel = heartbeatFailureDetector.suspicionLevel(member, now);
            logger.fine(format("Not receiving any heartbeats from %s since %s, suspicion level: %.2f",
                    member, timeToString(lastHeartbeat), suspicionLevel));
        }
        return false;
    }

    /**
     * Sends heartbeat to each of the cluster members.
     * Checks whether the master member has failed to send a heartbeat (see {@link #maxNoHeartbeatMillis})
     * and removes that master member from cluster, if it fails on heartbeat.
     * <p></p>
     * This method is called on NON-master members.
     */
    private void heartbeatWhenSlave(long now) {
        MembershipManager membershipManager = clusterService.getMembershipManager();

        for (Member member : clusterService.getMembers(NON_LOCAL_MEMBER_SELECTOR)) {
            try {
                logIfConnectionToEndpointIsMissing(now, member);

                if (suspectMemberIfNotHeartBeating(now, member)) {
                    continue;
                }

                if (membershipManager.isMemberSuspected((MemberImpl) member)) {
                    continue;
                }

                pingMemberIfRequired(now, member);
                sendHeartbeat(member);
            } catch (Throwable e) {
                logger.severe(e);
            }
        }
    }

    private boolean isMaster(MemberImpl member) {
        return member.getAddress().equals(clusterService.getMasterAddress());
    }

    /**
     * Pings the {@code member} if ICMP is enabled and more than {@link #HEART_BEAT_INTERVAL_FACTOR}
     * heartbeats have passed.
     */
    private void pingMemberIfRequired(long now, Member member) {
        if (!icmpEnabled || icmpParallelMode) {
            return;
        }

        long lastHeartbeat = heartbeatFailureDetector.lastHeartbeat(member);
        if ((now - lastHeartbeat) >= legacyIcmpCheckThresholdMillis) {
            runPingTask(member);
        }
    }

    private void startPeriodicPinger() {
        nodeEngine.getExecutionService().scheduleWithRepetition(CLUSTER_EXECUTOR_NAME, () -> {
            Collection<Member> members = clusterService.getMembers(NON_LOCAL_MEMBER_SELECTOR);

            for (Member member : members) {
                try {
                    runPingTask(member);
                } catch (Throwable e) {
                    logger.severe(e);
                }
            }
        }, icmpIntervalMillis, icmpIntervalMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * Tries to ping the {@code member} and removes the member if it is unreachable.
     *
     * @param member the member for which we need to determine reachability
     */
    private void runPingTask(final Member member) {
        nodeEngine.getExecutionService().execute(ExecutionService.SYSTEM_EXECUTOR,
                icmpParallelMode ? new PeriodicPingTask(member) : new PingTask(member));
    }

    /**
     * Send a {@link HeartbeatOp} to the {@code target}
     *
     * @param target target Member
     */
    private void sendHeartbeat(Member target) {
        if (target == null) {
            return;
        }
        try {
            MembersViewMetadata membersViewMetadata = clusterService.getMembershipManager().createLocalMembersViewMetadata();
            Collection<MemberInfo> suspectedMembers = Collections.emptySet();
            if (clusterService.getMembershipManager().isPartialDisconnectionDetectionEnabled()
                    && !clusterService.isMaster() && target.getAddress().equals(clusterService.getMasterAddress())) {
                suspectedMembers = clusterService.getMembershipManager()
                                                 .getSuspectedMembers()
                                                 .stream()
                                                 .map(MemberInfo::new)
                                                 .collect(toSet());
            }

            long clusterTime = clusterClock.getClusterTime();
            Operation op = new HeartbeatOp(membersViewMetadata, target.getUuid(), clusterTime, suspectedMembers);
            op.setCallerUuid(clusterService.getThisUuid());
            node.nodeEngine.getOperationService().send(op, target.getAddress());
        } catch (Exception e) {
            if (logger.isFineEnabled()) {
                logger.fine(format("Error while sending heartbeat -> %s[%s]", e.getClass().getName(), e.getMessage()));
            }
        }
    }

    /**
     * Logs a warning if the {@code member} hasn't sent a heartbeat in {@link #HEART_BEAT_INTERVAL_FACTOR} heartbeat
     * intervals and there is no live connection to the member
     */
    private void logIfConnectionToEndpointIsMissing(long now, Member member) {
        long heartbeatTime = heartbeatFailureDetector.lastHeartbeat(member);
        if ((now - heartbeatTime) >= heartbeatIntervalMillis * HEART_BEAT_INTERVAL_FACTOR) {
            Connection conn = node.getServer().getConnectionManager(MEMBER).getOrConnect(member.getAddress());
            if (conn == null || !conn.isAlive()) {
                logger.warning("This node does not have a connection to " + member);
            }
        }
    }

    /** Reset all heartbeats to the current cluster time. Called when system clock jump is detected. */
    private void resetHeartbeats() {
        SplitBrainProtectionServiceImpl splitBrainProtectionService = nodeEngine.getSplitBrainProtectionService();
        long now = clusterClock.getClusterTime();
        for (MemberImpl member : clusterService.getMemberImpls()) {
            heartbeatFailureDetector.heartbeat(member, now);
            splitBrainProtectionService.onHeartbeat(member, now);
        }
    }

    /** Remove the {@code member}'s heartbeat timestamps */
    void removeMember(MemberImpl member) {
        heartbeatFailureDetector.remove(member);
        if (icmpParallelMode) {
            icmpFailureDetector.remove(member);
        }
    }

    void reset() {
        heartbeatFailureDetector.reset();
        if (icmpParallelMode) {
            icmpFailureDetector.reset();
        }
    }

    private class PingTask
            implements Runnable {

        final Member member;

        PingTask(Member member) {
            this.member = member;
        }

        public void run() {
            Address address = member.getAddress();
            logger.warning(format("%s will ping %s", node.getThisAddress(), address));
            for (int i = 0; i < MAX_PING_RETRY_COUNT; i++) {
                if (doPing(address, Level.INFO)) {
                    return;
                }
            }

            // host not reachable
            String reason = format("%s could not ping %s", node.getThisAddress(), address);
            logger.warning(reason);
            clusterService.suspectMember(member, reason, true);
        }

        boolean doPing(Address address, Level level) {
            try {
                if (address.getInetAddress().isReachable(null, icmpTtl, icmpTimeoutMillis)) {
                    String msg = format("%s pinged %s successfully", node.getThisAddress(), address);
                    logger.log(level, msg);
                    return true;
                }
            } catch (ConnectException ignored) {
                // no route to host, means we cannot connect anymore
                ignore(ignored);
            } catch (IOException e) {
                if (logger.isFinestEnabled()) {
                    logger.finest("Failed while pinging " + address, e);
                }
            }
            return false;
        }
    }

    private class PeriodicPingTask
            extends PingTask {
        final SplitBrainProtectionServiceImpl splitBrainProtectionService = nodeEngine.getSplitBrainProtectionService();

        PeriodicPingTask(Member member) {
            super(member);
        }

        public void run() {
            Address address = member.getAddress();
            logger.fine(format("%s will ping %s", node.getThisAddress(), address));
            if (doPing(address, Level.FINE)) {
                boolean pingRestored = (icmpFailureDetector.heartbeat(member) > 0);
                if (pingRestored) {
                    splitBrainProtectionService.onPingRestored(member);
                }
                return;
            }

            icmpFailureDetector.logAttempt(member);
            splitBrainProtectionService.onPingLost(member);

            // host not reachable
            String reason = format("%s could not ping %s", node.getThisAddress(), address);
            logger.warning(reason);

            if (!icmpFailureDetector.isAlive(member)) {
                clusterService.suspectMember(member, reason, true);
            }
        }
    }
}
