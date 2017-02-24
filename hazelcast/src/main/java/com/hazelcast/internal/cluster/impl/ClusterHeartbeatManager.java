/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeState;
import com.hazelcast.internal.cluster.impl.operations.HeartbeatOperation;
import com.hazelcast.internal.cluster.impl.operations.MasterConfirmationOperation;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.util.Clock;
import com.hazelcast.util.EmptyStatement;

import java.net.ConnectException;
import java.net.NetworkInterface;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.cluster.impl.ClusterServiceImpl.EXECUTOR_NAME;
import static com.hazelcast.util.StringUtil.timeToString;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * ClusterHeartbeatManager manages the heartbeat sending and receiving
 * process of a node.
 * <p/>
 * It periodically sends heartbeat to the other nodes and stores heartbeat timestamps
 * per node when a heartbeat is received from other nodes. If enabled and required, it can send
 * ping packets (an ICMP ping or an echo packet depending on the environment and settings).
 * <p/>
 * If it detects a member is not live anymore, that member is kicked out of cluster.
 * <p/>
 * Another job of ClusterHeartbeatManager is to send (if not master node) and track (if master)
 * master-confirmation requests. Each slave node sends a master-confirmation periodically and
 * master node stores them with timestamps. A slave node which does not send master-confirmation in
 * a timeout will be kicked out of the cluster by master node.
 */
public class ClusterHeartbeatManager {

    private static final long CLOCK_JUMP_THRESHOLD = MINUTES.toMillis(2);
    private static final int HEART_BEAT_INTERVAL_FACTOR = 10;
    private static final int MAX_PING_RETRY_COUNT = 5;

    private final ILogger logger;
    private final Node node;
    private final NodeEngineImpl nodeEngine;
    private final ClusterServiceImpl clusterService;
    private final ClusterClockImpl clusterClock;

    private final ConcurrentMap<MemberImpl, Long> heartbeatTimes = new ConcurrentHashMap<MemberImpl, Long>();
    private final ConcurrentMap<MemberImpl, Long> masterConfirmationTimes = new ConcurrentHashMap<MemberImpl, Long>();

    private final long maxNoHeartbeatMillis;
    private final long maxNoMasterConfirmationMillis;
    private final long heartbeatIntervalMillis;
    private final long pingIntervalMillis;
    private final boolean icmpEnabled;
    private final int icmpTtl;
    private final int icmpTimeoutMillis;

    @Probe(name = "lastHeartbeat")
    private volatile long lastHeartbeat;
    private volatile long lastClusterTimeDiff;

    public ClusterHeartbeatManager(Node node, ClusterServiceImpl clusterService) {
        this.node = node;
        this.clusterService = clusterService;
        this.nodeEngine = node.getNodeEngine();
        clusterClock = clusterService.getClusterClock();
        logger = node.getLogger(getClass());

        HazelcastProperties hazelcastProperties = node.getProperties();
        maxNoHeartbeatMillis = hazelcastProperties.getMillis(GroupProperty.MAX_NO_HEARTBEAT_SECONDS);
        maxNoMasterConfirmationMillis = hazelcastProperties.getMillis(GroupProperty.MAX_NO_MASTER_CONFIRMATION_SECONDS);

        heartbeatIntervalMillis = getHeartbeatInterval(hazelcastProperties);
        pingIntervalMillis = heartbeatIntervalMillis * HEART_BEAT_INTERVAL_FACTOR;

        icmpEnabled = hazelcastProperties.getBoolean(GroupProperty.ICMP_ENABLED);
        icmpTtl = hazelcastProperties.getInteger(GroupProperty.ICMP_TTL);
        icmpTimeoutMillis = (int) hazelcastProperties.getMillis(GroupProperty.ICMP_TIMEOUT);
    }

    private static long getHeartbeatInterval(HazelcastProperties hazelcastProperties) {
        long heartbeatInterval = hazelcastProperties.getMillis(GroupProperty.HEARTBEAT_INTERVAL_SECONDS);
        return heartbeatInterval > 0 ? heartbeatInterval : TimeUnit.SECONDS.toMillis(1);
    }

    /**
     * Initializes the {@link ClusterHeartbeatManager}. It will schedule the :
     * <ul>
     * <li>heartbeat operation to the {@link #getHeartbeatInterval(HazelcastProperties)} interval</li>
     * <li>master confirmation to the {@link GroupProperty#MASTER_CONFIRMATION_INTERVAL_SECONDS} interval</li>
     * <li>member list publication to the {@link GroupProperty#MEMBER_LIST_PUBLISH_INTERVAL_SECONDS} interval</li>
     * </ul>
     */
    void init() {
        ExecutionService executionService = nodeEngine.getExecutionService();
        HazelcastProperties hazelcastProperties = node.getProperties();

        executionService.scheduleWithRepetition(EXECUTOR_NAME, new Runnable() {
            public void run() {
                heartbeat();
            }
        }, heartbeatIntervalMillis, heartbeatIntervalMillis, TimeUnit.MILLISECONDS);

        long masterConfirmationInterval = hazelcastProperties.getSeconds(GroupProperty.MASTER_CONFIRMATION_INTERVAL_SECONDS);
        masterConfirmationInterval = (masterConfirmationInterval > 0 ? masterConfirmationInterval : 1);
        executionService.scheduleWithRepetition(EXECUTOR_NAME, new Runnable() {
            public void run() {
                sendMasterConfirmation();
            }
        }, masterConfirmationInterval, masterConfirmationInterval, TimeUnit.SECONDS);

        long memberListPublishInterval = hazelcastProperties.getSeconds(GroupProperty.MEMBER_LIST_PUBLISH_INTERVAL_SECONDS);
        memberListPublishInterval = (memberListPublishInterval > 0 ? memberListPublishInterval : 1);
        executionService.scheduleWithRepetition(EXECUTOR_NAME, new Runnable() {
            public void run() {
                clusterService.sendMemberListToOthers();
            }
        }, memberListPublishInterval, memberListPublishInterval, TimeUnit.SECONDS);
    }

    /**
     * Accepts the heartbeat message from {@code member} created at {@code timestamp}. The timestamp must be
     * related to the cluster clock, not the local clock. The heartbeat is ignored if the duration between
     * {@code timestamp} and the current cluster time is more than {@link GroupProperty#MAX_NO_HEARTBEAT_SECONDS}/2.
     * If the sending node is the master, this node will also calculate and set the cluster clock diff.
     *
     * @param member    the member sending the heartbeat
     * @param timestamp the timestamp when the heartbeat was created
     */
    public void onHeartbeat(MemberImpl member, long timestamp) {
        if (member != null) {
            long clusterTime = clusterClock.getClusterTime();
            if (logger.isFineEnabled()) {
                logger.fine(format("Received heartbeat from %s (now: %s, timestamp: %s)",
                        member, timeToString(clusterTime), timeToString(timestamp)));
            }

            if (clusterTime - timestamp > maxNoHeartbeatMillis / 2) {
                logger.warning(format("Ignoring heartbeat from %s since it is expired (now: %s, timestamp: %s)", member,
                        timeToString(clusterTime), timeToString(timestamp)));
                return;
            }

            if (isMaster(member)) {
                clusterClock.setMasterTime(timestamp);
            }
            heartbeatTimes.put(member, clusterClock.getClusterTime());
        }
    }

    /**
     * Accepts the master confirmation message sent from cluster members to the master. The timestamp must be
     * related to the cluster clock, not the local clock. If the duration between {@code timestamp} and the current
     * cluster clock time is more than {@link GroupProperty#MAX_NO_MASTER_CONFIRMATION_SECONDS}/2 then the confirmation
     * is ignored.
     *
     * @param member    the member sending the confirmation to the master node
     * @param timestamp the cluster timestamp when the confirmation was made
     */
    public void acceptMasterConfirmation(MemberImpl member, long timestamp) {
        if (member != null) {
            if (logger.isFineEnabled()) {
                logger.fine("MasterConfirmation has been received from " + member);
            }
            long clusterTime = clusterClock.getClusterTime();
            if (clusterTime - timestamp > maxNoMasterConfirmationMillis / 2) {
                logger.warning(
                        format("Ignoring master confirmation from %s, since it is expired (now: %s, timestamp: %s)",
                                member, timeToString(clusterTime), timeToString(timestamp)));
                return;
            }
            masterConfirmationTimes.put(member, clusterTime);
        }
    }

    /**
     * Send heartbeats and calculate clock drift. This method is expected to be called periodically because it calculates
     * the clock drift based on the expected and actual invocation period.
     */
    void heartbeat() {
        if (!node.joined()) {
            return;
        }

        checkClockDrift(heartbeatIntervalMillis);

        final long clusterTime = clusterClock.getClusterTime();
        if (node.isMaster()) {
            heartbeatWhenMaster(clusterTime);
        } else {
            heartbeatWhenSlave(clusterTime);
        }
    }

    /**
     * Checks the elapsed time from the last local heartbeat and compares it to the expected {@code intervalMillis}.
     * The method will correct a number of clocks and timestamps based on this difference :
     * <ul>
     * <li>
     * set the local cluster time diff if the absolute diff is larger than {@link #CLOCK_JUMP_THRESHOLD} and
     * the change in diff from the previous and current value is less than {@link #CLOCK_JUMP_THRESHOLD}.
     * In the case that the diff change is larger than the threshold, we assume that the current clock diff is not
     * from any local cause but that this node received a heartbeat message from the master, setting the cluster clock diff.
     * </li>
     * <li>
     * reset the master confirmations if the absolute diff is greater or equal to
     * {@link GroupProperty#MAX_NO_MASTER_CONFIRMATION_SECONDS}/2
     * </li>
     * <li>
     * Reset the heartbeat timestamps if the absolute diff is greater or equal to
     * {@link GroupProperty#MAX_NO_HEARTBEAT_SECONDS}/2
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

            if (absoluteClockJump >= maxNoMasterConfirmationMillis / 2) {
                logger.warning(format("Resetting master confirmation timestamps because of huge system clock jump!"
                        + " Clock-Jump: %d ms, Master-Confirmation-Timeout: %d ms", clockJump, maxNoMasterConfirmationMillis));
                resetMemberMasterConfirmations();
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
     * Checks whether a member has failed to send a heartbeat or master-confirmation in time
     * (see {@link #maxNoHeartbeatMillis} and {@link #maxNoMasterConfirmationMillis})
     * and removes that member from the cluster.
     * <p></p>
     * This method is only called on the master member.
     *
     * @param now the current cluster clock time
     */
    private void heartbeatWhenMaster(long now) {
        Collection<MemberImpl> members = clusterService.getMemberImpls();
        for (MemberImpl member : members) {
            if (!member.localMember()) {
                try {
                    logIfConnectionToEndpointIsMissing(now, member);
                    if (removeMemberIfNotHeartBeating(now, member)) {
                        continue;
                    }

                    if (removeMemberIfMasterConfirmationExpired(now, member)) {
                        continue;
                    }

                    pingMemberIfRequired(now, member);
                    sendHeartbeat(member.getAddress());
                } catch (Throwable e) {
                    logger.severe(e);
                }
            }
        }
    }

    /**
     * Removes the {@code member} if it has not sent any heartbeats in {@link GroupProperty#MAX_NO_HEARTBEAT_SECONDS}.
     * If it has not sent any heartbeats in {@link #HEART_BEAT_INTERVAL_FACTOR} heartbeat intervals, it will log a warning.
     *
     * @param now    the current cluster clock time
     * @param member the member which needs to be checked
     * @return if the member has been removed
     */
    private boolean removeMemberIfNotHeartBeating(long now, MemberImpl member) {
        long heartbeatTime = getHeartbeatTime(member);
        if ((now - heartbeatTime) > maxNoHeartbeatMillis) {
            String reason = format("Removing %s because it has not sent any heartbeats for %d ms."
                            + " Now: %s, last heartbeat time was %s", member, maxNoHeartbeatMillis,
                    timeToString(now), timeToString(heartbeatTime));
            logger.warning(reason);
            // TODO [basri] If I am the master, I can remove the member. Otherwise, I can only suspect it because I rely on my local information
            clusterService.suspectAddress(member.getAddress(), reason, true);
            return true;
        }
        if (logger.isFineEnabled() && (now - heartbeatTime) > heartbeatIntervalMillis * HEART_BEAT_INTERVAL_FACTOR) {
            logger.fine(format("Not receiving any heartbeats from %s since %s", member, timeToString(heartbeatTime)));
        }
        return false;
    }

    /**
     * Removes the {@code member} if it has not sent any master confirmation in
     * {@link GroupProperty#MAX_NO_MASTER_CONFIRMATION_SECONDS}.
     *
     * @param now    the current cluster clock time
     * @param member the member which needs to be checked
     * @return if the member has been removed
     */
    private boolean removeMemberIfMasterConfirmationExpired(long now, MemberImpl member) {
        Long lastConfirmation = masterConfirmationTimes.get(member);
        if (lastConfirmation == null) {
            lastConfirmation = 0L;
        }
        if (now - lastConfirmation > maxNoMasterConfirmationMillis) {
            String reason = format("Removing %s because it has not sent any master confirmation for %d ms. "
                            + " Clock time: %s."
                            + " Cluster time: %s."
                            + " Last confirmation time was %s.",
                    member, maxNoMasterConfirmationMillis,
                    timeToString(Clock.currentTimeMillis()),
                    timeToString(now),
                    timeToString(lastConfirmation));
            logger.warning(reason);
            // TODO [basri] I am the master so I can remove the member
            clusterService.removeAddress(member.getAddress(), reason);
            return true;
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
        Collection<MemberImpl> members = clusterService.getMemberImpls();

        for (MemberImpl member : members) {
            if (!member.localMember()) {
                try {
                    logIfConnectionToEndpointIsMissing(now, member);

                    if (isMaster(member)) {
                        if (removeMemberIfNotHeartBeating(now, member)) {
                            continue;
                        }
                    }

                    pingMemberIfRequired(now, member);
                    sendHeartbeat(member.getAddress());
                } catch (Throwable e) {
                    logger.severe(e);
                }
            }
        }
    }

    private boolean isMaster(MemberImpl member) {
        return member.getAddress().equals(node.getMasterAddress());
    }

    /**
     * Pings the {@code member} if {@link GroupProperty#ICMP_ENABLED} is true and more than {@link #HEART_BEAT_INTERVAL_FACTOR}
     * heartbeats have passed.
     */
    private void pingMemberIfRequired(long now, MemberImpl member) {
        if (!icmpEnabled) {
            return;
        }
        if ((now - getHeartbeatTime(member)) >= pingIntervalMillis) {
            ping(member);
        }
    }

    /**
     * Tries to ping the {@code memberImpl} and removes the member if it is unreachable. The actual method of determining
     * reachability is defined by the privileges and does not need to be an ICMP packet
     * (see {@link java.net.InetAddress#isReachable(NetworkInterface, int, int)}).
     *
     * @param memberImpl the member for which we need to determine reachability
     */
    private void ping(final MemberImpl memberImpl) {
        nodeEngine.getExecutionService().execute(ExecutionService.SYSTEM_EXECUTOR, new Runnable() {
            public void run() {
                try {
                    Address address = memberImpl.getAddress();
                    logger.warning(format("%s will ping %s", node.getThisAddress(), address));
                    for (int i = 0; i < MAX_PING_RETRY_COUNT; i++) {
                        try {
                            if (address.getInetAddress().isReachable(null, icmpTtl, icmpTimeoutMillis)) {
                                logger.info(format("%s pinged %s successfully", node.getThisAddress(), address));
                                return;
                            }
                        } catch (ConnectException ignored) {
                            // no route to host, means we cannot connect anymore
                            EmptyStatement.ignore(ignored);
                        }
                    }
                    // host not reachable
                    String reason = format("%s could not ping %s", node.getThisAddress(), address);
                    logger.warning(reason);
                    // TODO [basri] If I am the master, I can remove the member. Otherwise, I should only suspect it
                    clusterService.suspectAddress(address, reason, true);
                } catch (Throwable ignored) {
                    EmptyStatement.ignore(ignored);
                }
            }
        });
    }

    /** Send a {@link HeartbeatOperation} to the {@code target} */
    private void sendHeartbeat(Address target) {
        if (target == null) {
            return;
        }
        try {
            HeartbeatOperation heartbeat = new HeartbeatOperation(clusterService.getMemberListVersion(), clusterClock.getClusterTime());
            heartbeat.setCallerUuid(node.getThisUuid());
            node.nodeEngine.getOperationService().send(heartbeat, target);
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
    private void logIfConnectionToEndpointIsMissing(long now, MemberImpl member) {
        long heartbeatTime = getHeartbeatTime(member);
        if ((now - heartbeatTime) >= pingIntervalMillis) {
            Connection conn = node.connectionManager.getOrConnect(member.getAddress());
            if (conn == null || !conn.isAlive()) {
                logger.warning("This node does not have a connection to " + member);
            }
        }
    }

    /** Return the last heartbeat time for the {@code member} */
    private long getHeartbeatTime(MemberImpl member) {
        Long heartbeatTime = heartbeatTimes.get(member);
        return (heartbeatTime != null ? heartbeatTime : 0L);
    }

    /**
     * Sends a {@link MasterConfirmationOperation} to the master if this node is joined, it is not in the
     * {@link NodeState#SHUT_DOWN} state and is not the master node.
     */
    public void sendMasterConfirmation() {
        if (!node.joined() || node.getState() == NodeState.SHUT_DOWN || node.isMaster()) {
            return;
        }
        Address masterAddress = node.getMasterAddress();
        if (masterAddress == null) {
            logger.fine("Could not send MasterConfirmation, masterAddress is null!");
            return;
        }
        MemberImpl masterMember = clusterService.getMember(masterAddress);
        if (masterMember == null) {
            logger.fine("Could not send MasterConfirmation, masterMember is null!");
            return;
        }
        if (logger.isFineEnabled()) {
            logger.fine("Sending MasterConfirmation to " + masterMember);
        }
        nodeEngine.getOperationService().send(new MasterConfirmationOperation(clusterClock.getClusterTime()),
                masterAddress);
    }

    /**
     * Reset all master confirmations to the current cluster time. Called just before this node becomes the master and when
     * system clock jump is detected.
     */
    void resetMemberMasterConfirmations() {
        long now = clusterClock.getClusterTime();
        for (MemberImpl member : clusterService.getMemberImpls()) {
            masterConfirmationTimes.put(member, now);
        }
    }

    /** Reset all heartbeats to the current cluster time. Called when system clock jump is detected. */
    private void resetHeartbeats() {
        long now = clusterClock.getClusterTime();
        for (MemberImpl member : clusterService.getMemberImpls()) {
            heartbeatTimes.put(member, now);
        }
    }

    /** Remove the {@code member}'s master confirmation and heartbeat timestamps */
    void removeMember(MemberImpl member) {
        masterConfirmationTimes.remove(member);
        heartbeatTimes.remove(member);
    }

    void reset() {
        masterConfirmationTimes.clear();
        heartbeatTimes.clear();
    }
}
