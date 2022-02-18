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

package com.hazelcast.cp.internal.session;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftNodeLifecycleAwareService;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.TermChangeAwareService;
import com.hazelcast.cp.internal.datastructures.spi.AbstractCPMigrationAwareService;
import com.hazelcast.cp.internal.operation.unsafe.UnsafeRaftReplicateOp;
import com.hazelcast.cp.internal.raft.SnapshotAwareService;
import com.hazelcast.cp.internal.raft.impl.RaftNode;
import com.hazelcast.cp.internal.session.operation.CloseInactiveSessionsOp;
import com.hazelcast.cp.internal.session.operation.CloseSessionOp;
import com.hazelcast.cp.internal.session.operation.ExpireSessionsOp;
import com.hazelcast.cp.internal.session.operation.GetSessionsOp;
import com.hazelcast.cp.internal.util.PartitionSpecificRunnableAdaptor;
import com.hazelcast.cp.session.CPSession;
import com.hazelcast.cp.session.CPSession.CPSessionOwnerType;
import com.hazelcast.cp.session.CPSessionManagementService;
import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.metrics.ProbeUnit;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.internal.util.BiTuple;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.executor.ManagedExecutorService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

import static com.hazelcast.cp.CPGroup.METADATA_CP_GROUP_NAME;
import static com.hazelcast.cp.internal.RaftService.getCPGroupPartitionId;
import static com.hazelcast.cp.internal.raft.QueryPolicy.LINEARIZABLE;
import static com.hazelcast.internal.util.Preconditions.checkTrue;
import static com.hazelcast.spi.impl.InternalCompletableFuture.completingCallback;
import static com.hazelcast.spi.impl.executionservice.ExecutionService.SYSTEM_EXECUTOR;
import static java.util.Collections.unmodifiableCollection;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * This service offers a set of abstractions to track liveliness of Hazelcast
 * clients and servers that use (possibly blocking) data structures
 * on the Raft layer, such as locks and semaphores. From the perspective of
 * the Raft layer, there is no discrimination between Hazelcast clients and
 * servers that use these data structures. A caller starts a session and sends
 * periodic heartbeats to maintain its session. If there is no heartbeat for
 * {@link CPSubsystemConfig#getSessionTimeToLiveSeconds()} seconds, its session
 * is closed. That caller is considered to be alive as long as it is committing
 * heartbeats.
 * <p>
 * Blocking Raft services can make use of the session abstraction to attach
 * resources to sessions. On session termination, its attached resources will
 * be released automatically.
 */
@SuppressWarnings({"checkstyle:methodcount"})
public class RaftSessionService extends AbstractCPMigrationAwareService
        implements ManagedService, SnapshotAwareService<RaftSessionRegistry>, SessionAccessor,
        TermChangeAwareService, RaftNodeLifecycleAwareService, CPSessionManagementService, DynamicMetricsProvider {

    public static final String SERVICE_NAME = "hz:core:raftSession";

    private static final long CHECK_EXPIRED_SESSIONS_TASK_PERIOD_IN_MILLIS = SECONDS.toMillis(1);
    private static final long CHECK_INACTIVE_SESSIONS_TASK_PERIOD_IN_MILLIS = SECONDS.toMillis(30);
    private static final long COLLECT_INACTIVE_SESSIONS_TASK_TIMEOUT_SECONDS = 5;

    private final ILogger logger;
    private volatile RaftService raftService;

    private final Map<CPGroupId, RaftSessionRegistry> registries = new ConcurrentHashMap<>();

    public RaftSessionService(NodeEngine nodeEngine) {
        super(nodeEngine);
        this.logger = nodeEngine.getLogger(getClass());
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.raftService = nodeEngine.getService(RaftService.SERVICE_NAME);
        for (SessionAwareService service : nodeEngine.getServices(SessionAwareService.class)) {
            service.setSessionAccessor(this);
        }
        ExecutionService executionService = nodeEngine.getExecutionService();
        executionService.scheduleWithRepetition(new CheckSessionsToExpire(), CHECK_EXPIRED_SESSIONS_TASK_PERIOD_IN_MILLIS,
                CHECK_EXPIRED_SESSIONS_TASK_PERIOD_IN_MILLIS, MILLISECONDS);
        executionService.scheduleWithRepetition(new CheckInactiveSessions(), CHECK_INACTIVE_SESSIONS_TASK_PERIOD_IN_MILLIS,
                CHECK_INACTIVE_SESSIONS_TASK_PERIOD_IN_MILLIS, MILLISECONDS);

        this.nodeEngine.getMetricsRegistry().registerDynamicMetricsProvider(this);
    }

    @Override
    public void reset() {
    }

    @Override
    public void shutdown(boolean terminate) {
        registries.clear();
    }

    @Override
    public RaftSessionRegistry takeSnapshot(CPGroupId groupId, long commitIndex) {
        RaftSessionRegistry registry = registries.get(groupId);
        return registry != null ? registry.cloneForSnapshot() : null;
    }

    @Override
    public void restoreSnapshot(CPGroupId groupId, long commitIndex, RaftSessionRegistry registry) {
        if (registry != null) {
            registries.put(groupId, registry);
        }
    }

    @Override
    public void onNewTermCommit(CPGroupId groupId, long commitIndex) {
        RaftSessionRegistry registry = registries.get(groupId);
        if (registry != null) {
            registry.shiftExpirationTimes(getHeartbeatIntervalMillis());
            if (logger.isFineEnabled()) {
                logger.fine("Session expiration times are shifted in " + groupId);
            }
        }
    }

    @Override
    public void onRaftNodeTerminated(CPGroupId groupId) {
        registries.remove(groupId);
    }

    @Override
    public void onRaftNodeSteppedDown(CPGroupId groupId) {
    }

    @Override
    public InternalCompletableFuture<Collection<CPSession>> getAllSessions(String groupName) {
        checkTrue(!METADATA_CP_GROUP_NAME.equals(groupName), "Cannot query CP sessions on the METADATA CP group!");
        ManagedExecutorService executor = nodeEngine.getExecutionService().getExecutor(SYSTEM_EXECUTOR);
        InternalCompletableFuture<Collection<CPSession>> future = InternalCompletableFuture.withExecutor(executor);

        raftService.getCPGroup(groupName).whenCompleteAsync((group, t) -> {
            if (t == null) {
                if (group != null) {
                    getAllSessions(group.id()).whenCompleteAsync(completingCallback(future));
                } else {
                    future.completeExceptionally(new IllegalArgumentException());
                }
            } else {
                future.completeExceptionally(t);
            }
        });

        return future;
    }

    public InternalCompletableFuture<Collection<CPSession>> getAllSessions(CPGroupId groupId) {
        checkTrue(!METADATA_CP_GROUP_NAME.equals(groupId.getName()), "Cannot query CP sessions on the METADATA CP group!");
        return raftService.getInvocationManager().query(groupId, new GetSessionsOp(), LINEARIZABLE);
    }

    @Override
    public InternalCompletableFuture<Boolean> forceCloseSession(String groupName, final long sessionId) {
        ManagedExecutorService executor = nodeEngine.getExecutionService().getExecutor(SYSTEM_EXECUTOR);
        InternalCompletableFuture<Boolean> future = InternalCompletableFuture.withExecutor(executor);
        raftService.getCPGroup(groupName).whenCompleteAsync((group, t) -> {
            if (t == null) {
                if (group != null) {
                    raftService.getInvocationManager().<Boolean>invoke(group.id(), new CloseSessionOp(sessionId))
                            .whenCompleteAsync(completingCallback(future));
                } else {
                    future.complete(false);
                }
            } else {
                future.completeExceptionally(t);
            }
        });

        return future;
    }

    public SessionResponse createNewSession(CPGroupId groupId, Address endpoint, String endpointName,
                                            CPSessionOwnerType endpointType) {
        RaftSessionRegistry registry = getOrInitRegistry(groupId);
        long creationTime = Clock.currentTimeMillis();
        long sessionTTLMillis = getSessionTTLMillis();
        long sessionId = registry.createNewSession(sessionTTLMillis, endpoint, endpointName, endpointType, creationTime);
        logger.info("Created new session: " + sessionId + " in " + groupId + " for " + endpointType + " -> " + endpoint);
        return new SessionResponse(sessionId, sessionTTLMillis, getHeartbeatIntervalMillis());
    }

    private RaftSessionRegistry getOrInitRegistry(CPGroupId groupId) {
        RaftSessionRegistry registry = registries.get(groupId);
        if (registry == null) {
            registry = new RaftSessionRegistry(groupId);
            registries.put(groupId, registry);
            if (logger.isFineEnabled()) {
                logger.fine("Created new session registry for " + groupId);
            }
        }
        return registry;
    }

    public void heartbeat(CPGroupId groupId, long sessionId) {
        RaftSessionRegistry registry = registries.get(groupId);
        if (registry == null) {
            throw new IllegalStateException("No session: " + sessionId + " for CP group: " + groupId);
        }

        registry.heartbeat(sessionId, getSessionTTLMillis());
        if (logger.isFineEnabled()) {
            logger.fine("Session: " + sessionId + " heartbeat in " + groupId);
        }
    }

    public boolean closeSession(CPGroupId groupId, long sessionId) {
        RaftSessionRegistry registry = registries.get(groupId);
        if (registry == null) {
            return false;
        }

        if (registry.closeSession(sessionId)) {
            logger.info("Session: " + sessionId +  " is closed in " + groupId);
            notifyServices(groupId, Collections.singleton(sessionId));
            return true;
        }

        return false;
    }

    public void expireSessions(CPGroupId groupId, Collection<BiTuple<Long, Long>> sessionsToExpire) {
        RaftSessionRegistry registry = registries.get(groupId);
        if (registry == null) {
            return;
        }

        List<Long> expired = new ArrayList<>();
        for (BiTuple<Long, Long> s : sessionsToExpire) {
            long sessionId = s.element1;
            long version = s.element2;
            if (registry.expireSession(sessionId, version)) {
                expired.add(sessionId);
            }
        }

        if (expired.size() > 0) {
            if (logger.isFineEnabled()) {
                logger.fine("Sessions: " + expired + " are expired in " + groupId);
            }

            notifyServices(groupId, expired);
        }
    }

    public void closeInactiveSessions(CPGroupId groupId, Collection<Long> inactiveSessions) {
        RaftSessionRegistry registry = registries.get(groupId);
        if (registry == null) {
            return;
        }

        Collection<Long> closed = new HashSet<>(inactiveSessions);
        for (SessionAwareService service : nodeEngine.getServices(SessionAwareService.class)) {
            closed.removeAll(service.getAttachedSessions(groupId));
        }

        for (long sessionId : closed) {
            registry.closeSession(sessionId);
        }

        if (closed.size() > 0) {
            if (logger.isFineEnabled()) {
                logger.fine("Inactive sessions: " + closed + " are closed in " + groupId);
            }
            notifyServices(groupId, closed);
        }
    }

    public long generateThreadId(CPGroupId groupId) {
        return getOrInitRegistry(groupId).generateThreadId();
    }

    public Collection<CPSession> getSessionsLocally(CPGroupId groupId) {
        RaftSessionRegistry registry = getSessionRegistryOrNull(groupId);
        if (registry == null) {
            return Collections.emptyList();
        }

        return unmodifiableCollection(registry.getSessions());
    }

    // queried locally in tests
    RaftSessionRegistry getSessionRegistryOrNull(CPGroupId groupId) {
        return registries.get(groupId);
    }

    private long getHeartbeatIntervalMillis() {
        return SECONDS.toMillis(raftService.getConfig().getSessionHeartbeatIntervalSeconds());
    }

    private long getSessionTTLMillis() {
        return SECONDS.toMillis(raftService.getConfig().getSessionTimeToLiveSeconds());
    }

    private void notifyServices(CPGroupId groupId, Collection<Long> sessionIds) {
        Collection<SessionAwareService> services = nodeEngine.getServices(SessionAwareService.class);
        for (SessionAwareService sessionAwareService : services) {
            for (long sessionId : sessionIds) {
                sessionAwareService.onSessionClose(groupId, sessionId);
            }
        }
    }

    @Override
    public boolean isActive(CPGroupId groupId, long sessionId) {
        RaftSessionRegistry sessionRegistry = registries.get(groupId);
        if (sessionRegistry == null) {
            return false;
        }
        CPSessionInfo session = sessionRegistry.getSession(sessionId);
        return session != null;
    }

    // queried locally
    private Map<CPGroupId, Collection<BiTuple<Long, Long>>> getSessionsToExpire() {
        Map<CPGroupId, Collection<BiTuple<Long, Long>>> expired = new HashMap<>();
        for (RaftSessionRegistry registry : registries.values()) {
            Collection<BiTuple<Long, Long>> e = registry.getSessionsToExpire();
            if (!e.isEmpty()) {
                expired.put(registry.groupId(), e);
            }
        }

        return expired;
    }

    private Map<CPGroupId, Collection<Long>> getInactiveSessions() {
        Map<CPGroupId, Collection<Long>> response = new ConcurrentHashMap<>();
        Semaphore semaphore = new Semaphore(0);

        OperationServiceImpl operationService = nodeEngine.getOperationService();
        Collection<RaftSessionRegistry> registries = new ArrayList<>(this.registries.values());

        for (RaftSessionRegistry registry : registries) {
            CPGroupId groupId = registry.groupId();
            operationService.execute(new PartitionSpecificRunnableAdaptor(() -> {
                Set<Long> activeSessionIds = new HashSet<>();
                for (SessionAwareService service : nodeEngine.getServices(SessionAwareService.class)) {
                    activeSessionIds.addAll(service.getAttachedSessions(groupId));
                }

                Set<Long> inactiveSessionIds = new HashSet<>();
                for (CPSession session : registry.getSessions()) {
                    if (!activeSessionIds.contains(session.id())
                            && session.creationTime() + getSessionTTLMillis() < Clock.currentTimeMillis()) {
                        inactiveSessionIds.add(session.id());
                    }
                }

                if (inactiveSessionIds.size() > 0) {
                    response.put(groupId, inactiveSessionIds);
                }

                semaphore.release();
            }, nodeEngine.getPartitionService().getPartitionId(groupId)));
        }

        try {
            semaphore.tryAcquire(registries.size(), COLLECT_INACTIVE_SESSIONS_TASK_TIMEOUT_SECONDS, SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        return response;
    }

    @Override
    protected int getBackupCount() {
        return 1;
    }

    @Override
    protected Map<CPGroupId, Object> getSnapshotMap(int partitionId) {
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        return registries.keySet().stream()
                .filter(groupId -> getCPGroupPartitionId(groupId, partitionCount) == partitionId)
                .distinct()
                .map(groupId -> BiTuple.of(groupId, takeSnapshot(groupId, 0L)))
                .collect(Collectors.toMap(tuple -> tuple.element1, tuple -> tuple.element2));
    }

    @Override
    protected void clearPartitionReplica(int partitionId) {
        registries.keySet().removeIf(groupId -> raftService.getCPGroupPartitionId(groupId) == partitionId);
    }

    private class CheckSessionsToExpire implements Runnable {
        @Override
        public void run() {
            Map<CPGroupId, Collection<BiTuple<Long, Long>>> sessionsToExpire = getSessionsToExpire();
            for (Entry<CPGroupId, Collection<BiTuple<Long, Long>>> entry : sessionsToExpire.entrySet()) {
                CPGroupId groupId = entry.getKey();
                Collection<BiTuple<Long, Long>> sessions = entry.getValue();
                if (raftService.isCpSubsystemEnabled()) {
                    expireOnRaftNode(groupId, sessions);
                } else {
                    expireOnPartitionOwner(groupId, sessions);
                }
            }
        }

        private void expireOnRaftNode(CPGroupId groupId, Collection<BiTuple<Long, Long>> sessions) {
            RaftNode raftNode = raftService.getRaftNode(groupId);
            if (raftNode != null) {
                try {
                    InternalCompletableFuture f = raftNode.replicate(new ExpireSessionsOp(sessions));
                    f.get();
                } catch (Exception e) {
                    if (logger.isFineEnabled()) {
                        logger.fine("Could not invalidate sessions: " + sessions + " of " + groupId, e);
                    }
                }
            }
        }

        private void expireOnPartitionOwner(CPGroupId groupId, Collection<BiTuple<Long, Long>> sessions) {
            InternalCompletableFuture<Object> future = raftService.getInvocationManager()
                    .invokeOnPartition(new UnsafeRaftReplicateOp(groupId, new ExpireSessionsOp(sessions)));
            try {
                future.join();
            } catch (Exception e) {
                if (logger.isFineEnabled()) {
                    logger.fine("Could not invalidate sessions: " + sessions + " of " + groupId, e);
                }
            }
        }
    }

    private class CheckInactiveSessions implements Runnable {
        @Override
        public void run() {
            Map<CPGroupId, Collection<Long>> inactiveSessions = getInactiveSessions();
            for (Entry<CPGroupId, Collection<Long>> entry : inactiveSessions.entrySet()) {
                CPGroupId groupId = entry.getKey();
                Collection<Long> sessions = entry.getValue();
                if (raftService.isCpSubsystemEnabled()) {
                    closeOnRaft(groupId, sessions);
                } else {
                    closeOnPartitionOwner(groupId, sessions);
                }
            }

        }

        private void closeOnRaft(CPGroupId groupId, Collection<Long> sessions) {
            RaftNode raftNode = raftService.getRaftNode(groupId);
            if (raftNode != null) {
                try {
                    InternalCompletableFuture f = raftNode.replicate(new CloseInactiveSessionsOp(sessions));
                    f.get();
                } catch (Exception e) {
                    if (logger.isFineEnabled()) {
                        logger.fine("Could not close inactive sessions: " + sessions + " of " + groupId, e);
                    }
                }
            }
        }

        private void closeOnPartitionOwner(CPGroupId groupId, Collection<Long> sessions) {
            InternalCompletableFuture<Object> future = raftService.getInvocationManager()
                    .invokeOnPartition(new UnsafeRaftReplicateOp(groupId, new CloseInactiveSessionsOp(sessions)));
            try {
                future.join();
            } catch (Exception e) {
                if (logger.isFineEnabled()) {
                    logger.fine("Could not close inactive sessions: " + sessions + " of " + groupId, e);
                }
            }
        }
    }

    @Override
    public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext context) {
        MetricDescriptor root = descriptor.withPrefix("cp.session");

        for (RaftSessionRegistry registry : registries.values()) {
            CPGroupId groupId = registry.groupId();
            for (CPSession session : registry.getSessions()) {
                MetricDescriptor desc = root.copy()
                        .withDiscriminator("id", session.id() + "@" + groupId.getName())
                        .withTag("sessionId", String.valueOf(session.id()))
                        .withTag("group", groupId.getName());

                context.collect(desc.copy().withTag("endpoint", session.endpoint().toString()).withMetric("endpoint"), 0);
                context.collect(desc.copy().withTag("endpointType", session.endpointType().toString())
                        .withMetric("endpointType"), 0);

                context.collect(desc.copy().withMetric("version"), session.version());
                context.collect(desc.copy().withUnit(ProbeUnit.MS).withMetric("creationTime"), session.creationTime());
                context.collect(desc.copy().withUnit(ProbeUnit.MS).withMetric("expirationTime"), session.expirationTime());
            }
        }
    }
}
