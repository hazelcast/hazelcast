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

package com.hazelcast.cp.internal.datastructures.spi.blocking;

import com.hazelcast.cluster.Address;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftNodeLifecycleAwareService;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.datastructures.spi.AbstractCPMigrationAwareService;
import com.hazelcast.cp.internal.datastructures.spi.RaftManagedService;
import com.hazelcast.cp.internal.datastructures.spi.RaftRemoteService;
import com.hazelcast.cp.internal.datastructures.spi.blocking.operation.ExpireWaitKeysOp;
import com.hazelcast.cp.internal.operation.unsafe.UnsafeRaftReplicateOp;
import com.hazelcast.cp.internal.raft.SnapshotAwareService;
import com.hazelcast.cp.internal.raft.impl.RaftNode;
import com.hazelcast.cp.internal.session.SessionAccessor;
import com.hazelcast.cp.internal.session.SessionAwareService;
import com.hazelcast.cp.internal.session.SessionExpiredException;
import com.hazelcast.internal.partition.MigrationAwareService;
import com.hazelcast.internal.util.BiTuple;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.collection.Long2ObjectHashMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.operationservice.LiveOperations;
import com.hazelcast.spi.impl.operationservice.LiveOperationsTracker;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import static com.hazelcast.cp.internal.session.AbstractProxySessionManager.NO_SESSION_ID;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Base class for services that maintain blocking resources.
 * Contains common behaviour that will be needed by service implementations.
 *
 * @param <W>  concrete type of the WaitKey
 * @param <R>  concrete type of the resource
 * @param <RR> concrete ty;e lf the resource registry
 */
@SuppressWarnings("checkstyle:methodcount")
public abstract class AbstractBlockingService<W extends WaitKey, R extends BlockingResource<W>, RR extends ResourceRegistry<W, R>>
        extends AbstractCPMigrationAwareService
        implements RaftManagedService, RaftNodeLifecycleAwareService, RaftRemoteService, SessionAwareService,
        SnapshotAwareService<RR>, LiveOperationsTracker, MigrationAwareService {

    public static final long WAIT_TIMEOUT_TASK_UPPER_BOUND_MILLIS = 1500;
    private static final long WAIT_TIMEOUT_TASK_PERIOD_MILLIS = 500;

    protected final ILogger logger;
    protected volatile RaftService raftService;

    private final ConcurrentMap<CPGroupId, RR> registries = new ConcurrentHashMap<>();
    private volatile SessionAccessor sessionAccessor;

    protected AbstractBlockingService(NodeEngine nodeEngine) {
        super(nodeEngine);
        this.logger = nodeEngine.getLogger(getClass());
    }

    @Override
    public final void init(NodeEngine nodeEngine, Properties properties) {
        this.raftService = nodeEngine.getService(RaftService.SERVICE_NAME);
        ExecutionService executionService = nodeEngine.getExecutionService();
        executionService.scheduleWithRepetition(new ExpireWaitKeysPeriodicTask(),
                WAIT_TIMEOUT_TASK_PERIOD_MILLIS, WAIT_TIMEOUT_TASK_PERIOD_MILLIS, MILLISECONDS);

        initImpl();
    }

    /**
     * Subclasses can implement their custom initialization logic here
     */
    protected void initImpl() {
    }

    @Override
    public void reset() {
        if (!raftService.isCpSubsystemEnabled()) {
            registries.clear();
        }
    }

    @Override
    public void onCPSubsystemRestart() {
        registries.clear();
    }

    @Override
    public final void shutdown(boolean terminate) {
        registries.clear();
        shutdownImpl(terminate);
    }

    /**
     * Subclasses can implement their custom shutdown logic here
     */
    protected void shutdownImpl(boolean terminate) {
    }

    /**
     * Returns name of the service.
     */
    protected abstract String serviceName();

    /**
     * Creates a registry for the given Raft group.
     */
    protected abstract RR createNewRegistry(CPGroupId groupId);

    /**
     * Creates the response object that will be sent for a expired wait key.
     */
    protected abstract Object expiredWaitKeyResponse();

    protected void onRegistryRestored(RR registry) {
    }

    @Override
    public boolean destroyRaftObject(CPGroupId groupId, String name) {
        Collection<W> keys = getOrInitRegistry(groupId).destroyResource(name);
        if (keys == null) {
            return false;
        }

        List<Long> commitIndices = new ArrayList<>();
        for (W key : keys) {
            commitIndices.add(key.commitIndex());
        }

        completeFutures(groupId, commitIndices, new DistributedObjectDestroyedException(name + " is destroyed"));
        return true;
    }

    @Override
    public final RR takeSnapshot(CPGroupId groupId, long commitIndex) {
        RR registry = getRegistryOrNull(groupId);
        return registry != null ? (RR) registry.cloneForSnapshot() : null;
    }

    @Override
    public final void restoreSnapshot(CPGroupId groupId, long commitIndex, RR registry) {
        RR prev = registries.put(registry.getGroupId(), registry);
        // do not shift the already existing wait timeouts...
        Map<BiTuple<String, UUID>, BiTuple<Long, Long>> existingWaitTimeouts =
                prev != null ? prev.getWaitTimeouts() : Collections.emptyMap();
        Map<BiTuple<String, UUID>, Long> newWaitKeys = registry.overwriteWaitTimeouts(existingWaitTimeouts);
        for (Entry<BiTuple<String, UUID>, Long> e : newWaitKeys.entrySet()) {
            scheduleTimeout(groupId, e.getKey().element1, e.getKey().element2, e.getValue());
        }
        registry.onSnapshotRestore();
        onRegistryRestored(registry);
    }

    @Override
    public void setSessionAccessor(SessionAccessor accessor) {
        this.sessionAccessor = accessor;
    }

    @Override
    public final void onSessionClose(CPGroupId groupId, long sessionId) {
        ResourceRegistry<W, R> registry = registries.get(groupId);
        if (registry == null) {
            if (logger.isFineEnabled()) {
                logger.fine("Resource registry of " + groupId + " not found to handle closed Session[" + sessionId + "]");
            }
            return;
        }

        List<Long> expiredWaitKeys = new ArrayList<>();
        Long2ObjectHashMap<Object> completedWaitKeys = new Long2ObjectHashMap<>();
        registry.closeSession(sessionId, expiredWaitKeys, completedWaitKeys);

        if (logger.isFineEnabled() && (expiredWaitKeys.size() > 0 || completedWaitKeys.size() > 0)) {
            logger.fine("Closed Session[" + sessionId + "] in " + groupId + " expired wait key commit indices: "
                    + expiredWaitKeys + " completed wait keys: " + completedWaitKeys);
        }

        completeFutures(groupId, expiredWaitKeys, new SessionExpiredException());
        raftService.completeFutures(groupId, completedWaitKeys.entrySet());
    }

    @Override
    public final Collection<Long> getAttachedSessions(CPGroupId groupId) {
        RR registry = getRegistryOrNull(groupId);
        return registry != null ? registry.getAttachedSessions() : Collections.emptyList();
    }

    @Override
    public final void onRaftNodeTerminated(CPGroupId groupId) {
        ResourceRegistry<W, R> registry = registries.get(groupId);
        if (registry != null) {
            Collection<Long> indices = registry.destroy();
            completeFutures(groupId, indices, new DistributedObjectDestroyedException(groupId + " is destroyed"));
        }
    }

    @Override
    public final void onRaftNodeSteppedDown(CPGroupId groupId) {
    }

    @Override
    public final void populate(LiveOperations liveOperations) {
        long now = Clock.currentTimeMillis();
        for (RR registry : registries.values()) {
            registry.populate(liveOperations, now);
        }
    }

    public final void expireWaitKeys(CPGroupId groupId, Collection<BiTuple<String, UUID>> keys) {
        // no need to validate the session. if the session is expired, the corresponding wait key is gone already
        ResourceRegistry<W, R> registry = registries.get(groupId);
        if (registry == null) {
            logger.severe("Registry of " + groupId + " not found to expire wait keys: " + keys);
            return;
        }

        List<W> expired = new ArrayList<>();
        for (BiTuple<String, UUID> key : keys) {
            registry.expireWaitKey(key.element1, key.element2, expired);
        }

        List<Long> commitIndices = new ArrayList<>();
        for (W key : expired) {
            commitIndices.add(key.commitIndex());
            registry.removeLiveOperation(key);
        }

        completeFutures(groupId, commitIndices, expiredWaitKeyResponse());
    }

    public final RR getRegistryOrNull(CPGroupId groupId) {
        return registries.get(groupId);
    }

    public Collection<BiTuple<Address, Long>> getLiveOperations(CPGroupId groupId) {
        RR registry = registries.get(groupId);
        if (registry == null) {
            return Collections.emptySet();
        }
        return Collections.unmodifiableCollection(registry.getLiveOperations());
    }

    public int getTotalResourcesCount() {
        return registries.values().stream()
                .mapToInt(collection -> collection.resources.size())
                .sum();
    }

    protected final RR getOrInitRegistry(CPGroupId groupId) {
        checkNotNull(groupId);
        RR registry = registries.get(groupId);
        if (registry == null) {
            registry = createNewRegistry(groupId);
            registries.put(groupId, registry);
        }
        return registry;
    }

    protected final void scheduleTimeout(CPGroupId groupId, String name, UUID invocationUid, long timeoutMs) {
        if (timeoutMs > 0 && timeoutMs <= WAIT_TIMEOUT_TASK_UPPER_BOUND_MILLIS) {
            ExecutionService executionService = nodeEngine.getExecutionService();
            executionService.schedule(new ExpireWaitKeysTask(groupId, BiTuple.of(name, invocationUid)), timeoutMs, MILLISECONDS);
        }
    }

    protected final void heartbeatSession(CPGroupId groupId, long sessionId) {
        if (sessionId == NO_SESSION_ID) {
            return;
        }

        if (sessionAccessor.isActive(groupId, sessionId)) {
            sessionAccessor.heartbeat(groupId, sessionId);
            return;
        }

        throw new SessionExpiredException("active session: " + sessionId + " does not exist in " + groupId);
    }

    protected final void notifyWaitKeys(CPGroupId groupId, String name, Collection<W> keys, Object result) {
        if (keys.isEmpty()) {
            return;
        }

        if (logger.isFineEnabled()) {
            logger.fine("Resource[" + name + "] in " + groupId + " completed wait keys: " + keys + " result: " + result);
        }

        List<Long> indices = new ArrayList<>(keys.size());
        for (W key : keys) {
            indices.add(key.commitIndex());
        }

        completeFutures(groupId, indices, result);
    }

    private void completeFutures(CPGroupId groupId, Collection<Long> indices, Object result) {
        if (!indices.isEmpty()) {
            if (!raftService.completeFutures(groupId, indices, result)) {
                logger.severe("RaftNode not found for " + groupId + " to notify commit indices " + indices + " with " + result);
            }
        }
    }

    private void tryReplicateExpiredWaitKeys(CPGroupId groupId, Collection<BiTuple<String, UUID>> keys) {
        InternalCompletableFuture future = null;
        try {
            ExpireWaitKeysOp op = new ExpireWaitKeysOp(serviceName(), keys);
            if (raftService.isCpSubsystemEnabled()) {
                RaftNode raftNode = raftService.getRaftNode(groupId);
                if (raftNode != null) {
                    future = raftNode.replicate(op);
                }
            } else {
                future = raftService.getInvocationManager()
                        .invokeOnPartition(new UnsafeRaftReplicateOp(groupId, op));
            }

            if (future != null) {
                future.get(WAIT_TIMEOUT_TASK_PERIOD_MILLIS, MILLISECONDS);
            }
        } catch (Exception e) {
            if (logger.isFineEnabled()) {
                logger.fine("Could not expire wait keys: " + keys + " in " + groupId, e);
            }
        }
    }

    protected Set<CPGroupId> getGroupIdSet() {
        return registries.keySet();
    }

    private class ExpireWaitKeysTask implements Runnable {
        final CPGroupId groupId;
        final Collection<BiTuple<String, UUID>> keys;

        ExpireWaitKeysTask(CPGroupId groupId, BiTuple<String, UUID> key) {
            this.groupId = groupId;
            this.keys = Collections.singleton(key);
        }

        @Override
        public void run() {
            tryReplicateExpiredWaitKeys(groupId, keys);
        }
    }

    private class ExpireWaitKeysPeriodicTask implements Runnable {
        @Override
        public void run() {
            for (Entry<CPGroupId, Collection<BiTuple<String, UUID>>> e : getWaitKeysToExpire().entrySet()) {
                if (currentThread().isInterrupted()) {
                    break;
                }
                tryReplicateExpiredWaitKeys(e.getKey(), e.getValue());
            }
        }

        // queried locally
        private Map<CPGroupId, Collection<BiTuple<String, UUID>>> getWaitKeysToExpire() {
            Map<CPGroupId, Collection<BiTuple<String, UUID>>> timeouts = new HashMap<>();
            long now = Clock.currentTimeMillis();
            for (ResourceRegistry<W, R> registry : registries.values()) {
                Collection<BiTuple<String, UUID>> t = registry.getWaitKeysToExpire(now);
                if (t.size() > 0) {
                    timeouts.put(registry.getGroupId(), t);
                }
            }

            return timeouts;
        }
    }

    @Override
    protected int getBackupCount() {
        return 1;
    }

    @Override
    protected Map<CPGroupId, Object> getSnapshotMap(int partitionId) {
        assert !raftService.isCpSubsystemEnabled();
        return getGroupIdSet().stream()
                .filter(groupId -> raftService.getCPGroupPartitionId(groupId) == partitionId)
                .map(groupId -> BiTuple.of(groupId, takeSnapshot(groupId, 0L)))
                .collect(Collectors.toMap(tuple -> tuple.element1, tuple -> tuple.element2));
    }

    @Override
    protected void clearPartitionReplica(int partitionId) {
        getGroupIdSet().removeIf(groupId -> raftService.getCPGroupPartitionId(groupId) == partitionId);
    }
}
