/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.RaftInvocationManager;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.datastructures.spi.RaftManagedService;
import com.hazelcast.cp.internal.session.operation.CloseSessionOp;
import com.hazelcast.cp.internal.session.operation.CreateSessionOp;
import com.hazelcast.cp.internal.session.operation.GenerateThreadIdOp;
import com.hazelcast.cp.internal.session.operation.HeartbeatSessionOp;
import com.hazelcast.internal.services.GracefulShutdownAwareService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cp.session.CPSession.CPSessionOwnerType.SERVER;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Server-side implementation of Raft proxy session manager
 */
public class ProxySessionManagerService extends AbstractProxySessionManager implements GracefulShutdownAwareService,
                                                                                       RaftManagedService {

    /**
     * Name of the service
     */
    public static final String SERVICE_NAME = "hz:raft:proxySessionManagerService";

    private static final long SHUTDOWN_TASK_PERIOD_IN_MILLIS = SECONDS.toMillis(1);
    private volatile boolean skipSessionCloseOnShutdown;


    private final NodeEngine nodeEngine;

    public ProxySessionManagerService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    @Override
    protected long generateThreadId(RaftGroupId groupId) {
        return getInvocationManager().<Long>invoke(groupId, new GenerateThreadIdOp()).joinInternal();
    }

    @Override
    protected SessionResponse requestNewSession(RaftGroupId groupId) {
        String instanceName = nodeEngine.getConfig().getInstanceName();
        RaftOp op = new CreateSessionOp(nodeEngine.getThisAddress(), instanceName, SERVER);
        InternalCompletableFuture<SessionResponse> future = getInvocationManager().invoke(groupId, op);
        return future.joinInternal();
    }

    @Override
    protected InternalCompletableFuture<Object> heartbeat(RaftGroupId groupId, long sessionId) {
        return getInvocationManager().invoke(groupId, new HeartbeatSessionOp(sessionId));
    }

    @Override
    protected InternalCompletableFuture<Object> closeSession(RaftGroupId groupId, Long sessionId) {
        if (skipSessionCloseOnShutdown) {
            return InternalCompletableFuture.newCompletedFuture(Boolean.TRUE);
        }
        return getInvocationManager().invoke(groupId, new CloseSessionOp(sessionId));
    }

    @Override
    protected ScheduledFuture<?> scheduleWithRepetition(Runnable task, long period, TimeUnit unit) {
        return nodeEngine.getExecutionService().scheduleWithRepetition(task, period, period, unit);
    }

    @Override
    public boolean onShutdown(long timeout, TimeUnit unit) {
        this.skipSessionCloseOnShutdown = shouldSkipCpCleanupNow();
        ILogger logger = nodeEngine.getLogger(getClass());

        // HZ-5226: Limited scope — optimized for ordered and graceful shutdown semantics
        // (e.g., orderedReady in Kubernetes).
        //
        // If all nodes shut down in parallel, this member may become the last active one
        // during its own shutdown hook. In that case, it might attempt to replicate a
        // CloseSession operation with a cluster size of one. This is harmless but can cause
        // the shutdown process to stall unnecessarily.
        if (skipSessionCloseOnShutdown) {
            logger.fine("Skipping CP session close for this shutdown as this is the last active member.");
            return true;
        }

        Map<RaftGroupId, InternalCompletableFuture<Object>> futures = shutdown();
        long remainingTimeNanos = unit.toNanos(timeout);
        boolean successful = true;

        while (remainingTimeNanos > 0 && futures.size() > 0) {
            Iterator<Entry<RaftGroupId, InternalCompletableFuture<Object>>> it = futures.entrySet().iterator();
            while (it.hasNext()) {
                Entry<RaftGroupId, InternalCompletableFuture<Object>> entry = it.next();
                RaftGroupId groupId = entry.getKey();
                InternalCompletableFuture<Object> f = entry.getValue();
                if (f.isDone()) {
                    it.remove();
                    try {
                        f.get();
                        logger.fine("Session closed for %s", groupId);
                    } catch (Exception e) {
                        logger.warning("Close session failed for " + groupId, e);
                        successful = false;
                    }
                }
            }

            try {
                Thread.sleep(SHUTDOWN_TASK_PERIOD_IN_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }

            remainingTimeNanos -= MILLISECONDS.toNanos(SHUTDOWN_TASK_PERIOD_IN_MILLIS);
        }

        return successful && futures.isEmpty();
    }

    /*
    If last member in the cluster, there are no other members to replicate to.
     */
    private boolean shouldSkipCpCleanupNow() {
        RaftService raftService = nodeEngine.getService(RaftService.SERVICE_NAME);
        return raftService.getMetadataGroupManager().getActiveMembers().size() <= 1;
    }

    private RaftInvocationManager getInvocationManager() {
        RaftService raftService = nodeEngine.getService(RaftService.SERVICE_NAME);
        return raftService.getInvocationManager();
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
    }

    @Override
    public void reset() {
    }

    @Override
    public void shutdown(boolean terminate) {
    }

    // TODO: This is never actually called from anywhere ... as
    // this class is not a RaftRemoteService (see resetLocalRaftState)
    // After resest, the internal state will exist for some short time
    // until cleaned up by lack of heartbeats. Doesn't result in any
    // long lived state but should be fixed.
    @Override
    public void onCPSubsystemRestart() {
        resetInternalState();
    }
}
