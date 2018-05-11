/*
 *  Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.raft.service.session;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.service.RaftInvocationManager;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.session.SessionResponse;
import com.hazelcast.raft.impl.session.operation.CloseSessionOp;
import com.hazelcast.raft.impl.session.operation.CreateSessionOp;
import com.hazelcast.raft.impl.session.operation.HeartbeatSessionOp;
import com.hazelcast.spi.GracefulShutdownAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ExceptionUtil;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * TODO: Javadoc Pending...
 */
public class SessionManagerService extends AbstractSessionManager implements GracefulShutdownAwareService {

    private static final long SHUTDOWN_TASK_PERIOD_IN_MILLIS = SECONDS.toMillis(1);

    public static String SERVICE_NAME = "hz:raft:sessionManager";

    private final NodeEngine nodeEngine;

    public SessionManagerService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    @Override
    public boolean onShutdown(long timeout, TimeUnit unit) {
        ILogger logger = nodeEngine.getLogger(getClass());

        Map<RaftGroupId, ICompletableFuture<Object>> futures = shutdown();
        long remainingTimeNanos = unit.toNanos(timeout);
        boolean successful = true;

        while (remainingTimeNanos > 0 && futures.size() > 0) {
            Iterator<Map.Entry<RaftGroupId, ICompletableFuture<Object>>> it = futures.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<RaftGroupId, ICompletableFuture<Object>> entry = it.next();
                RaftGroupId groupId = entry.getKey();
                ICompletableFuture<Object> f = entry.getValue();
                if (f.isDone()) {
                    it.remove();
                    try {
                        f.get();
                        logger.fine("Session closed for " + groupId);
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

            remainingTimeNanos -= SHUTDOWN_TASK_PERIOD_IN_MILLIS;
        }

        return successful && futures.isEmpty();
    }

    private RaftInvocationManager getInvocationManager() {
        RaftService raftService = nodeEngine.getService(RaftService.SERVICE_NAME);
        return raftService.getInvocationManager();
    }

    @Override
    protected ScheduledFuture<?> scheduleWithRepetition(Runnable task, long period, TimeUnit unit) {
        return nodeEngine.getExecutionService().scheduleWithRepetition(task, period, period, unit);
    }

    @Override
    protected SessionResponse requestNewSession(RaftGroupId groupId) {
        CreateSessionOp op = new CreateSessionOp(nodeEngine.getThisAddress());
        ICompletableFuture<SessionResponse> future = getInvocationManager().invoke(groupId, op);
        try {
            return future.get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    protected ICompletableFuture<Object> heartbeat(RaftGroupId groupId, long sessionId) {
        return getInvocationManager().invoke(groupId, new HeartbeatSessionOp(sessionId));
    }

    @Override
    protected ICompletableFuture<Object> closeSession(RaftGroupId groupId, Long sessionId) {
        return getInvocationManager().invoke(groupId, new CloseSessionOp(sessionId));
    }
}
