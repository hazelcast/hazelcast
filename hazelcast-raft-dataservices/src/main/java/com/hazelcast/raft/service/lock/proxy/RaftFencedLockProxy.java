/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.raft.service.lock.proxy;

import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.impl.service.RaftInvocationManager;
import com.hazelcast.raft.service.lock.FencedLock;
import com.hazelcast.raft.service.lock.RaftLockOwnershipState;
import com.hazelcast.raft.service.lock.operation.ForceUnlockOp;
import com.hazelcast.raft.service.lock.operation.GetLockOwnershipStateOp;
import com.hazelcast.raft.service.lock.operation.LockOp;
import com.hazelcast.raft.service.lock.operation.TryLockOp;
import com.hazelcast.raft.service.lock.operation.UnlockOp;
import com.hazelcast.raft.service.session.SessionManagerService;
import com.hazelcast.raft.service.spi.operation.DestroyRaftObjectOp;
import com.hazelcast.spi.InternalCompletableFuture;

import java.util.UUID;

/**
 * Server-side proxy of Raft-based {@link FencedLock} API
 */
public class RaftFencedLockProxy extends AbstractRaftFencedLockProxy {

    private final RaftInvocationManager invocationManager;

    public RaftFencedLockProxy(RaftInvocationManager invocationManager, SessionManagerService sessionManager,
                               RaftGroupId groupId, String name) {
        super(sessionManager, groupId, name);
        this.invocationManager = invocationManager;
    }

    @Override
    protected final InternalCompletableFuture<RaftLockOwnershipState> doLock(RaftGroupId groupId, String name,
                                                                             long sessionId, long threadId,
                                                                             UUID invocationUid) {
        return invoke(new LockOp(name, sessionId, threadId, invocationUid));
    }

    @Override
    protected final InternalCompletableFuture<RaftLockOwnershipState> doTryLock(RaftGroupId groupId, String name,
                                                                                long sessionId, long threadId,
                                                                                UUID invocationUid, long timeoutMillis) {
        return invoke(new TryLockOp(name, sessionId, threadId, invocationUid, timeoutMillis));
    }

    @Override
    protected final InternalCompletableFuture<Object> doUnlock(RaftGroupId groupId, String name,
                                                               long sessionId, long threadId,
                                                               UUID invocationUid, int releaseCount) {
        return invoke(new UnlockOp(name, sessionId, threadId, invocationUid, releaseCount));
    }

    @Override
    protected final InternalCompletableFuture<Object> doForceUnlock(RaftGroupId groupId, String name,
                                                                    UUID invocationUid, long expectedFence) {
        return invoke(new ForceUnlockOp(name, expectedFence, invocationUid));
    }

    @Override
    protected final InternalCompletableFuture<RaftLockOwnershipState> doGetLockOwnershipState(RaftGroupId groupId,
                                                                                              String name) {
        return invoke(new GetLockOwnershipStateOp(name));
    }

    private <T> InternalCompletableFuture<T> invoke(RaftOp op) {
        return invocationManager.invoke(groupId, op);
    }

    @Override
    public void destroy() {
        invocationManager.invoke(groupId, new DestroyRaftObjectOp(getServiceName(), name)).join();
    }

}
