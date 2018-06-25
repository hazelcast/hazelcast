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

package com.hazelcast.raft.service.lock.operation;

import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.IndeterminateOperationStateAware;
import com.hazelcast.raft.impl.util.PostponedResponse;
import com.hazelcast.raft.service.lock.FencedLock;
import com.hazelcast.raft.service.lock.LockEndpoint;
import com.hazelcast.raft.service.lock.RaftLockDataSerializerHook;
import com.hazelcast.raft.service.lock.RaftLockOwnershipState;
import com.hazelcast.raft.service.lock.RaftLockService;

import java.util.UUID;

/**
 * Operation for {@link FencedLock#lock()}
 *
 * @see com.hazelcast.raft.service.lock.RaftLock#acquire(LockEndpoint, long, UUID, boolean)
 */
public class LockOp extends AbstractLockOp implements IndeterminateOperationStateAware {

    public LockOp() {
    }

    public LockOp(String name, long sessionId, long threadId, UUID invUid) {
        super(name, sessionId, threadId, invUid);
    }

    @Override
    public Object run(RaftGroupId groupId, long commitIndex) {
        RaftLockService service = getService();
        LockEndpoint endpoint = getLockEndpoint();
        RaftLockOwnershipState ownership = service.acquire(groupId, name, endpoint, commitIndex, invocationUid);
        return ownership.isLocked() ? ownership : PostponedResponse.INSTANCE;
    }

    @Override
    public boolean isRetryableOnIndeterminateOperationState() {
        return true;
    }

    @Override
    public int getId() {
        return RaftLockDataSerializerHook.LOCK_OP;
    }
}
