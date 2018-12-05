/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.lock.operation;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.lock.FencedLock;
import com.hazelcast.cp.internal.IndeterminateOperationStateAware;
import com.hazelcast.cp.internal.datastructures.lock.AcquireResult;
import com.hazelcast.cp.internal.datastructures.lock.RaftLockDataSerializerHook;
import com.hazelcast.cp.internal.datastructures.lock.RaftLockService;
import com.hazelcast.cp.internal.raft.impl.util.PostponedResponse;

import java.util.UUID;

import static com.hazelcast.cp.internal.datastructures.lock.AcquireResult.AcquireStatus.WAIT_KEY_ADDED;

/**
 * Operation for {@link FencedLock#lock()}
 *
 * @see com.hazelcast.cp.internal.datastructures.lock.RaftLock#acquire(LockEndpoint, long, UUID, boolean)
 */
public class LockOp extends AbstractLockOp implements IndeterminateOperationStateAware {

    public LockOp() {
    }

    public LockOp(String name, long sessionId, long threadId, UUID invUid) {
        super(name, sessionId, threadId, invUid);
    }

    @Override
    public Object run(CPGroupId groupId, long commitIndex) {
        RaftLockService service = getService();
        AcquireResult result = service.acquire(groupId, commitIndex, name, getLockEndpoint(), invocationUid);
        if (result.status() == WAIT_KEY_ADDED) {
            return PostponedResponse.INSTANCE;
        }

        // SUCCESSFUL or FAILED
        return result.fence();
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
