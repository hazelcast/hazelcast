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

package com.hazelcast.cp.internal.datastructures.lock.operation;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.datastructures.lock.Lock;
import com.hazelcast.cp.lock.FencedLock;
import com.hazelcast.cp.internal.IndeterminateOperationStateAware;
import com.hazelcast.cp.internal.datastructures.lock.LockDataSerializerHook;
import com.hazelcast.cp.internal.datastructures.lock.LockService;

import java.util.UUID;

/**
 * Operation for {@link FencedLock#unlock()}
 *
 * @see Lock#release(LockEndpoint, UUID, int)
 */
public class UnlockOp extends AbstractLockOp implements IndeterminateOperationStateAware {

    public UnlockOp() {
    }

    public UnlockOp(String name, long sessionId, long threadId, UUID invUid) {
        super(name, sessionId, threadId, invUid);
    }

    @Override
    public Object run(CPGroupId groupId, long commitIndex) {
        LockService service = getService();
        return service.release(groupId, commitIndex, name, getLockEndpoint(), invocationUid);
    }

    @Override
    public boolean isRetryableOnIndeterminateOperationState() {
        return true;
    }

    @Override
    public int getClassId() {
        return LockDataSerializerHook.UNLOCK_OP;
    }
}
