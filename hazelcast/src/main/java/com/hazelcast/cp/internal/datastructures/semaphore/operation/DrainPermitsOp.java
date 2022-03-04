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

package com.hazelcast.cp.internal.datastructures.semaphore.operation;

import com.hazelcast.cp.ISemaphore;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.IndeterminateOperationStateAware;
import com.hazelcast.cp.internal.datastructures.semaphore.Semaphore;
import com.hazelcast.cp.internal.datastructures.semaphore.SemaphoreDataSerializerHook;
import com.hazelcast.cp.internal.datastructures.semaphore.SemaphoreService;

import java.util.UUID;

import static com.hazelcast.cp.internal.session.AbstractProxySessionManager.NO_SESSION_ID;

/**
 * Operation for {@link ISemaphore#drainPermits()}}
 *
 * @see Semaphore#drain(long, long, UUID)
 */
public class DrainPermitsOp extends AbstractSemaphoreOp implements IndeterminateOperationStateAware {

    public DrainPermitsOp() {
    }

    public DrainPermitsOp(String name, long sessionId, long threadId, UUID invocationUid) {
        super(name, sessionId, threadId, invocationUid);
    }

    @Override
    public Object run(CPGroupId groupId, long commitIndex) {
        SemaphoreService service = getService();
        return service.drainPermits(groupId, name, commitIndex, getSemaphoreEndpoint(), invocationUid);
    }

    @Override
    public boolean isRetryableOnIndeterminateOperationState() {
        return sessionId != NO_SESSION_ID;
    }

    @Override
    public int getClassId() {
        return SemaphoreDataSerializerHook.DRAIN_PERMITS_OP;
    }
}
