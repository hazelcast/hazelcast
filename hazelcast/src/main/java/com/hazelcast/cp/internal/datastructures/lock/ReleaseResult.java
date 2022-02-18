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

package com.hazelcast.cp.internal.datastructures.lock;

import java.util.Collection;
import java.util.Collections;

import static com.hazelcast.cp.internal.datastructures.lock.LockOwnershipState.NOT_LOCKED;
import static java.util.Collections.unmodifiableCollection;

/**
 * Represents result of an unlock() request
 */
class ReleaseResult {

    static final ReleaseResult FAILED = new ReleaseResult(false, NOT_LOCKED, Collections.<LockInvocationKey>emptyList());

    /**
     * true if the unlock() request is successful
     */
    private final boolean success;

    /**
     * If the unlock() request is successful, represents new state of the lock ownership.
     * It can be {@link LockOwnershipState#NOT_LOCKED} if the lock has no new owner after successful release.
     * It is {@link LockOwnershipState#NOT_LOCKED} if the unlock() request is failed.
     */
    private final LockOwnershipState ownership;

    /**
     * If the unlock() request is successful and ownership is given to some other endpoint, contains its wait keys.
     * If the unlock() request is failed, can contain cancelled wait keys of the caller, if there is any.
     */
    private final Collection<LockInvocationKey> completedWaitKeys;

    ReleaseResult(boolean success, LockOwnershipState ownership, Collection<LockInvocationKey> completedWaitKeys) {
        this.success = success;
        this.ownership = ownership;
        this.completedWaitKeys = unmodifiableCollection(completedWaitKeys);
    }

    static ReleaseResult successful(LockOwnershipState ownership) {
        return new ReleaseResult(true, ownership, Collections.emptyList());
    }

    static ReleaseResult successful(LockOwnershipState ownership, Collection<LockInvocationKey> notifications) {
        return new ReleaseResult(true, ownership, notifications);
    }

    static ReleaseResult failed(Collection<LockInvocationKey> notifications) {
        return new ReleaseResult(false, NOT_LOCKED, notifications);
    }

    public boolean success() {
        return success;
    }

    public LockOwnershipState ownership() {
        return ownership;
    }

    Collection<LockInvocationKey> completedWaitKeys() {
        return completedWaitKeys;
    }
}
