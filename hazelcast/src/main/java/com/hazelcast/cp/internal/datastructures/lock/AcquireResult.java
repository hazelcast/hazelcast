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

import com.hazelcast.cp.lock.FencedLock;

import java.util.Collection;
import java.util.Collections;

import static com.hazelcast.cp.lock.FencedLock.INVALID_FENCE;
import static com.hazelcast.cp.internal.datastructures.lock.AcquireResult.AcquireStatus.FAILED;
import static com.hazelcast.cp.internal.datastructures.lock.AcquireResult.AcquireStatus.SUCCESSFUL;
import static com.hazelcast.cp.internal.datastructures.lock.AcquireResult.AcquireStatus.WAIT_KEY_ADDED;

/**
 * Represents result of a lock() request
 */
public class AcquireResult {

    public enum AcquireStatus {
        /**
         * Denotes that a lock() call has successfully acquired the lock
         */
        SUCCESSFUL,

        /**
         * Denotes that a wait key is added to the wait queue for a lock() call
         */
        WAIT_KEY_ADDED,

        /**
         * Denotes that a lock() request has not acquired the lock, either
         * because the lock is held by someone else or the lock acquire limit
         * is reached.
         */
        FAILED
    }

    private final AcquireStatus status;

    /**
     * If the lock() request is successful, represents the new fencing token.
     * It is {@link FencedLock#INVALID_FENCE} otherwise.
     */
    private final long fence;

    /**
     * If new a lock() request is send while there are pending wait keys of a previous lock() request,
     * pending wait keys are cancelled. It is because LockEndpoint is a single-threaded entity and
     * a new lock() request implies that the LockEndpoint is no longer interested in its previous lock() call.
     */
    private final Collection<LockInvocationKey> cancelledWaitKeys;

    AcquireResult(AcquireStatus status, long fence, Collection<LockInvocationKey> cancelledWaitKeys) {
        this.status = status;
        this.fence = fence;
        this.cancelledWaitKeys = Collections.unmodifiableCollection(cancelledWaitKeys);
    }

    static AcquireResult acquired(long fence) {
        return new AcquireResult(SUCCESSFUL, fence, Collections.<LockInvocationKey>emptyList());
    }

    static AcquireResult failed(Collection<LockInvocationKey> cancelled) {
        return new AcquireResult(FAILED, INVALID_FENCE, cancelled);
    }

    static AcquireResult waitKeyAdded(Collection<LockInvocationKey> cancelled) {
        return new AcquireResult(WAIT_KEY_ADDED, INVALID_FENCE, cancelled);
    }

    public AcquireStatus status() {
        return status;
    }

    public long fence() {
        return fence;
    }

    Collection<LockInvocationKey> cancelledWaitKeys() {
        return cancelledWaitKeys;
    }
}
