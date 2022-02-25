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

package com.hazelcast.cp.internal.datastructures.semaphore;

import java.util.Collection;

import static java.util.Collections.unmodifiableCollection;

/**
 * Represents result of an ISemaphore.acquire() request
 */
public final class AcquireResult {

    public enum AcquireStatus {
        /**
         * Denotes that an acquire() call has successfully acquired
         * the requested number of permits
         */
        SUCCESSFUL,

        /**
         * Denotes that a wait key is added to the wait queue
         * for an acquire() call
         */
        WAIT_KEY_ADDED,

        /**
         * Denotes that an acquire() request has not acquired the requested
         * number of permits because there is no enough permits available
         */
        FAILED
    }

    private final AcquireStatus status;

    /**
     * Number of acquired permits
     */
    private final int permits;

    /**
     * Cancelled wait keys of the caller if there is any, independent of the acquire request is successful or not.
     */
    private final Collection<AcquireInvocationKey> cancelledWaitKeys;

    AcquireResult(AcquireStatus status, int permits, Collection<AcquireInvocationKey> cancelledWaitKeys) {
        this.status = status;
        this.permits = permits;
        this.cancelledWaitKeys = unmodifiableCollection(cancelledWaitKeys);
    }

    public AcquireStatus status() {
        return status;
    }

    public int permits() {
        return permits;
    }

    Collection<AcquireInvocationKey> cancelledWaitKeys() {
        return cancelledWaitKeys;
    }

}
