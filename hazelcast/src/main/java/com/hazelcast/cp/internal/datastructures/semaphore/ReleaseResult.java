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
import java.util.Collections;

import static java.util.Collections.unmodifiableCollection;

/**
 * Represents result of an ISemaphore.release() request
 */
final class ReleaseResult {

    /**
     * true if the release() request is successful
     */
    private final boolean success;

    /**
     * If the release() request is successful and permits are assigned to some other endpoints, contains their wait keys.
     */
    private final Collection<AcquireInvocationKey> acquiredWaitKeys;

    /**
     * Cancelled wait keys of the caller if there is any, independent of the release request is successful or not.
     */
    private final Collection<AcquireInvocationKey> cancelledWaitKeys;

    private ReleaseResult(boolean success, Collection<AcquireInvocationKey> acquiredWaitKeys,
                          Collection<AcquireInvocationKey> cancelledWaitKeys) {
        this.success = success;
        this.acquiredWaitKeys = unmodifiableCollection(acquiredWaitKeys);
        this.cancelledWaitKeys = unmodifiableCollection(cancelledWaitKeys);
    }

    static ReleaseResult successful(Collection<AcquireInvocationKey> acquiredWaitKeys,
                                            Collection<AcquireInvocationKey> cancelledWaitKeys) {
        return new ReleaseResult(true, acquiredWaitKeys, cancelledWaitKeys);
    }

    static ReleaseResult failed(Collection<AcquireInvocationKey> cancelledWaitKeys) {
        return new ReleaseResult(false, Collections.<AcquireInvocationKey>emptyList(), cancelledWaitKeys);
    }

    public boolean success() {
        return success;
    }

    public Collection<AcquireInvocationKey> acquiredWaitKeys() {
        return acquiredWaitKeys;
    }

    public Collection<AcquireInvocationKey> cancelledWaitKeys() {
        return cancelledWaitKeys;
    }
}
