/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.dataconnection.impl;

import com.hazelcast.core.HazelcastException;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A closeable reference counter to handle closing of a resource after all
 * references are released. The initial refCount is 1. It is incremented by
 * calling {@link #retain()} and decremented by calling {@link #release()},
 * which must be paired. When the refCount gets to 0, the {@link #destroyAction}
 * is ran, and it is not possible to call {@link #retain()} afterwards.
 * <p>
 * It is useful to delay closing of a resource provider itself until all
 * resources it provided are closed too. The initial refCount of 1 is for the
 * resource provider itself, other retain/release calls are for other resources.
 * The resource then has a `close` method which releases the initial reference,
 * but the destroy action is called only after all other resources are released
 * too.
 */
public class ReferenceCounter {

    private final AtomicInteger referenceCount = new AtomicInteger(1);
    private final Runnable destroyAction;

    public ReferenceCounter(Runnable destroyAction) {
        this.destroyAction = destroyAction;
    }

    /**
     * Increments the reference count
     *
     * @throws IllegalStateException when called after the reference count reached 0
     */
    public void retain() {
        referenceCount.updateAndGet(cnt -> {
            if (cnt <= 0) {
                throw new IllegalStateException("Resurrected a dead object");
            }
            return cnt + 1;
        });
    }

    /**
     * Decrements the reference count
     *
     * @return true if the counter reached 0, false otherwise
     */
    public boolean release() {
        long newCount = referenceCount.decrementAndGet();
        if (newCount < 0) {
            throw new IllegalStateException("release without retain");
        }
        if (newCount == 0) {
            try {
                destroyAction.run();
            } catch (Exception e) {
                throw new HazelcastException("Could not destroy reference counted object: " + e, e);
            }

            return true;
        } else {
            return false;
        }
    }
}
