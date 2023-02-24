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

package com.hazelcast.datalink;

import com.hazelcast.core.HazelcastException;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

public class ReferenceCounter {

    private final AtomicInteger referenceCount = new AtomicInteger(1);
    private final Callable<Void> destroy;

    public ReferenceCounter(Callable<Void> destroy) {
        this.destroy = destroy;
    }

    public ReferenceCounter retain() {
        int oldCount = referenceCount.getAndIncrement();

        if (oldCount <= 0) {
            referenceCount.getAndDecrement();

            throw new IllegalStateException("Resurrected a dead object");
        }

        return this;
    }

    public boolean release() {
        long newCount = referenceCount.decrementAndGet();
        if (newCount == 0) {
            try {
                destroy.call();
            } catch (Exception e) {
                throw new HazelcastException("Could not destroy reference counted object", e);
            }

            return true;
        } else {
            return false;
        }
    }
}
