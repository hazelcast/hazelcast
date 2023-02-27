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
import com.hazelcast.jet.core.Processor;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Reference counter to handle closing of shared / pooled instance
 * of physical connection in {@link DataLink}.
 * <p>
 * Each call to {@link #retain()} method must be matched by a call to
 * {@link #release()} method.
 * <p>
 * The typical implementation using this ReferenceCounter class will look
 * as follows:
 * <ul>
 * <li>a new DataLink is created with counter=1</li>
 * <li>when the {@link DataLinkService#getDataLink} is called the counter
 * is incremented (the service calls the {@link #retain()} method)</li>
 * <li>the caller must call `close()` on this data link after it's done
 * with it (e.g. when it retrieves the physical instance, or after the
 * processor has finished and {@link Processor#close()} is called).
 * The implementation of the DataLink should delegate the close method
 * to {@link #release()}</li>
 * <li>when using a pooled instance, {@link #retain()} must be called
 * when an instance is borrowed from the pool</li>
 * <li>when an instance is returned to the pool, {@link #release()} must
 * be called</li>
 * <li>the data link is closed when the member shuts down or when it
 * is removed, causing the reference count to reach zero, and destroys
 * the instance</li>
 * </ul>
 * <p>
 * We increase the counter both in DataLinkService and DataLink to avoid race condition when
 * <ul>
 * <li>a DataLink is retrieved from the service</li>
 * <li>the DataLink is removed and physical instance is closed</li>
 * <li>a closed instance is returned</li>
 * </ul>
 */
public class ReferenceCounter {

    private final AtomicInteger referenceCount = new AtomicInteger(1);
    private final Runnable destroy;

    public ReferenceCounter(Runnable destroy) {
        this.destroy = destroy;
    }

    /**
     * Increments the reference count
     *
     * @throws IllegalStateException when called after the reference count reached 0
     */
    public void retain() {
        int oldCount = referenceCount.getAndIncrement();

        if (oldCount <= 0) {
            referenceCount.getAndDecrement();

            throw new IllegalStateException("Resurrected a dead object");
        }
    }

    /**
     * Decrements the reference count
     *
     * @return true if the counter reached 0, false otherwise
     */
    public boolean release() {
        long newCount = referenceCount.decrementAndGet();
        if (newCount == 0) {
            try {
                destroy.run();
            } catch (Exception e) {
                throw new HazelcastException("Could not destroy reference counted object", e);
            }

            return true;
        } else {
            return false;
        }
    }
}
