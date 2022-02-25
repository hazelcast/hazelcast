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

package com.hazelcast.map.impl.mapstore.writebehind;

import com.hazelcast.map.ReachedMaxSizeException;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.spi.properties.HazelcastProperties;

import java.util.concurrent.atomic.AtomicLong;

import static java.lang.String.format;

/**
 * Holds sum of all current write-behind-queue sizes(including
 * backup partitions) of all maps on this node. This value is useful
 * to put a higher limit on total number of entries that can exist
 * in all write-behind-queues to prevent OOME.
 *
 * There is only one counter instance per node.
 */
public class NodeWideUsedCapacityCounter {
    private final long maxPerNodeCapacity;
    private final AtomicLong nodeWideUsedCapacityCounter = new AtomicLong(0);

    public NodeWideUsedCapacityCounter(HazelcastProperties properties) {
        this.maxPerNodeCapacity = properties.getLong(ClusterProperty.MAP_WRITE_BEHIND_QUEUE_CAPACITY);
    }

    /**
     * Increments/decrements counter by adding supplied delta.
     * When counter is exceeded preconfigured node-wide limit,
     * this method throws {@link ReachedMaxSizeException}.
     *
     * @param delta capacity to be added or subtracted.
     * @throws ReachedMaxSizeException
     */
    public void checkAndAddCapacityOrThrowException(int delta) {
        if (delta == 0) {
            return;
        }

        long currentCapacity = nodeWideUsedCapacityCounter.get();
        long newCapacity = currentCapacity + delta;

        if (newCapacity < 0) {
            return;
        }

        if (delta > 0 && maxPerNodeCapacity < newCapacity) {
            throwException(currentCapacity, maxPerNodeCapacity, delta);
        }

        while (!nodeWideUsedCapacityCounter.compareAndSet(currentCapacity, newCapacity)) {
            currentCapacity = nodeWideUsedCapacityCounter.get();
            newCapacity = currentCapacity + delta;

            if (newCapacity < 0) {
                return;
            }

            if (delta > 0 && maxPerNodeCapacity < newCapacity) {
                throwException(currentCapacity, maxPerNodeCapacity, delta);
            }
        }
    }

    private static void throwException(long currentCapacity, long maxPerNodeCapacity, int requiredCapacity) {
        String msg = format("Reached node-wide write-behind-queue max capacity [max=%d, current=%d, required=%d]",
                maxPerNodeCapacity, currentCapacity, requiredCapacity);

        throw new ReachedMaxSizeException(msg);
    }

    /**
     * Increments/decrements current value
     * of node-wide-used-capacity counter by adding supplied delta.
     */
    public void add(long delta) {
        nodeWideUsedCapacityCounter.addAndGet(delta);
    }

    /**
     * @return current value of node-wide-used-capacity counter
     */
    public long currentValue() {
        return nodeWideUsedCapacityCounter.get();
    }
}
