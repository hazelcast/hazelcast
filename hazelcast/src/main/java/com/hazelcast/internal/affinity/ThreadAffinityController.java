/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.affinity;

import com.hazelcast.logging.ILogger;
import net.openhft.affinity.AffinityLock;

import java.util.Set;

public class ThreadAffinityController {

    private final ILogger logger;
    private final ThreadLocal<Entry> assignments = new ThreadLocal<>();
    private final ThreadAffinity.Group group;
    private final CpuIdPool cores;

    ThreadAffinityController(ILogger logger, ThreadAffinity type) {
        this.logger = logger;
        this.group = type.value();
        Set<Integer> assignedCoreIds = ThreadAffinityProperties.getCoreIds(group);
        if (assignedCoreIds.isEmpty()) {
            logger.warning("No assigned core ids for type " + group + ". Affinity enabled in NOOP mode.");
        }
        this.cores = new CpuIdPool(assignedCoreIds);
    }

    public void assign() {
        if (assignments.get() != null) {
            throw new IllegalStateException("Affinity lock re-enter attempt " + Thread.currentThread());
        }

        int assignedId = cores.take();
        if (assignedId == -1) {
            return;
        }

        logger.fine("Assigning affinity on " + Thread.currentThread().getName() + " of type: "
                + group + " to core: " + assignedId + ".");
        Entry entry = new Entry(assignedId, AffinityLock.acquireLock(assignedId));
        assignments.set(entry);
    }

    public void release() {
        Entry entry = assignments.get();
        assignments.remove();

        if (entry != null) {
            entry.lock.release();
            cores.release(entry.coreId);
        }
    }

    @Override
    public String toString() {
        return "AffinityEnforcerThreadInterceptor{" + "type='" + group + '\'' + ", cores=" + cores + '}';
    }

    private class Entry {
        private final int coreId;
        private final AffinityLock lock;

        public Entry(int coreId, AffinityLock lock) {
            this.coreId = coreId;
            this.lock = lock;
        }
    }
}
