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

package com.hazelcast.sql.impl.memory;

import java.util.ArrayList;
import java.util.List;

/**
 * Reservation manager for specific operator.
 */
public class OperatorMemoryReservationManager {
    private final FragmentMemoryReservationManager parent;
    private final int id;
    private final long limit;
    private int allocated;
    private List<Releasable> tasks;

    public OperatorMemoryReservationManager(FragmentMemoryReservationManager parent, int id, long limit) {
        this.parent = parent;
        this.id = id;
        this.limit = limit;
    }

    public int getId() {
        return id;
    }

    public long reserve(long size) {
        if (limit > 0 && size + allocated - limit > 0) {
            return -1;
        }

        long res = parent.reserve(id, size);

        if (res != -1) {
            allocated += res;
        }

        return res;
    }

    public void release(int size) {
        parent.release(size);

        allocated -= size;
    }

    public void registerReleasable(Releasable task) {
        if (tasks == null) {
            tasks = new ArrayList<>();
        }

        tasks.add(task);
    }

    /**
     * Try offloading some memory to disk.
     *
     * @param size Amount of memory to offload.
     * @return Actually offloaded memory.
     */
    public int tryRelease(long size) {
        int released = 0;

        if (tasks != null) {
            for (Releasable task : tasks) {
                released += task.tryRelease(size - released);

                if (released >= size) {
                    break;
                }
            }
        }

        return released;
    }
}
