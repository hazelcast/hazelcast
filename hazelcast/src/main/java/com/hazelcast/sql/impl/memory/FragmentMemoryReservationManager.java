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

import java.util.HashMap;
import java.util.Map;

/**
 * Reservation manager which manages reservations on fragment level.
 */
public class FragmentMemoryReservationManager {
    private final GlobalMemoryReservationManager parent;
    private final Map<Integer, OperatorMemoryReservationManager> operatorManagers = new HashMap<>();
    private int nextOperatorId;

    public FragmentMemoryReservationManager(GlobalMemoryReservationManager parent) {
        this.parent = parent;
    }

    public OperatorMemoryReservationManager newOperatorManager(long limit) {
        int operatorId = nextOperatorId++;

        OperatorMemoryReservationManager operatorManager = new OperatorMemoryReservationManager(this, operatorId, limit);

        operatorManagers.put(operatorId, operatorManager);

        return operatorManager;
    }

    public long reserve(int operatorId, long size) {
        long res = parent.reserve(size);

        if (res == -1) {
            // Failed to allocate memory. Try to release something from other operators.
            long remaining = size;

            for (OperatorMemoryReservationManager operatorManager : operatorManagers.values()) {
                if (operatorManager.getId() == operatorId) {
                    // Do not release self.
                    continue;
                }

                remaining -= operatorManager.tryRelease(size);

                if (remaining == 0) {
                    break;
                }
            }

            if (remaining != size) {
                // If we were able to release at least some bytes, then optimistically try to allocate again.
                res = parent.reserve(size);
            }
        }

        return res;
    }

    public void release(int size) {
        parent.release(size);
    }
}
