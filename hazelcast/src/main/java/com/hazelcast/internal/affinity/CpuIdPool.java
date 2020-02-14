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

import java.util.Collections;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Set;

class CpuIdPool {

    private final Set<Integer> ids;
    private final LinkedList<Integer> available;

    CpuIdPool(Set<Integer> ids) {
        this.ids = Collections.unmodifiableSet(ids);
        this.available = new LinkedList<>(ids);
    }

    public synchronized int take() {
        try {
            return available.removeFirst();
        } catch (NoSuchElementException e) {
            return -1;
        }
    }

    public synchronized void release(int id) {
        available.push(id);
    }

    @Override
    public synchronized String toString() {
        return "CpuIdPool{" + "assigned=" + ids + ", available=" + available + '}';
    }
}
