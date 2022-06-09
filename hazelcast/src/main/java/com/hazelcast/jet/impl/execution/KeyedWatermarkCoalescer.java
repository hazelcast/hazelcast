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

package com.hazelcast.jet.impl.execution;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class KeyedWatermarkCoalescer {
    private final int queueCount;
    private final Map<Byte, WatermarkCoalescer> coalescers = new HashMap<>();

    KeyedWatermarkCoalescer(int queueCount) {
        this.queueCount = queueCount;
    }

    private WatermarkCoalescer coalescer(byte key) {
        return coalescers.computeIfAbsent(key, x -> WatermarkCoalescer.create(queueCount));
    }

    public Set<Byte> keys() {
        return coalescers.keySet();
    }

    public Set<Entry<Byte, WatermarkCoalescer>> entries() {
        return coalescers.entrySet();
    }

    public long queueDone(byte key, int queueIndex) {
        return coalescer(key).queueDone(queueIndex);
    }

    public void observeEvent(int queueIndex) {
        for (WatermarkCoalescer c : coalescers.values()) {
            c.observeEvent(queueIndex);
        }
    }

    public long observeWm(byte key, int queueIndex, long wmValue) {
        return coalescer(key).observeWm(queueIndex, wmValue);
    }

    public boolean idleMessagePending(byte key) {
        return coalescer(key).idleMessagePending();
    }

    public long coalescedWm(byte key) {
        return coalescer(key).coalescedWm();
    }

    public long topObservedWm(byte key) {
        return coalescer(key).topObservedWm();
    }
}
