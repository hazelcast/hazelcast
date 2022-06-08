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
    private final Map<Byte, WatermarkCoalescer> coalescers = new HashMap<>();

    KeyedWatermarkCoalescer() {
    }

    KeyedWatermarkCoalescer(byte[] keys, int queueCount) {
        for (byte k : keys) {
            coalescers.putIfAbsent(k, WatermarkCoalescer.create(queueCount, k));
        }
    }

    public void register(byte key, int queueCount) {
        coalescers.putIfAbsent(key, WatermarkCoalescer.create(queueCount, key));
    }

    public Set<Byte> keys() {
        return coalescers.keySet();
    }

    public Set<Entry<Byte, WatermarkCoalescer>> entries() {
        return coalescers.entrySet();
    }

    public int count() {
        return coalescers.size();
    }

    public long queueDone(byte key, int queueIndex) {
        return coalescers.get(key).queueDone(queueIndex);
    }

    private void observeEvent(byte key, int queueIndex) {
        coalescers.get(key).observeEvent(queueIndex);
    }

    public void observeEvent(int queueIndex) {
        for (Byte key : coalescers.keySet()) {
            observeEvent(key, queueIndex);
        }
    }

    public long observeWm(byte key, int queueIndex, long wmValue) {
        return coalescers.get(key).observeWm(queueIndex, wmValue);
    }

    public long checkWmHistory(byte key) {
        return coalescers.get(key).checkWmHistory();
    }

    public long coalescedWm(byte key) {
        return coalescers.get(key).coalescedWm();
    }

    public long topObservedWm(byte key) {
        return coalescers.get(key).topObservedWm();
    }
}
