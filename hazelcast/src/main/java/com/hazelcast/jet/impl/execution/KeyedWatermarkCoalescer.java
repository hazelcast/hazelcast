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

import com.hazelcast.jet.core.Watermark;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.hazelcast.jet.impl.execution.WatermarkCoalescer.IDLE_MESSAGE_TIME;
import static com.hazelcast.jet.impl.execution.WatermarkCoalescer.NO_NEW_WM;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

/**
 * A watermark coalescer for multiple WM keys that can occur on input streams.
 * The set of WM keys is initialized lazily, as they appear.
 */
public class KeyedWatermarkCoalescer {
    private final int queueCount;
    private final Map<Byte, WatermarkCoalescer> coalescers = new HashMap<>();
    private final Set<Integer> doneQueues = new HashSet<>();

    KeyedWatermarkCoalescer(int queueCount) {
        this.queueCount = queueCount;
    }

    private WatermarkCoalescer coalescer(byte key) {
        return coalescers.computeIfAbsent(key, x -> {
            WatermarkCoalescer wc = WatermarkCoalescer.create(queueCount);
            for (Integer queueIndex : doneQueues) {
                wc.queueDone(queueIndex);
            }
            return wc;
        });
    }

//    public Set<Entry<Byte, WatermarkCoalescer>> entries() {
//        return coalescers.entrySet();
//    }

    public List<Watermark> queueDone(int queueIndex) {
        doneQueues.add(queueIndex);
        List<Watermark> result = new ArrayList<>();
        for (Entry<Byte, WatermarkCoalescer> entry : coalescers.entrySet()) {
            long value = entry.getValue().queueDone(queueIndex);
            if (value != NO_NEW_WM) {
                result.add(new Watermark(value, entry.getKey()));
            }
        }

        return result;
    }

    public void observeEvent(int queueIndex) {
        for (WatermarkCoalescer c : coalescers.values()) {
            c.observeEvent(queueIndex);
        }
    }

    public List<Watermark> observeWm(byte key, int queueIndex, long wmValue) {
        WatermarkCoalescer c = coalescer(key);
        long newWmValue = c.observeWm(queueIndex, wmValue);
        if (newWmValue == NO_NEW_WM) {
            return c.idleMessagePending()
                    ? singletonList(new Watermark(IDLE_MESSAGE_TIME, key))
                    : emptyList();
        }
        Watermark newWm = new Watermark(newWmValue, key);
        return c.idleMessagePending()
                ? asList(newWm, new Watermark(IDLE_MESSAGE_TIME, key))
                : singletonList(newWm);
    }

    public long coalescedWm(byte key) {
        return coalescer(key).coalescedWm();
    }

    public long topObservedWm(byte key) {
        return coalescer(key).topObservedWm();
    }
}
