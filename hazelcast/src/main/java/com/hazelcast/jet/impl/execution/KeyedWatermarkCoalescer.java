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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.jet.core.Watermark;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.hazelcast.jet.impl.execution.WatermarkCoalescer.IDLE_MESSAGE;
import static com.hazelcast.jet.impl.execution.WatermarkCoalescer.IDLE_MESSAGE_TIME;
import static com.hazelcast.jet.impl.execution.WatermarkCoalescer.NO_NEW_WM;
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
    private final boolean[] idleQueues;

    KeyedWatermarkCoalescer(int queueCount) {
        this.queueCount = queueCount;
        this.idleQueues = new boolean[queueCount];
    }

    public Set<Byte> keys() {
        return coalescers.keySet();
    }

    private WatermarkCoalescer coalescer(byte key) {
        return coalescers.computeIfAbsent(key, x -> {
            WatermarkCoalescer wc = WatermarkCoalescer.create(queueCount);
            for (Integer queueIndex : doneQueues) {
                wc.queueDone(queueIndex);
            }
            for (int i = 0; i < idleQueues.length; ++i) {
                if (idleQueues[i]) {
                    wc.observeWm(i, IDLE_MESSAGE_TIME);
                }
            }
            return wc;
        });
    }

    public List<Watermark> queueDone(int queueIndex) {
        // Store the index of the queue that is finished, so that we can use it for coalescers
        // for WM keys we didn't yet observe, when they are lazily created.
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
        idleQueues[queueIndex] = false;
    }

    public List<Watermark> observeWm(int queueIndex, Watermark watermark) {
        if (watermark.equals(IDLE_MESSAGE)) {
            idleQueues[queueIndex] = true;
            boolean allIdle = true;
            // we need to track idle queues for the case when there's no coalescer yet
            for (int i = 0; i < idleQueues.length; i++) {
                if (!doneQueues.contains(i)) {
                    allIdle &= idleQueues[i];
                }
            }

            List<Watermark> watermarks = new ArrayList<>();
            for (Entry<Byte, WatermarkCoalescer> coalescerEntry : coalescers.entrySet()) {
                long observedWm = coalescerEntry.getValue().observeWm(queueIndex, watermark.timestamp());
                assert observedWm != IDLE_MESSAGE_TIME;
                if (observedWm != NO_NEW_WM) {
                    watermarks.add(new Watermark(observedWm, coalescerEntry.getKey()));
                }
                assert coalescerEntry.getValue().idleMessagePending() == allIdle;
            }

            if (allIdle) {
                watermarks.add(IDLE_MESSAGE);
            }
            return watermarks;
        }

        idleQueues[queueIndex] = false;

        WatermarkCoalescer c = coalescer(watermark.key());
        long newWmValue = c.observeWm(queueIndex, watermark.timestamp());
        assert !c.idleMessagePending();
        if (newWmValue == NO_NEW_WM) {
            return emptyList();
        }
        return singletonList(new Watermark(newWmValue, watermark.key()));
    }

    public long coalescedWm(byte key) {
        return coalescer(key).coalescedWm();
    }

    public long topObservedWm(byte key) {
        return coalescer(key).topObservedWm();
    }
}
