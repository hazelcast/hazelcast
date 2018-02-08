/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.concurrent.atomiclong;

import com.hazelcast.config.AtomicLongConfig;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.SplitBrainAwareDataContainer;
import com.hazelcast.spi.SplitBrainMergeEntryView;
import com.hazelcast.spi.SplitBrainMergePolicy;
import com.hazelcast.spi.serialization.SerializationService;

import static com.hazelcast.spi.impl.merge.SplitBrainEntryViews.createSplitBrainMergeEntryView;

public class AtomicLongContainer implements SplitBrainAwareDataContainer<Boolean, Long, Long> {

    private final String name;
    private final AtomicLongConfig config;
    private final SerializationService serializationService;

    private long value;

    public AtomicLongContainer(String name, NodeEngine nodeEngine) {
        this.name = name;
        this.config = nodeEngine.getConfig().findAtomicLongConfig(name);
        this.serializationService = nodeEngine.getSerializationService();
    }

    public String getName() {
        return name;
    }

    public AtomicLongConfig getConfig() {
        return config;
    }

    public long get() {
        return value;
    }

    public long addAndGet(long delta) {
        value += delta;
        return value;
    }

    public void set(long value) {
        this.value = value;
    }

    public boolean compareAndSet(long expect, long value) {
        if (this.value != expect) {
            return false;
        }
        this.value = value;
        return true;
    }

    public long getAndAdd(long delta) {
        long tempValue = value;
        value += delta;
        return tempValue;
    }

    public long getAndSet(long value) {
        long tempValue = this.value;
        this.value = value;
        return tempValue;
    }

    @Override
    public Long merge(SplitBrainMergeEntryView<Boolean, Long> mergingEntry, SplitBrainMergePolicy mergePolicy) {
        mergePolicy.setSerializationService(serializationService);

        if (mergingEntry.getKey()) {
            SplitBrainMergeEntryView<Boolean, Long> existingEntry = createSplitBrainMergeEntryView(true, value);
            Long newValue = mergePolicy.merge(mergingEntry, existingEntry);
            if (newValue != null && !newValue.equals(value)) {
                value = newValue;
                return newValue;
            }
        } else {
            Long newValue = mergePolicy.merge(mergingEntry, null);
            if (newValue != null) {
                value = newValue;
                return newValue;
            }
        }
        return null;
    }
}
