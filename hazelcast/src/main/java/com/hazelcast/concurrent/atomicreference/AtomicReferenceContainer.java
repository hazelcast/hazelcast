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

package com.hazelcast.concurrent.atomicreference;

import com.hazelcast.config.AtomicReferenceConfig;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.MergeDataHolder;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.spi.serialization.SerializationServiceAware;

import static com.hazelcast.spi.merge.MergeDataHolders.createSplitBrainMergeEntryView;

public class AtomicReferenceContainer {

    private final String name;
    private final AtomicReferenceConfig config;
    private final SerializationService serializationService;

    private Data value;

    public AtomicReferenceContainer(NodeEngine nodeEngine, String name) {
        this.name = name;
        this.config = nodeEngine.getConfig().findAtomicReferenceConfig(name);
        this.serializationService = nodeEngine.getSerializationService();
    }

    public String getName() {
        return name;
    }

    public AtomicReferenceConfig getConfig() {
        return config;
    }

    public Data get() {
        return value;
    }

    public void set(Data value) {
        this.value = value;
    }

    public boolean compareAndSet(Data expect, Data value) {
        if (!contains(expect)) {
            return false;
        }
        this.value = value;
        return true;
    }

    public boolean contains(Data expected) {
        if (value == null) {
            return expected == null;
        }
        return value.equals(expected);
    }

    public Data getAndSet(Data value) {
        Data tempValue = this.value;
        this.value = value;
        return tempValue;
    }

    public boolean isNull() {
        return value == null;
    }

    public Data merge(MergeDataHolder<Data> mergeDataHolder, SplitBrainMergePolicy mergePolicy, boolean isExistingContainer) {
        if (mergePolicy instanceof SerializationServiceAware) {
            ((SerializationServiceAware) mergePolicy).setSerializationService(serializationService);
        }

        if (isExistingContainer) {
            MergeDataHolder<Data> existingEntry = createSplitBrainMergeEntryView(true, value);
            Data newValue = mergePolicy.merge(mergeDataHolder, existingEntry);
            if (newValue != null && !newValue.equals(value)) {
                value = newValue;
                return newValue;
            }
        } else {
            Data newValue = mergePolicy.merge(mergeDataHolder, null);
            if (newValue != null) {
                value = newValue;
                return newValue;
            }
        }
        return null;
    }
}
