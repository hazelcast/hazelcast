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

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.AtomicReferenceMergeTypes;
import com.hazelcast.spi.serialization.SerializationService;

import static com.hazelcast.spi.impl.merge.MergingValueFactory.createMergingValue;

public class AtomicReferenceContainer {

    private Data value;

    public AtomicReferenceContainer() {
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

    /**
     * Merges the given {@link AtomicReferenceMergeTypes} via the given {@link SplitBrainMergePolicy}.
     *
     * @param mergingValue         the {@link AtomicReferenceMergeTypes} instance to merge
     * @param mergePolicy          the {@link SplitBrainMergePolicy} instance to apply
     * @param serializationService the {@link SerializationService} to inject dependencies
     * @return the new value if merge is applied, otherwise {@code null}
     */
    public Data merge(AtomicReferenceMergeTypes mergingValue, SplitBrainMergePolicy<Data, AtomicReferenceMergeTypes> mergePolicy,
                      boolean isExistingContainer, SerializationService serializationService) {
        serializationService.getManagedContext().initialize(mergePolicy);

        if (isExistingContainer) {
            AtomicReferenceMergeTypes existingValue = createMergingValue(serializationService, value);
            Data newValue = mergePolicy.merge(mergingValue, existingValue);
            if (newValue != null && !newValue.equals(value)) {
                value = newValue;
                return newValue;
            }
        } else {
            Data newValue = mergePolicy.merge(mergingValue, null);
            if (newValue != null) {
                value = newValue;
                return newValue;
            }
        }
        return null;
    }
}
