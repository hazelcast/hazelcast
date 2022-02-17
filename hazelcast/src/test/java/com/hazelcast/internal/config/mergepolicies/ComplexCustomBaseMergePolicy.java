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

package com.hazelcast.internal.config.mergepolicies;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.merge.MergingCosts;
import com.hazelcast.spi.merge.MergingExpirationTime;
import com.hazelcast.spi.merge.MergingValue;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;

import java.io.IOException;

@SuppressWarnings("unused")
public class ComplexCustomBaseMergePolicy<V, C extends Comparable<V>, S extends String,
        T extends MergingValue<V> & MergingExpirationTime & MergingCosts, U extends Comparable<T>>
        implements SplitBrainMergePolicy<V, T, Object> {

    @Override
    public Object merge(T mergingValue, T existingValue) {
        return null;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
    }
}
