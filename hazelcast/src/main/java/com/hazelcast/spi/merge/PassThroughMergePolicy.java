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

package com.hazelcast.spi.merge;

import com.hazelcast.spi.impl.merge.AbstractSplitBrainMergePolicy;
import com.hazelcast.spi.impl.merge.SplitBrainDataSerializerHook;

/**
 * Merges data structure entries from source to destination directly unless the merging entry is {@code null}.
 *
 * @param <V> the type of the merged value
 * @param <T> the type of the merging value
 * @since 3.10
 */
public class PassThroughMergePolicy<V, T extends MergingValue<V>> extends AbstractSplitBrainMergePolicy<V, T, Object> {

    public PassThroughMergePolicy() {
    }

    @Override
    public Object merge(T mergingValue, T existingValue) {
        if (mergingValue == null) {
            return existingValue.getRawValue();
        }
        return mergingValue.getRawValue();
    }

    @Override
    public int getClassId() {
        return SplitBrainDataSerializerHook.PASS_THROUGH;
    }
}
