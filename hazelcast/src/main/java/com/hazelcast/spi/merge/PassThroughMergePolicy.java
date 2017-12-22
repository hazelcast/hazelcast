/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.spi.SplitBrainMergeEntryView;

/**
 * Merges data structure entries from source to destination directly unless the merging entry is {@code null}.
 *
 * @since 3.10
 */
public class PassThroughMergePolicy extends AbstractMergePolicy {

    PassThroughMergePolicy() {
    }

    @Override
    public <K, V> V merge(SplitBrainMergeEntryView<K, V> mergingEntry, SplitBrainMergeEntryView<K, V> existingEntry) {
        if (mergingEntry == null) {
            return existingEntry.getValue();
        }
        return mergingEntry.getValue();
    }

    @Override
    public int getId() {
        return SplitBrainMergePolicyDataSerializerHook.PASS_THROUGH;
    }
}
