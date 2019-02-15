/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

/**
 * Returns a class with the provided merge value types.
 * <p>
 * Is implemented by config classes of split-brain capable data structures.
 * <p>
 * Is used by the {@link com.hazelcast.internal.config.ConfigValidator} to
 * check if a data structure provides he required merge types of a
 * configured {@link SplitBrainMergePolicy}.
 *
 * @param <T> type of the provided merging values, e.g. a simple {@code MergingValue}
 *            or a composition like {@code MergingEntry & MergingHits & MergingLastAccessTime}
 * @since 3.10
 */
public interface SplitBrainMergeTypeProvider<T extends MergingValue> {

    Class<T> getProvidedMergeTypes();
}
