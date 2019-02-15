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
 * Represents a read-only view of a last update time for the merging process after a split-brain.
 *
 * @param <V> the type of the value
 * @since 3.10
 */
public interface MergingLastUpdateTime<V> extends MergingValue<V> {

    /**
     * Returns the last update time of the merge data.
     *
     * @return the last update time of the merge data
     */
    long getLastUpdateTime();
}
