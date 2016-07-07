/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.memory.binarystorage;

import com.hazelcast.jet.memory.binarystorage.cursor.SlotAddressCursor;

/**
 * Sorted binary key-value storage. Once the storage is sorted, it is no longer usable
 * for random-access lookup by key.
 */
public interface SortedStorage extends Storage {

    /** Makes this flyweight instance aware of the pre-existing sort order of the underlying binary storage. */
    void setAlreadySorted(SortOrder sortOrder);

    /**
     * Ensures that the uderlying data structure is sorted. If it is not already sorted,
     * it will be sorted in the supplied preferred order; otherwise it will be left as-is.
     * Since all iteration methods accept an explicit sort order parameter, the iteration
     * order can be adjusted with respect to the pre-existing sort order.
     *
     * @param preferredOrder the preferred sort order
     */
    void ensureSorted(SortOrder preferredOrder);

    /**
     * @param sortOrder the desired iteration order (ascending or descending)
     * @return address of the first slot in the given iteration order
     */
    long addrOfFirstSlot(SortOrder sortOrder);

    /**
     * @param sortOrder the desired iteration order (ascending or descending)
     * @param slotAddress current slot address
     * @return address of the next slot in the given iteration order
     */
    long addrOfNextSlot(long slotAddress, SortOrder sortOrder);

    /**
     * @param sortOrder the desired iteration order (ascending or descending)
     * @return an iterator which will encounter entries in the specified order
     */
    SlotAddressCursor slotCursor(SortOrder sortOrder);
}
