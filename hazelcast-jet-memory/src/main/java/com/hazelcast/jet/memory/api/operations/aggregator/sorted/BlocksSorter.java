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

package com.hazelcast.jet.memory.api.operations.aggregator.sorted;

import com.hazelcast.jet.memory.api.memory.management.MemoryBlockChain;

/**
 * Abstract sorter interface;
 */
public interface BlocksSorter {
    /**
     * Starts sorting of storage inside blocks
     */
    void startSorting();

    /**
     * @return true , if last record has been sorted,
     * false if there is more records to sort;
     */
    boolean sort();

    /**
     * Chain with sorted
     *
     * @return chain with sorted data inside memory blocks
     */
    MemoryBlockChain getMemoryChainWithSortedData();
}
