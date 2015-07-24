/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.ringbuffer;

import com.hazelcast.spi.annotation.Beta;

/**
 * The result of a {@link Ringbuffer#readManyAsync(long, int, int, com.hazelcast.core.IFunction)} operation.
 *
 * Important:
 * If an item is retrieved multiple times from the result set, a new instance is returned for every invocation. This is done
 * to prevent unexpected sharing if the {@link com.hazelcast.core.ICompletableFuture} is shared between multiple threads.
 *
 * @param <E>
 */
@Beta
public interface ReadResultSet<E> extends Iterable<E> {

    /**
     * Returns the number of items that have been read before filtering.
     *
     * If no filter is set, then the readCount will be the same as size. But if a filter is applied, it could be that items
     * are read, but are filtered out. So if you are trying to make another read based on the ReadResultSet then you should
     * increment the sequence by readCount and not by size. Otherwise you will be re-reading the same filtered messages.
     *
     * @return the number of items read (including the filtered ones).
     */
    int readCount();

    /**
     * Gets the item at the given index.
     *
     * @param index the index
     * @return the found item.
     * @throws IllegalArgumentException if index out of bounds.
     */
    E get(int index);
}
