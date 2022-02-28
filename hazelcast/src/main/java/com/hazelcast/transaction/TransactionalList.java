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

package com.hazelcast.transaction;

import com.hazelcast.collection.IList;
import com.hazelcast.config.SplitBrainProtectionConfig;

/**
 * Transactional implementation of {@link IList}.
 * <p>
 * Supports split brain protection {@link SplitBrainProtectionConfig} since 3.10 in
 * cluster versions 3.10 and higher.
 *
 * @param <E> the type of elements maintained by this list
 */
public interface TransactionalList<E> extends TransactionalObject {

    /**
     * Adds a new item to the transactional list.
     *
     * @param e the new item added to the transactional list
     * @return {@code true} if the item is added successfully
     */
    boolean add(E e);

    /**
     * Removes an item from the transactional list.
     *
     * @param e item to remove the transactional list
     * @return {@code true} if the item is removed successfully
     */
    boolean remove(E e);

    /**
     * Returns the size of the list.
     *
     * @return the size of the list
     */
    int size();
}
