/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.core;

/**
 * Concurrent, distributed, partitioned, observable collection.
 *
 * @param <E> item
 */
public interface ICollection<E> extends Instance {
    /**
     * Returns the name of this collection
     *
     * @return name of this collection
     */
    String getName();

    /**
     * Adds an item listener for this collection. Listener will get notified
     * for all collection add/remove events.
     *
     * @param listener     item listener
     * @param includeValue <tt>true</tt> updated item should be passed
     *                     to the item listener, <tt>false</tt> otherwise.
     */
    void addItemListener(ItemListener<E> listener, boolean includeValue);

    /**
     * Removes the specified item listener.
     * Returns silently if the specified listener is not added before.
     *
     * @param listener item listener for this collection
     */
    void removeItemListener(ItemListener<E> listener);
}
