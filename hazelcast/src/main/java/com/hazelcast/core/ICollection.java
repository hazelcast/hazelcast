/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import java.util.Collection;

/**
 * Concurrent, distributed, partitioned, listenable collection.
 *
 * @param <E> item
 */
public interface ICollection<E> extends Collection<E>, DistributedObject {

    /**
     * Returns the name of this collection.
     *
     * @return the name of this collection
     */
    String getName();

    /**
     * Adds an item listener for this collection.
     * The listener will be notified for all collection add/remove events.
     *
     * @param listener     the item listener
     * @param includeValue {@code true} if the updated item should be passed
     *                     to the item listener, {@code false} otherwise
     * @return returns the registration ID
     */
    String addItemListener(ItemListener<E> listener, boolean includeValue);

    /**
     * Removes the specified item listener.
     * Returns silently if the specified listener was not added before.
     *
     * @param registrationId ID of the listener registration
     * @return {@code true} if the item listener is removed, {@code false} otherwise
     */
    boolean removeItemListener(String registrationId);
}
