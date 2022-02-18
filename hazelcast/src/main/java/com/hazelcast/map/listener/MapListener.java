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

package com.hazelcast.map.listener;

import java.util.EventListener;

/**
 * <p>A marker interface which is used to get
 * notified upon a map or an entry event.
 * <p>Specifically:
 * <ul>
 * <li>
 * A map event is fired as a result of map-wide operations. This
 * kind of operations possibly run on multiple map entries, like
 * e.g. {@link com.hazelcast.core.EntryEventType#CLEAR_ALL}.
 * {@link com.hazelcast.core.EntryEventType#EVICT_ALL}.
 * </li>
 * <li>
 * An entry event is fired after an operation on a single entry
 * like e.g. {@link com.hazelcast.core.EntryEventType#ADDED},
 * {@link com.hazelcast.core.EntryEventType#UPDATED}
 * </li>
 * </ul>
 * <p>An implementer of this interface should extend one of its
 * sub-interfaces to receive a corresponding event.
 *
 * @see MapClearedListener
 * @see MapEvictedListener
 * @see EntryAddedListener
 * @see EntryEvictedListener
 * @see EntryExpiredListener
 * @see EntryRemovedListener
 * @see EntryMergedListener
 * @see EntryUpdatedListener
 * @see EntryLoadedListener
 * @since 3.5
 */
public interface MapListener extends EventListener {
}
