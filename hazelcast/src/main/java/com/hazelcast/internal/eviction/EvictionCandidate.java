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

package com.hazelcast.internal.eviction;

import com.hazelcast.spi.eviction.EvictableEntryView;

/**
 * Interface for entries, records or whatever that can be evictable via its accessor (key or ID).
 *
 * @param <A> Type of the accessor
 * @param <E> Type of the {@link Evictable} value
 */
public interface EvictionCandidate<A, E extends Evictable>
        extends EvictableEntryView {

    /**
     * The accessor (key or ID) of {@link Evictable} entry or record or whatever.
     *
     * @return the accessor (key or ID) of {@link Evictable} entry or record or whatever
     */
    A getAccessor();

    /**
     * The value of {@link Evictable} entry or record or whatever.
     *
     * @return the value of {@link Evictable} entry or record or whatever
     */
    E getEvictable();

}
