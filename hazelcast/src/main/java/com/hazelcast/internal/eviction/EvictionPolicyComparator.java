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

package com.hazelcast.internal.eviction;

import java.io.Serializable;
import java.util.Comparator;

/**
 * A kind of {@link java.util.Comparator} to be used while comparing
 * entries to be evicted.
 */
public abstract class EvictionPolicyComparator<K, V, E extends EvictableEntryView<K, V>>
        implements Comparator<E>, Serializable {

    /**
     * Integer constant for representing behaviour for giving higher priority to first entry to be evicted.
     */
    public static final int FIRST_ENTRY_HAS_HIGHER_PRIORITY_TO_BE_EVICTED = -1;

    /**
     * Integer constant for representing behaviour for giving higher priority to second entry to be evicted.
     */
    public static final int SECOND_ENTRY_HAS_HIGHER_PRIORITY_TO_BE_EVICTED = +1;

    /**
     * Integer constant for representing behaviour for giving same priority to both of entry to be evicted.
     */
    public static final int BOTH_OF_ENTRIES_HAVE_SAME_PRIORITY_TO_BE_EVICTED = 0;

    /**
     * Compares the given {@link EvictableEntryView} instances and returns the result.
     * The result should be one of the
     * <ul>
     *   <li>#FIRST_ENTRY_HAS_HIGHER_PRIORITY_TO_BE_EVICTED</li>
     *   <li>#SECOND_ENTRY_HAS_HIGHER_PRIORITY_TO_BE_EVICTED</li>
     *   <li>#BOTH_OF_ENTRIES_HAVE_SAME_PRIORITY_TO_BE_EVICTED</li>
     * </ul>
     *
     * @param e1 the first {@link EvictableEntryView} instance to be compared
     * @param e2 the second {@link EvictableEntryView} instance to be compared
     *
     * @return the result of comparison
     */
    @Override
    public abstract int compare(E e1, E e2);

}
