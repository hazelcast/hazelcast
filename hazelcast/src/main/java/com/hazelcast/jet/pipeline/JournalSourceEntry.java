/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.pipeline;

import java.util.Map;

/**
 * Entry emitted by journal sources such as
 * {@link Sources#mapJournalEntries(String, JournalInitialPosition)},
 * {@link  Sources#cacheJournalEntries(String, JournalInitialPosition)}, and others.
 * <p>
 * This interface extends {@link java.util.Map.Entry} and provides additional
 * metadata about the journaled entry. In particular, it exposes whether any
 * events were lost before this entry due to an event journal overflow.
 *
 * @param <K> the key type
 * @param <V> the value type
 * @since 5.7
 */
public interface JournalSourceEntry<K, V> extends Map.Entry<K, V> {

    /**
     * Indicates whether some events preceding this event were lost.
     * @return true if events were lost, false otherwise
     */
    boolean isAfterLostEvents();
}
