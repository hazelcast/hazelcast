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

package com.hazelcast.util.scheduler;

import java.util.Set;

/**
 * Schedules (or reschedules) the execution of given entry.
 *
 * @param <K> key type of related entries
 * @param <V> value type of related entries
 */
public interface EntryTaskScheduler<K, V> {
    /**
     * Schedules (or reschedules) the execution of given entry. Key parameter is
     * used to check whether there is an existing scheduling for this entry.
     *
     * @param delayMillis milliseconds to delay the execution.
     * @param key         key of this scheduling.
     * @param object      user object to pass back when it is time to execute.
     * @return returns true if this call resulted in a new scheduling,
     * false otherwise.
     */
    boolean schedule(long delayMillis, K key, V object);

    ScheduledEntry<K, V> cancel(K key);

    ScheduledEntry<K, V> get(K key);

    Set<K> flush(Set<K> keys);

    void cancelAll();

    int size();
}
