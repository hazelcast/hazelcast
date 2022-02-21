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

package com.hazelcast.internal.util.scheduler;

import java.util.Collection;

/**
 *
 * @param <K> key type of related entries
 * @param <V> value type of related entries
 */
public interface ScheduledEntryProcessor<K, V> {

    /**
     * Processes all entries. Implementation has to
     * handle the failures and can possibly reschedule for a future time.
     * Imagine you are implementing this for dirty records: if mapStore.storeAll
     * throws an exception, you might want to reschedule the failed records.
     *
     * @param scheduler the EntryTaskScheduler
     * @param entries the entries (key and value) to process
     */
    void process(EntryTaskScheduler<K, V> scheduler, Collection<ScheduledEntry<K, V>> entries);
}
