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

package com.hazelcast.util.scheduler;

import com.hazelcast.spi.TaskScheduler;

/**
 * Factory for EntryTaskSchedulers.
 */
public final class EntryTaskSchedulerFactory {

    private EntryTaskSchedulerFactory() {
    }

    /**
     * Creates a new EntryTaskScheduler that will run all second operations in bulk.
     * Imagine a write-behind map where dirty entries will be stored in bulk.
     * Note that each key can only occur once; meaning you cannot delay the execution.
     * For example, once an entry is marked as dirty, it will run in write-delay-seconds,
     * even if the entry is updated again within write-delay-seconds.
     * Two things to remember:
     * 1. A key cannot be re-scheduled (postponing its execution).
     * 2. All entries scheduled for a given second will be executed once by your
     * SecondBulkExecutor implementation.
     * Once a key is executed, it can be re-scheduled for another execution.
     * <p/>
     * EntryTaskScheduler implementation is thread-safe.
     *
     * @param taskScheduler ScheduledExecutorService instance to execute the second
     * @param entryProcessor           bulk processor
     * @return EntryTaskScheduler that will run all second operations in bulk
     */
    public static <K, V> EntryTaskScheduler<K, V> newScheduler(TaskScheduler taskScheduler,
                                                               ScheduledEntryProcessor<K, V> entryProcessor,
                                                               ScheduleType scheduleType) {
        return new SecondsBasedEntryTaskScheduler<K, V>(taskScheduler, entryProcessor, scheduleType);
    }

}
