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

/**
 * Controls behaviour of {@link SecondsBasedEntryTaskScheduler} when a new entry is added
 * under already existing key.
 */
public enum ScheduleType {

    /**
     * If there is an entry already scheduled under a given key then
     * the existing entry will be removed and a new one will be scheduled instead.
     */
    POSTPONE,

    /**
     * Always add a new entry even when there is one already scheduled under a given key. The existing entry
     * won't be affected.
     */
    FOR_EACH
}
