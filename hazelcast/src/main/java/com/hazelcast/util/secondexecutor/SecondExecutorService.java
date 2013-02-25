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

package com.hazelcast.util.secondexecutor;

/**
 * Schedule execution of an entry for seconds later.
 * This is kind of like a scheduled executor service but instead of scheduling
 * a execution for a specific millisecond, this service will
 * schedule it with second proximity. If delayMillis is 600 ms for example,
 * then the entry will be scheduled to execute in 1 second. If delayMillis is 2400
 * this the entry will be scheduled to execute in 3 seconds. So delayMillis is
 * ceil-ed to the next second. It gives up from exact time scheduling to gain
 * the power of
 * a) bulk execution of all operations within the same second
 * or
 * b) being able to reschedule (postpone) execution
 */
public interface SecondExecutorService {
    /**
     * Schedules (or reschedules) the execution of given entry. key parameter is
     * used to check whether there is an existing scheduling for this entry.
     *
     * @param delayMillis milliseconds to delay the execution.
     *                    It is ceil to the next second. 2300 ms means 3 seconds.
     * @param key         key of this scheduling.
     * @param object      user object to pass back when it is time to execute.
     * @return returns true if this call resulted in a new scheduling,
     *         false otherwise.
     */
    boolean schedule(long delayMillis, Object key, Object object);
}
