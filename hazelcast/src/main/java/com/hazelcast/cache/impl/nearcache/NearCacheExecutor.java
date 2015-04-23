/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.impl.nearcache;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Contract point for executing near-cache specific tasks.
 *
 * @see java.lang.Runnable
 * @see java.util.concurrent.ScheduledFuture
 */
public interface NearCacheExecutor {

    /**
     * Creates and executes a periodic action that becomes enabled first after the given initial delay.
     *
     * @param command       the task to execute.
     * @param initialDelay  the time to delay the first execution of the task.
     * @param delay         the delay between the termination of one task and the commencement of the next task.
     * @param unit          the time unit of the <code>initialDelay</code> and <code>delay</code> parameters.
     *
     * @return the {@link ScheduledFuture} instance representing the pending completion of the task.
     */
    ScheduledFuture<?> scheduleWithFixedDelay(Runnable command,
                                              long initialDelay,
                                              long delay,
                                              TimeUnit unit);

}
