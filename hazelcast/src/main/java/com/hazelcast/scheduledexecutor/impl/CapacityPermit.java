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

package com.hazelcast.scheduledexecutor.impl;

import java.util.concurrent.RejectedExecutionException;

/**
 * A capacity controller that keeps track of task counts upon scheduling/disposing and Rejects tasks accordingly
 */
public interface CapacityPermit {

    /**
     * Acquires a permit to schedule a new task.
     * When max allowance is reached, future attempts will trigger a {@link RejectedExecutionException} exception
     *
     * @throws RejectedExecutionException
     */
    void acquire() throws RejectedExecutionException;

    /**
     * Acquires a permit to schedule a new task.
     * When max allowance is reached, future attempts will silently keep acquiring permits without any exceptions
     */
    void acquireQuietly();

    /**
     * Releases a permit back to pool for future use
     */
    void release();

    /**
     * @return the total number of acquired permits
     */
    int totalAcquired();
}
