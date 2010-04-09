/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.monitor;

/**
 * Local Map Operation Statistics returns number of map operations in bounded period. The period
 * has start and end times. Given the number of operations in that period, one can calculate the number of
 * operations per second. 
 */
public interface LocalMapOperationStats {

    /**
     * Gets the start time of the period in milliseconds.
     *
     * @return start time in milliseconds.
     */
    public long getPeriodStart();

    /**
     * Gets the end time of the period in milliseconds.
     *
     * @return end time in milliseconds.
     */
    public long getPeriodEnd();

    /**
     * Returns the number of put operations
     *
     * @return number of put operations
     */
    public long getNumberOfPuts();

    /**
     * Returns the number of get operations
     *
     * @return number of get operations
     */
    public long getNumberOfGets();

    /**
     * Returns the number of Remove operations
     *
     * @return number of remove operations
     */
    public long getNumberOfRemoves();

    /**
     * Returns the number of Events Received
     *
     * @return number of events received
     */
    public long getNumberOfEvents();

    /**
     * Returns the total number of Other Operations
     *
     * @return number of other operations
     */
    public long getNumberOfOtherOperations();

    /**
     * Returns the total number of total operations
     *
     * @return number of total operations
     */
    public long total();
}
