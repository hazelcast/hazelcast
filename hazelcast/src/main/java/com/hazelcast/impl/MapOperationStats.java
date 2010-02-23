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

package com.hazelcast.impl;

import java.io.Serializable;

/**
 * Map Operation Statistics returns number of map operations in bounded period. The period
 * has start and end times. Given the number of operations in that period, one can calculate the number of
 * operations per second. Start and End times are given in milliseconds.
 */
public interface MapOperationStats extends Serializable {

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
     * Number of put operations
     *
     * @return
     */
    public long getNumberOfPuts();

    /**
     * Number of get operations
     *
     * @return
     */
    public long getNumberOfGets();

    /**
     * Number of Remove operations
     *
     * @return
     */
    public long getNumberOfRemoves();

    /**
     * Returns number of total operations
     *
     * @return
     */
    public long total();
}
