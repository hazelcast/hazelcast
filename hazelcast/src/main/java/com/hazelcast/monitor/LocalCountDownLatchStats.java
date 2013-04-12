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

package com.hazelcast.monitor;

public interface LocalCountDownLatchStats extends LocalInstanceStats {

    /**
     //     * Returns the number of countDown operations in this period.
     //     *
     //     * @return number of acquire operations
     //     */
//    public long getNumberOfCountDowns();
//
//    /**
//     * Returns the number of await operations in this period.
//     *
//     * @return number of await operations
//     */
//    public long getNumberOfAwaits();
//
//    /**
//     * Returns the number of operations other than await or countDown
//     * in this period.
//     *
//     * @return number of await operations
//     */
//    public long getNumberOfOthers();
//
//    /**
//     * Returns the total latency of await operations in this period.
//     * <p>To get the average latency, divide by the number of awaits.
//     * </p>
//     *
//     * @return total latency of all await operations
//     */
//    public long getTotalAwaitLatency();
//
//    /**
//     * Returns the total latency of countdown operations in this period.
//     * <p>To get the average latency, divide by the number of countdowns.
//     * </p>
//     *
//     * @return total latency of all countdown operations
//     */
//    public long getTotalCountDownLatency();
//
//    /**
//     * Returns the total latency of operations other than await or
//     * countdown in this period.
//     * <p>To get the average latency, divide by the number of others.
//     * </p>
//     *
//     * @return total latency of all await operations
//     */
//    public long getTotalOtherLatency();
//
//    /**
//     * Returns the number of times the count reached zero from a countdown
//     * operation in this period.
//     *
//     * @return number of attach operations
//     */
//    public long getNumberOfGatesOpened();
//
//    /**
//     * Returns the number of awaits released in this period from gate
//     * openings.
//     *
//     * @return number of awaits released
//     */
//    public long getNumberOfAwaitsReleased();
}
