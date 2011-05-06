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

import com.hazelcast.nio.DataSerializable;

/**
 * Local Queue Operation Statistics returns number of queue operations in bounded period. The period
 * has start and end times. Given the number of operations in that period, one can calculate the number of
 * operations per second.
 */
public interface LocalQueueOperationStats extends DataSerializable {
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
     * Returns the number of offer/put/add operations.
     * Offers returning false will be included.
     * #getNumberOfRejectedOffers can be used
     * to get the rejected offers.
     *
     * @return number offer/put/add operations
     */
    public long getNumberOfOffers();

    /**
     * Returns the number of rejected offers. Offer
     * can be rejected because of max-size limit
     * on the queue.
     *
     * @return number of rejected offers.
     */
    public long getNumberOfRejectedOffers();

    /**
     * Returns the number of poll/take/remove operations.
     * Polls returning null (empty) will be included.
     * #getNumberOfEmptyPolls can be used to get the
     * number of polls returned null.
     *
     * @return number of poll/take/remove operations.
     */
    public long getNumberOfPolls();

    /**
     * Returns number of null returning poll operations.
     * Poll operation might return null, if the queue is empty.
     *
     * @return number of null returning poll operations.
     */
    public long getNumberOfEmptyPolls();
}
