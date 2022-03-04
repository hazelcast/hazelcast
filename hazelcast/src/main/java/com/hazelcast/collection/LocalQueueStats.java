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

package com.hazelcast.collection;

import com.hazelcast.instance.LocalInstanceStats;
import com.hazelcast.internal.monitor.MemberState;

/**
 * Local queue statistics to be used by {@link MemberState} implementations.
 */
public interface LocalQueueStats extends LocalInstanceStats {

    /**
     * Returns the number of owned items in this member.
     *
     * @return number of owned items in this member.
     */
    long getOwnedItemCount();

    /**
     * Returns the number of backup items in this member.
     *
     * @return number of backup items in this member.
     */
    long getBackupItemCount();

    /**
     * Returns the minimum age of the items in this member.
     *
     * @return minimum age of the items in this member
     */
    long getMinAge();

    /**
     * Returns the maximum age of the items in this member.
     *
     * @return maximum age of the items in this member
     */
    long getMaxAge();

    /**
     * Returns the average age of the items in this member.
     *
     * @return average age of the items in this member
     */
    long getAverageAge();

    /**
     * Returns the number of offer/put/add operations.
     * Offers returning false will be included.
     * #getRejectedOfferOperationCount can be used
     * to get the rejected offers.
     *
     * @return number of offer/put/add operations
     */
    long getOfferOperationCount();

    /**
     * Returns the number of rejected offers. Offer
     * can be rejected because of max-size limit
     * on the queue.
     *
     * @return number of rejected offers.
     */
    long getRejectedOfferOperationCount();

    /**
     * Returns the number of poll/take/remove operations.
     * Polls returning null (empty) will be included.
     * #getEmptyPollOperationCount can be used to get the
     * number of polls returned null.
     *
     * @return number of poll/take/remove operations.
     */
    long getPollOperationCount();

    /**
     * Returns number of null returning poll operations.
     * Poll operation might return null, if the queue is empty.
     *
     * @return number of null returning poll operations.
     */
    long getEmptyPollOperationCount();

    /**
     * Returns number of other operations
     *
     * @return number of other operations
     */
    long getOtherOperationsCount();

    /**
     * Returns number of event operations
     *
     * @return number of event operations
     */
    long getEventOperationCount();
}
