/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.retry;

import java.io.Serializable;

/**
 * Description of a strategy to be followed when retrying a failed action, like
 * connecting to a server. A typical strategy for this scenario could be for
 * example: "attempt to reconnect 3 times, wait 5 seconds between each attempt,
 * if all of them fail, then give up".
 * <p>
 * The lifecycle of a retry strategy begins whenever its covered action fails
 * and ends in one of two ways: either a retry of the action is successful (the
 * action is successfully undertaken, for example reconnect to a server
 * succeeds) or the strategy gives up (for example it had a specified maximum
 * number of retries, all of them had been attempted and none was successful).
 *
 * @since 4.3
 */
public interface RetryStrategy extends Serializable {

    /**
     * Maximum number of retry attempt that should be made before giving up.
     * <p>
     * A value of 0 should be interpreted as "give up on first failure, without
     * any retries".
     * <p>
     * A negative value should be interpreted as "retry indefinitely, until
     * success".
     */
    int getMaxAttempts();

    /**
     * Function specifying how much time to wait before each retry attempt. Can
     * specify a constant wait time, exponential backoff or any other,
     * arbitrarily complex wait strategy.
     */
    IntervalFunction getIntervalFunction();

}
