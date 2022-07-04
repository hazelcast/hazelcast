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

package com.hazelcast.jet.retry;

import java.io.Serializable;

/**
 * Description of a strategy to take when an action fails, like when
 * connecting to a server. An example strategy for this scenario can be:
 * "attempt to reconnect 3 times, wait 5 seconds between each attempt, if
 * all of them fail, give up".
 * <p>
 * The lifecycle of a retry strategy begins whenever an action fails and
 * ends in one of two ways: either a retry of the action is successful (for
 * example reconnection attempt to a server succeeds) or the strategy gives
 * up (for example it did a specified maximum number of retries, all of
 * them failed).
 *
 * @since Jet 4.3
 */
public interface RetryStrategy extends Serializable {

    /**
     * Maximum number of retry attempt that should be made before giving up.
     * <p>
     * A value of 0 should be interpreted as "never retry". Action is performed
     * once.
     * <p>
     * A negative value is interpreted as "retry indefinitely, until success".
     */
    int getMaxAttempts();

    /**
     * Function specifying how much time to wait before each retry attempt. Can
     * specify a constant wait time, exponential backoff or any other,
     * arbitrarily complex wait strategy.
     */
    IntervalFunction getIntervalFunction();

}
