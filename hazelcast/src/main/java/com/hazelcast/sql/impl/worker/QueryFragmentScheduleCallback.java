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

package com.hazelcast.sql.impl.worker;

import com.hazelcast.sql.impl.exec.root.BlockingRootResultConsumer;

public interface QueryFragmentScheduleCallback {
    /**
     * Schedule the fragment for execution without the {@code force} flag.
     *
     * @see #schedule(boolean)
     * @return {@code true} if the fragment was scheduled, {@code false} if already scheduled.
     */
    default boolean schedule() {
        return schedule(false);
    }

    /**
     * Schedule the fragment for execution.
     *
     * @param force {@code true} to ensure that the fragment is re-executed after the call to the "schedule" even if there
     *     are no new exchange operations. This is required by the result consumer.
     * @see BlockingRootResultConsumer
     * @return {@code true} if the fragment was scheduled, {@code false} if already scheduled.
     */
    boolean schedule(boolean force);
}
