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

package com.hazelcast.internal.monitor;

import com.hazelcast.instance.LocalInstanceStats;

/**
 * Local statistics of a single PN counter to be used by {@link MemberState}
 * implementations.
 */
public interface LocalPNCounterStats extends LocalInstanceStats {

    /**
     * Returns the current value of this PN counter
     */
    long getValue();

    /**
     * Returns the total number of add (including increment) operations on this
     * PN counter on this member.
     */
    long getTotalIncrementOperationCount();

    /**
     * Returns the total number of subtract (including decrement) operations on
     * this PN counter on this member.
     */
    long getTotalDecrementOperationCount();
}
