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

package com.hazelcast.topic;

import com.hazelcast.instance.LocalInstanceStats;
import com.hazelcast.internal.monitor.MemberState;

/**
 * Local topic statistics to be used by {@link MemberState} implementations.
 */
public interface LocalTopicStats extends LocalInstanceStats {

    /**
     * Returns the creation time of this topic on this member
     *
     * @return creation time of this topic on this member
     */
    long getCreationTime();

    /**
     * Returns the total number of published messages of this topic on this member
     *
     * @return total number of published messages of this topic on this member
     */
    long getPublishOperationCount();

    /**
     * Returns the total number of received messages of this topic on this member
     *
     * @return total number of received messages of this topic on this member
     */
    long getReceiveOperationCount();
}
