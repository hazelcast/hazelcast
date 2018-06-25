/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.raft;

import java.util.Collection;

/**
 * Contains information about a Raft group
 */
public interface RaftGroup {

    /**
     * Represents status of a Raft group
     */
    enum RaftGroupStatus {
        /**
         * A Raft group is active after it is initialized with the first request it has received,
         * and before its destroy process is initialized.
         */
        ACTIVE,

        /**
         * A Raft group moves into this state after its destroy process is initialized but not completed yet.
         */
        DESTROYING,

        /**
         * A Raft group moves into this state after its destroy process is completed.
         */
        DESTROYED
    }

    /**
     * Returns unique id of the Raft group
     */
    RaftGroupId id();

    /**
     * Returns status of the Raft group
     */
    RaftGroupStatus status();

    /**
     * Returns members that the Raft group is initialized with.
     */
    Collection<RaftMember> initialMembers();

    /**
     * Returns current members of the Raft group
     */
    Collection<RaftMember> members();
}
