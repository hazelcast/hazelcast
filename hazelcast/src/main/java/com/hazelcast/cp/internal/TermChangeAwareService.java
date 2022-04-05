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

package com.hazelcast.cp.internal;

import com.hazelcast.cp.CPGroupId;

/**
 * Services can implement this interface to get notified when there is
 * a leader change in a Raft group. After a new leader is elected,
 * a special commit is made first to notify services via this interface.
 */
public interface TermChangeAwareService {

    /**
     * Invokes as part of the first commit after a new leader is elected.
     *
     * @param groupId id of the Raft group in which a new leader is elected
     * @param commitIndex index of the commit in the Raft log
     */
    void onNewTermCommit(CPGroupId groupId, long commitIndex);

}
