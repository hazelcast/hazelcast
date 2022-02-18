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
 * Contains methods that are invoked on life cycle changes of a Raft node
 */
public interface RaftNodeLifecycleAwareService {

    /**
     * Called on the thread of the Raft group when the given Raft node is
     * terminated, either gracefully via Raft group destroy or forcefully.
     */
    void onRaftNodeTerminated(CPGroupId groupId);

    /**
     * Called on the thread of the Raft group when the given Raft node is
     * stepped down, either because it is shutting down, or it could not be
     * added to the Raft group
     */
    void onRaftNodeSteppedDown(CPGroupId groupId);
}
