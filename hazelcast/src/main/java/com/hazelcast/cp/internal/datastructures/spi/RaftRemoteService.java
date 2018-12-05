/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.spi;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.spi.RemoteService;

/**
 * Defines methods to create and destroy Raft data structure instances
 * and their proxies
 */
public interface RaftRemoteService extends RemoteService {

    /**
     * Destroys the given Raft data structure on the Raft group.
     */
    boolean destroyRaftObject(CPGroupId groupId, String objectName);
}
