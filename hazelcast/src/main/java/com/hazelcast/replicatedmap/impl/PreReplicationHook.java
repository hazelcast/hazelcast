/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.replicatedmap.impl;

import com.hazelcast.replicatedmap.impl.messages.MultiReplicationMessage;
import com.hazelcast.replicatedmap.impl.messages.ReplicationMessage;

/**
 * This is an interceptor interface to hook into the current replication process. This is not meant to be public API
 * and should not be used in any external application. It might also be prospect to later changes.
 */
public interface PreReplicationHook {

    void preReplicateMessage(ReplicationMessage message, ReplicationChannel channel);

    void preReplicateMultiMessage(MultiReplicationMessage message, ReplicationChannel channel);

}
