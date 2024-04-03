/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.raft.impl;

import com.hazelcast.internal.serialization.impl.FactoryIdHelper;

public abstract class RaftDataSerializerConstants {

    public static final int RAFT_DS_FACTORY_ID = -1001;
    public static final String RAFT_DS_FACTORY = "hazelcast.serialization.ds.raft";

    public static final int F_ID = FactoryIdHelper.getFactoryId(RAFT_DS_FACTORY, RAFT_DS_FACTORY_ID);
    public static final int PRE_VOTE_REQUEST = 1;
    public static final int PRE_VOTE_RESPONSE = 2;
    public static final int VOTE_REQUEST = 3;
    public static final int VOTE_RESPONSE = 4;
    public static final int APPEND_REQUEST = 5;
    public static final int APPEND_SUCCESS_RESPONSE = 6;
    public static final int APPEND_FAILURE_RESPONSE = 7;
    public static final int LOG_ENTRY = 8;
    public static final int SNAPSHOT_ENTRY = 9;
    public static final int INSTALL_SNAPSHOT = 10;
    public static final int DESTROY_RAFT_GROUP_COMMAND = 11;
    public static final int UPDATE_RAFT_GROUP_MEMBERS_COMMAND = 12;
    public static final int TRIGGER_LEADER_ELECTION = 13;
}
