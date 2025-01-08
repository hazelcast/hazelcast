/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.serialization.impl.FactoryIdHelper;

public abstract class RaftServiceSerializerConstants {

    public static final int RAFT_DS_FACTORY_ID = -1002;
    public static final String RAFT_DS_FACTORY = "hazelcast.serialization.ds.raft.service";

    public static final int F_ID = FactoryIdHelper.getFactoryId(RAFT_DS_FACTORY, RAFT_DS_FACTORY_ID);
    public static final int GROUP_ID = 1;
    public static final int RAFT_GROUP_INFO = 2;
    public static final int PRE_VOTE_REQUEST_OP = 3;
    public static final int PRE_VOTE_RESPONSE_OP = 4;
    public static final int VOTE_REQUEST_OP = 5;
    public static final int VOTE_RESPONSE_OP = 6;
    public static final int APPEND_REQUEST_OP = 7;
    public static final int APPEND_SUCCESS_RESPONSE_OP = 8;
    public static final int APPEND_FAILURE_RESPONSE_OP = 9;
    public static final int METADATA_RAFT_GROUP_SNAPSHOT = 10;
    public static final int INSTALL_SNAPSHOT_OP = 11;
    public static final int DEFAULT_RAFT_GROUP_REPLICATE_OP = 12;
    public static final int CREATE_RAFT_GROUP_OP = 13;
    public static final int TRIGGER_DESTROY_RAFT_GROUP_OP = 14;
    public static final int COMPLETE_DESTROY_RAFT_GROUPS_OP = 15;
    public static final int REMOVE_CP_MEMBER_OP = 16;
    public static final int COMPLETE_RAFT_GROUP_MEMBERSHIP_CHANGES_OP = 17;
    public static final int MEMBERSHIP_CHANGE_REPLICATE_OP = 18;
    public static final int MEMBERSHIP_CHANGE_SCHEDULE = 19;
    public static final int DEFAULT_RAFT_GROUP_QUERY_OP = 20;
    public static final int TERMINATE_RAFT_NODES_OP = 21;
    public static final int GET_ACTIVE_CP_MEMBERS_OP = 22;
    public static final int GET_DESTROYING_RAFT_GROUP_IDS_OP = 23;
    public static final int GET_MEMBERSHIP_CHANGE_SCHEDULE_OP = 24;
    public static final int GET_RAFT_GROUP_OP = 25;
    public static final int GET_ACTIVE_RAFT_GROUP_BY_NAME_OP = 26;
    public static final int CREATE_RAFT_NODE_OP = 27;
    public static final int DESTROY_RAFT_GROUP_OP = 28;
    public static final int RESTORE_SNAPSHOT_OP = 29;
    public static final int NOTIFY_TERM_CHANGE_OP = 30;
    public static final int CP_MEMBER = 31;
    public static final int PUBLISH_ACTIVE_CP_MEMBERS_OP = 32;
    public static final int ADD_CP_MEMBER_OP = 33;
    public static final int INIT_METADATA_RAFT_GROUP_OP = 34;
    public static final int FORCE_DESTROY_RAFT_GROUP_OP = 35;
    public static final int GET_INITIAL_RAFT_GROUP_MEMBERS_IF_CURRENT_GROUP_MEMBER_OP = 36;
    public static final int GET_RAFT_GROUP_IDS_OP = 37;
    public static final int GET_ACTIVE_RAFT_GROUP_IDS_OP = 38;
    public static final int RAFT_PRE_JOIN_OP = 39;
    public static final int RESET_CP_MEMBER_OP = 40;
    public static final int GROUP_MEMBERSHIP_CHANGE = 41;
    public static final int UNSAFE_RAFT_REPLICATE_OP = 42;
    public static final int UNSAFE_RAFT_QUERY_OP = 43;
    public static final int UNSAFE_RAFT_BACKUP_OP = 44;
    public static final int UNSAFE_SNAPSHOT_REPLICATE_OP = 45;
    public static final int CP_ENDPOINT = 46;
    public static final int CP_GROUP_SUMMARY = 47;
    public static final int GET_LEADED_GROUPS = 48;
    public static final int TRANSFER_LEADERSHIP_OP = 49;
    public static final int TRIGGER_LEADER_ELECTION_OP = 50;
    public static final int UNSAFE_MODE_PARTITION_STATE = 51;
    public static final int UNSAFE_STATE_REPLICATE_OP = 52;
    public static final int GET_CP_OBJECT_INFOS_OP = 53;
    public static final int PUBLISH_CP_GROUP_INFO_OP = 54;
}
