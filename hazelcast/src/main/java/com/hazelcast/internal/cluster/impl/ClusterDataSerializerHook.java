/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.instance.MemberImpl;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.cluster.impl.operations.AuthenticationFailureOp;
import com.hazelcast.internal.cluster.impl.operations.AuthorizationOp;
import com.hazelcast.internal.cluster.impl.operations.BeforeJoinCheckFailureOp;
import com.hazelcast.internal.cluster.impl.operations.CommitClusterStateOp;
import com.hazelcast.internal.cluster.impl.operations.ConfigMismatchOp;
import com.hazelcast.internal.cluster.impl.operations.ExplicitSuspicionOp;
import com.hazelcast.internal.cluster.impl.operations.FetchMembersViewOp;
import com.hazelcast.internal.cluster.impl.operations.FinalizeJoinOp;
import com.hazelcast.internal.cluster.impl.operations.GroupMismatchOp;
import com.hazelcast.internal.cluster.impl.operations.HeartbeatComplaintOp;
import com.hazelcast.internal.cluster.impl.operations.HeartbeatOp;
import com.hazelcast.internal.cluster.impl.operations.JoinRequestOp;
import com.hazelcast.internal.cluster.impl.operations.LockClusterStateOp;
import com.hazelcast.internal.cluster.impl.operations.JoinMastershipClaimOp;
import com.hazelcast.internal.cluster.impl.operations.MasterConfirmationOp;
import com.hazelcast.internal.cluster.impl.operations.WhoisMasterOp;
import com.hazelcast.internal.cluster.impl.operations.MemberAttributeChangedOp;
import com.hazelcast.internal.cluster.impl.operations.MembersUpdateOp;
import com.hazelcast.internal.cluster.impl.operations.MemberRemoveOperation;
import com.hazelcast.internal.cluster.impl.operations.MergeClustersOp;
import com.hazelcast.internal.cluster.impl.operations.PostJoinOp;
import com.hazelcast.internal.cluster.impl.operations.RollbackClusterStateOp;
import com.hazelcast.internal.cluster.impl.operations.MasterResponseOp;
import com.hazelcast.internal.cluster.impl.operations.ShutdownNodeOp;
import com.hazelcast.internal.cluster.impl.operations.SplitBrainMergeValidationOp;
import com.hazelcast.internal.cluster.impl.operations.TriggerExplicitSuspicionOp;
import com.hazelcast.internal.cluster.impl.operations.TriggerMemberListPublishOp;
import com.hazelcast.internal.cluster.impl.operations.PromoteLiteMemberOp;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.ArrayDataSerializableFactory;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.version.MemberVersion;
import com.hazelcast.version.Version;

public final class ClusterDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = 0;

    // never reassign numbers, in case of deletion leave a number gap.
    public static final int AUTH_FAILURE = 0;
    public static final int ADDRESS = 1;
    public static final int MEMBER = 2;
    public static final int HEARTBEAT = 3;
    public static final int CONFIG_CHECK = 4;
    public static final int BIND_MESSAGE = 5;
    public static final int MEMBER_INFO_UPDATE = 6;
    public static final int FINALIZE_JOIN = 7;
    public static final int AUTHORIZATION = 8;
    public static final int BEFORE_JOIN_CHECK_FAILURE = 9;
    public static final int CHANGE_CLUSTER_STATE = 10;
    public static final int CONFIG_MISMATCH = 11;
    public static final int GROUP_MISMATCH = 12;
    public static final int SPLIT_BRAIN_MERGE_VALIDATION = 13;
    public static final int JOIN_REQUEST_OP = 14;
    public static final int LOCK_CLUSTER_STATE = 15;
    public static final int MASTER_CLAIM = 16;
    public static final int MASTER_CONFIRM = 17;
    public static final int WHOIS_MASTER = 18;
    public static final int MEMBER_ATTR_CHANGED = 19;
    public static final int MEMBER_REMOVE = 20;
    public static final int MERGE_CLUSTERS = 21;
    public static final int POST_JOIN = 22;
    public static final int ROLLBACK_CLUSTER_STATE = 23;
    public static final int MASTER_RESPONSE = 24;
    public static final int SHUTDOWN_NODE = 25;
    public static final int TRIGGER_MEMBER_LIST_PUBLISH = 26;
    public static final int CLUSTER_STATE_TRANSACTION_LOG_RECORD = 27;
    public static final int MEMBER_INFO = 28;
    public static final int JOIN_MESSAGE = 29;
    public static final int JOIN_REQUEST = 30;
    public static final int MIGRATION_INFO = 31;
    public static final int MEMBER_VERSION = 32;
    public static final int CLUSTER_STATE_CHANGE = 33;
    public static final int SPLIT_BRAIN_JOIN_MESSAGE = 34;
    public static final int VERSION = 35;
    public static final int FETCH_MEMBER_LIST_STATE = 36;
    public static final int EXPLICIT_SUSPICION = 37;
    public static final int MEMBERS_VIEW = 38;
    public static final int TRIGGER_EXPLICIT_SUSPICION = 39;
    public static final int MEMBERS_VIEW_METADATA = 40;
    public static final int HEARTBEAT_COMPLAINT = 41;
    public static final int PROMOTE_LITE_MEMBER = 42;

    static final int LEN = PROMOTE_LITE_MEMBER + 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructors = new ConstructorFunction[LEN];

        constructors[AUTH_FAILURE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new AuthenticationFailureOp();
            }
        };
        constructors[ADDRESS] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new Address();
            }
        };
        constructors[MEMBER] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MemberImpl();
            }
        };
        constructors[HEARTBEAT] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HeartbeatOp();
            }
        };
        constructors[CONFIG_CHECK] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ConfigCheck();
            }
        };
        constructors[BIND_MESSAGE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new BindMessage();
            }
        };
        constructors[MEMBER_INFO_UPDATE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MembersUpdateOp();
            }
        };
        constructors[FINALIZE_JOIN] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new FinalizeJoinOp();
            }
        };
        constructors[AUTHORIZATION] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new AuthorizationOp();
            }
        };
        constructors[BEFORE_JOIN_CHECK_FAILURE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new BeforeJoinCheckFailureOp();
            }
        };
        constructors[CHANGE_CLUSTER_STATE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CommitClusterStateOp();
            }
        };
        constructors[CONFIG_MISMATCH] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ConfigMismatchOp();
            }
        };
        constructors[GROUP_MISMATCH] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new GroupMismatchOp();
            }
        };
        constructors[SPLIT_BRAIN_MERGE_VALIDATION] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new SplitBrainMergeValidationOp();
            }
        };
        constructors[JOIN_REQUEST_OP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new JoinRequestOp();
            }
        };
        constructors[LOCK_CLUSTER_STATE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new LockClusterStateOp();
            }
        };
        constructors[MASTER_CLAIM] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new JoinMastershipClaimOp();
            }
        };
        constructors[MASTER_CONFIRM] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MasterConfirmationOp();
            }
        };
        constructors[WHOIS_MASTER] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new WhoisMasterOp();
            }
        };
        constructors[MEMBER_ATTR_CHANGED] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MemberAttributeChangedOp();
            }
        };
        constructors[MEMBER_REMOVE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MemberRemoveOperation();
            }
        };
        constructors[MERGE_CLUSTERS] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MergeClustersOp();
            }
        };
        constructors[POST_JOIN] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new PostJoinOp();
            }
        };
        constructors[ROLLBACK_CLUSTER_STATE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new RollbackClusterStateOp();
            }
        };
        constructors[MASTER_RESPONSE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MasterResponseOp();
            }
        };
        constructors[SHUTDOWN_NODE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ShutdownNodeOp();
            }
        };
        constructors[TRIGGER_MEMBER_LIST_PUBLISH] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new TriggerMemberListPublishOp();
            }
        };
        constructors[CLUSTER_STATE_TRANSACTION_LOG_RECORD] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ClusterStateTransactionLogRecord();
            }
        };
        constructors[MEMBER_INFO] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MemberInfo();
            }
        };
        constructors[JOIN_MESSAGE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new JoinMessage();
            }
        };
        constructors[JOIN_REQUEST] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new JoinRequest();
            }
        };
        constructors[MIGRATION_INFO] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MigrationInfo();
            }
        };
        constructors[MEMBER_VERSION] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MemberVersion();
            }
        };
        constructors[CLUSTER_STATE_CHANGE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ClusterStateChange();
            }
        };
        constructors[SPLIT_BRAIN_JOIN_MESSAGE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new SplitBrainJoinMessage();
            }
        };
        constructors[VERSION] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new Version();
            }
        };

        constructors[FETCH_MEMBER_LIST_STATE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new FetchMembersViewOp();
            }
        };
        constructors[EXPLICIT_SUSPICION] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ExplicitSuspicionOp();
            }
        };
        constructors[MEMBERS_VIEW] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MembersView();
            }
        };
        constructors[TRIGGER_EXPLICIT_SUSPICION] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new TriggerExplicitSuspicionOp();
            }
        };
        constructors[MEMBERS_VIEW_METADATA] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MembersViewMetadata();
            }
        };
        constructors[HEARTBEAT_COMPLAINT] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HeartbeatComplaintOp();
            }
        };
        constructors[PROMOTE_LITE_MEMBER] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new PromoteLiteMemberOp();
            }
        };
        return new ArrayDataSerializableFactory(constructors);
    }
}
