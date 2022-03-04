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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.cluster.impl.VectorClock;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.cluster.impl.operations.AuthenticationFailureOp;
import com.hazelcast.internal.cluster.impl.operations.BeforeJoinCheckFailureOp;
import com.hazelcast.internal.cluster.impl.operations.CommitClusterStateOp;
import com.hazelcast.internal.cluster.impl.operations.ConfigMismatchOp;
import com.hazelcast.internal.cluster.impl.operations.ExplicitSuspicionOp;
import com.hazelcast.internal.cluster.impl.operations.FetchMembersViewOp;
import com.hazelcast.internal.cluster.impl.operations.FinalizeJoinOp;
import com.hazelcast.internal.cluster.impl.operations.ClusterMismatchOp;
import com.hazelcast.internal.cluster.impl.operations.HeartbeatComplaintOp;
import com.hazelcast.internal.cluster.impl.operations.HeartbeatOp;
import com.hazelcast.internal.cluster.impl.operations.JoinMastershipClaimOp;
import com.hazelcast.internal.cluster.impl.operations.JoinRequestOp;
import com.hazelcast.internal.cluster.impl.operations.LockClusterStateOp;
import com.hazelcast.internal.cluster.impl.operations.MasterResponseOp;
import com.hazelcast.internal.cluster.impl.operations.MembersUpdateOp;
import com.hazelcast.internal.cluster.impl.operations.MergeClustersOp;
import com.hazelcast.internal.cluster.impl.operations.OnJoinOp;
import com.hazelcast.internal.cluster.impl.operations.PromoteLiteMemberOp;
import com.hazelcast.internal.cluster.impl.operations.RollbackClusterStateOp;
import com.hazelcast.internal.cluster.impl.operations.ShutdownNodeOp;
import com.hazelcast.internal.cluster.impl.operations.SplitBrainMergeValidationOp;
import com.hazelcast.internal.cluster.impl.operations.TriggerExplicitSuspicionOp;
import com.hazelcast.internal.cluster.impl.operations.TriggerMemberListPublishOp;
import com.hazelcast.internal.cluster.impl.operations.WhoisMasterOp;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.ArrayDataSerializableFactory;
import com.hazelcast.cluster.Address;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.internal.util.ConstructorFunction;
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
    public static final int MEMBER_HANDSHAKE = 5;
    public static final int MEMBER_INFO_UPDATE = 6;
    public static final int FINALIZE_JOIN = 7;
    public static final int BEFORE_JOIN_CHECK_FAILURE = 8;
    public static final int CHANGE_CLUSTER_STATE = 9;
    public static final int CONFIG_MISMATCH = 10;
    public static final int CLUSTER_MISMATCH = 11;
    public static final int SPLIT_BRAIN_MERGE_VALIDATION = 12;
    public static final int JOIN_REQUEST_OP = 13;
    public static final int LOCK_CLUSTER_STATE = 14;
    public static final int MASTER_CLAIM = 15;
    public static final int WHOIS_MASTER = 16;
    public static final int ENDPOINT_QUALIFIER = 17;
    public static final int MERGE_CLUSTERS = 18;
    public static final int POST_JOIN = 19;
    public static final int ROLLBACK_CLUSTER_STATE = 20;
    public static final int MASTER_RESPONSE = 21;
    public static final int SHUTDOWN_NODE = 22;
    public static final int TRIGGER_MEMBER_LIST_PUBLISH = 23;
    public static final int CLUSTER_STATE_TRANSACTION_LOG_RECORD = 24;
    public static final int MEMBER_INFO = 25;
    public static final int JOIN_MESSAGE = 26;
    public static final int JOIN_REQUEST = 27;
    public static final int MIGRATION_INFO = 28;
    public static final int MEMBER_VERSION = 29;
    public static final int CLUSTER_STATE_CHANGE = 30;
    public static final int SPLIT_BRAIN_JOIN_MESSAGE = 31;
    public static final int VERSION = 32;
    public static final int FETCH_MEMBER_LIST_STATE = 33;
    public static final int EXPLICIT_SUSPICION = 34;
    public static final int MEMBERS_VIEW = 35;
    public static final int TRIGGER_EXPLICIT_SUSPICION = 36;
    public static final int MEMBERS_VIEW_METADATA = 37;
    public static final int HEARTBEAT_COMPLAINT = 38;
    public static final int PROMOTE_LITE_MEMBER = 39;
    public static final int VECTOR_CLOCK = 40;

    static final int LEN = VECTOR_CLOCK + 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructors = new ConstructorFunction[LEN];

        constructors[AUTH_FAILURE] = arg -> new AuthenticationFailureOp();
        constructors[ADDRESS] = arg -> new Address();
        constructors[MEMBER] = arg -> new MemberImpl();
        constructors[HEARTBEAT] = arg -> new HeartbeatOp();
        constructors[CONFIG_CHECK] = arg -> new ConfigCheck();
        constructors[MEMBER_HANDSHAKE] = arg -> new MemberHandshake();
        constructors[MEMBER_INFO_UPDATE] = arg -> new MembersUpdateOp();
        constructors[FINALIZE_JOIN] = arg -> new FinalizeJoinOp();
        constructors[BEFORE_JOIN_CHECK_FAILURE] = arg -> new BeforeJoinCheckFailureOp();
        constructors[CHANGE_CLUSTER_STATE] = arg -> new CommitClusterStateOp();
        constructors[CONFIG_MISMATCH] = arg -> new ConfigMismatchOp();
        constructors[CLUSTER_MISMATCH] = arg -> new ClusterMismatchOp();
        constructors[SPLIT_BRAIN_MERGE_VALIDATION] = arg -> new SplitBrainMergeValidationOp();
        constructors[JOIN_REQUEST_OP] = arg -> new JoinRequestOp();
        constructors[LOCK_CLUSTER_STATE] = arg -> new LockClusterStateOp();
        constructors[MASTER_CLAIM] = arg -> new JoinMastershipClaimOp();
        constructors[WHOIS_MASTER] = arg -> new WhoisMasterOp();
        constructors[MERGE_CLUSTERS] = arg -> new MergeClustersOp();
        constructors[POST_JOIN] = arg -> new OnJoinOp();
        constructors[ROLLBACK_CLUSTER_STATE] = arg -> new RollbackClusterStateOp();
        constructors[MASTER_RESPONSE] = arg -> new MasterResponseOp();
        constructors[SHUTDOWN_NODE] = arg -> new ShutdownNodeOp();
        constructors[TRIGGER_MEMBER_LIST_PUBLISH] = arg -> new TriggerMemberListPublishOp();
        constructors[CLUSTER_STATE_TRANSACTION_LOG_RECORD] = arg -> new ClusterStateTransactionLogRecord();
        constructors[MEMBER_INFO] = arg -> new MemberInfo();
        constructors[JOIN_MESSAGE] = arg -> new JoinMessage();
        constructors[JOIN_REQUEST] = arg -> new JoinRequest();
        constructors[MIGRATION_INFO] = arg -> new MigrationInfo();
        constructors[MEMBER_VERSION] = arg -> new MemberVersion();
        constructors[CLUSTER_STATE_CHANGE] = arg -> new ClusterStateChange();
        constructors[SPLIT_BRAIN_JOIN_MESSAGE] = arg -> new SplitBrainJoinMessage();
        constructors[VERSION] = arg -> new Version();
        constructors[FETCH_MEMBER_LIST_STATE] = arg -> new FetchMembersViewOp();
        constructors[EXPLICIT_SUSPICION] = arg -> new ExplicitSuspicionOp();
        constructors[MEMBERS_VIEW] = arg -> new MembersView();
        constructors[TRIGGER_EXPLICIT_SUSPICION] = arg -> new TriggerExplicitSuspicionOp();
        constructors[MEMBERS_VIEW_METADATA] = arg -> new MembersViewMetadata();
        constructors[HEARTBEAT_COMPLAINT] = arg -> new HeartbeatComplaintOp();
        constructors[PROMOTE_LITE_MEMBER] = arg -> new PromoteLiteMemberOp();
        constructors[VECTOR_CLOCK] = arg -> new VectorClock();
        constructors[ENDPOINT_QUALIFIER] = arg -> new EndpointQualifier();
        return new ArrayDataSerializableFactory(constructors);
    }
}
