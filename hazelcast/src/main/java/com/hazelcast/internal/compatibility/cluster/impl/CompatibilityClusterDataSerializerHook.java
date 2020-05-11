/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.compatibility.cluster.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.nio.serialization.DataSerializableFactory;

/**
 * Data serializer hook containing (de)serialization information for communicating
 * with 3.x members over WAN.
 */
public final class CompatibilityClusterDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = 0;

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
    // MasterConfirmationOp was assigned to 17th index. Now it is gone.
    public static final int WHOIS_MASTER = 18;
    public static final int MEMBER_ATTR_CHANGED = 19;
    // MemberRemoveOperation was assigned to 20th index. Now it is gone.
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
    public static final int VECTOR_CLOCK = 43;
    public static final int EXTENDED_BIND_MESSAGE = 44;
    public static final int ENDPOINT_QUALIFIER = 45;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return typeId -> {
            switch (typeId) {
                case ADDRESS:
                    return new Address();
                case AUTHORIZATION:
                    return new CompatibilityWanAuthorizationOp();
                case EXTENDED_BIND_MESSAGE:
                    return new CompatibilityExtendedBindMessage();
                default:
                    return null;
            }
        };
    }
}
