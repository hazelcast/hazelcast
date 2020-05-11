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

import com.hazelcast.internal.compatibility.version.CompatibilityMemberVersion;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.version.Version;

/**
 * Data serializer hook containing (de)serialization information for communicating
 * with 4.x members over WAN.
 */
public final class CompatibilityClusterDataSerializerHook implements DataSerializerHook {

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
        return new DataSerializableFactory() {
            @Override
            public IdentifiedDataSerializable create(int typeId) {
                switch (typeId) {
                    case ADDRESS:
                        return new Address();
                    case VERSION:
                        return new Version();
                    case BIND_MESSAGE:
                        return new CompatibilityBindMessage();
                    case MEMBER_VERSION:
                        return new CompatibilityMemberVersion();
                    default:
                        return null;
                }
            }
        };
    }
}
