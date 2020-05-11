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

package com.hazelcast.internal.compatibility.map;

import com.hazelcast.internal.compatibility.serialization.impl.CompatibilityFactoryIdHelper;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import static com.hazelcast.internal.compatibility.serialization.impl.CompatibilityFactoryIdHelper.MAP_DS_FACTORY;
import static com.hazelcast.internal.compatibility.serialization.impl.CompatibilityFactoryIdHelper.MAP_DS_FACTORY_ID;


/**
 * Data serializer hook containing (de)serialization information for communicating
 * with 4.x members over WAN.
 */
@SuppressWarnings("unused")
public final class CompatibilityMapDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = CompatibilityFactoryIdHelper.getFactoryId(MAP_DS_FACTORY, MAP_DS_FACTORY_ID);

    public static final int PUT = 0;
    public static final int GET = 1;
    public static final int REMOVE = 2;
    public static final int PUT_BACKUP = 3;
    public static final int REMOVE_BACKUP = 4;
    public static final int CREATE_ACCUMULATOR_INFO = 5;
    public static final int DATA_COLLECTION = 6;
    public static final int ENTRIES = 7;
    public static final int ENTRY_VIEW = 8;
    public static final int QUERY_RESULT_ROW = 9;
    public static final int QUERY_RESULT = 10;
    public static final int EVICT_BACKUP = 11;
    public static final int CONTAINS_KEY = 12;
    public static final int KEYS_WITH_CURSOR = 13;
    public static final int ENTRIES_WITH_CURSOR = 14;
    public static final int SET = 15;
    public static final int LOAD_MAP = 16;
    public static final int KEY_LOAD_STATUS = 17;
    public static final int LOAD_ALL = 18;
    public static final int ENTRY_BACKUP = 19;
    public static final int ENTRY_OPERATION = 20;
    public static final int PUT_ALL = 21;
    public static final int PUT_ALL_BACKUP = 22;
    public static final int REMOVE_IF_SAME = 23;
    public static final int REPLACE = 24;
    public static final int SIZE = 25;
    public static final int CLEAR_BACKUP = 26;
    public static final int CLEAR = 27;
    public static final int DELETE = 28;
    public static final int EVICT = 29;
    public static final int EVICT_ALL = 30;
    public static final int EVICT_ALL_BACKUP = 31;
    public static final int GET_ALL = 32;
    public static final int IS_EMPTY = 33;
    public static final int NEAR_CACHE_SINGLE_INVALIDATION = 34;
    public static final int NEAR_CACHE_BATCH_INVALIDATION = 35;
    public static final int IS_PARTITION_LOADED = 36;
    public static final int PARTITION_WIDE_ENTRY = 37;
    public static final int PARTITION_WIDE_ENTRY_BACKUP = 38;
    public static final int PARTITION_WIDE_PREDICATE_ENTRY = 39;
    public static final int PARTITION_WIDE_PREDICATE_ENTRY_BACKUP = 40;
    public static final int ADD_INDEX = 41;
    public static final int AWAIT_MAP_FLUSH = 42;
    public static final int CONTAINS_VALUE = 43;
    public static final int GET_ENTRY_VIEW = 44;
    public static final int FETCH_ENTRIES = 45;
    public static final int FETCH_KEYS = 46;
    public static final int FLUSH_BACKUP = 47;
    public static final int FLUSH = 48;
    public static final int MULTIPLE_ENTRY_BACKUP = 49;
    public static final int MULTIPLE_ENTRY = 50;
    public static final int MULTIPLE_ENTRY_PREDICATE_BACKUP = 51;
    public static final int MULTIPLE_ENTRY_PREDICATE = 52;
    public static final int NOTIFY_MAP_FLUSH = 53;
    public static final int PUT_IF_ABSENT = 54;
    public static final int PUT_FROM_LOAD_ALL = 55;
    public static final int PUT_FROM_LOAD_ALL_BACKUP = 56;
    public static final int QUERY_PARTITION = 57;
    public static final int QUERY_OPERATION = 58;
    public static final int PUT_TRANSIENT = 59;
    public static final int REPLACE_IF_SAME = 60;
    public static final int TRY_PUT = 61;
    public static final int TRY_REMOVE = 62;
    public static final int TXN_LOCK_AND_GET = 63;
    public static final int TXN_DELETE = 64;
    public static final int TXN_PREPARE = 65;
    public static final int TXN_PREPARE_BACKUP = 66;
    public static final int TXN_ROLLBACK = 67;
    public static final int TXN_ROLLBACK_BACKUP = 68;
    public static final int TXN_SET = 69;
    public static final int TXN_UNLOCK = 70;
    public static final int TXN_UNLOCK_BACKUP = 71;
    public static final int IS_PARTITION_LOADED_FACTORY = 72;
    public static final int ADD_INDEX_FACTORY = 73;
    public static final int CLEAR_FACTORY = 74;
    public static final int CONTAINS_VALUE_FACTORY = 75;
    public static final int EVICT_ALL_FACTORY = 76;
    public static final int IS_EMPTY_FACTORY = 77;
    public static final int KEY_LOAD_STATUS_FACTORY = 78;
    public static final int MAP_FLUSH_FACTORY = 79;
    public static final int MAP_GET_ALL_FACTORY = 80;
    public static final int LOAD_ALL_FACTORY = 81;
    public static final int PARTITION_WIDE_ENTRY_FACTORY = 82;
    public static final int PARTITION_WIDE_PREDICATE_ENTRY_FACTORY = 83;
    public static final int PUT_ALL_PARTITION_AWARE_FACTORY = 84;
    public static final int SIZE_FACTORY = 85;
    public static final int MULTIPLE_ENTRY_FACTORY = 86;
    public static final int ENTRY_EVENT_FILTER = 87;
    public static final int EVENT_LISTENER_FILTER = 88;
    public static final int PARTITION_LOST_EVENT_FILTER = 89;
    public static final int ADD_INTERCEPTOR = 90;
    public static final int MAP_REPLICATION = 91;
    public static final int POST_JOIN_MAP_OPERATION = 92;
    public static final int MAP_INDEX_INFO = 94;
    public static final int INTERCEPTOR_INFO = 95;
    public static final int REMOVE_INTERCEPTOR = 96;
    public static final int QUERY_EVENT_FILTER = 97;
    public static final int RECORD_INFO = 98;
    public static final int RECORD_REPLICATION_INFO = 99;
    public static final int UUID_FILTER = 100;
    public static final int MAP_TRANSACTION_LOG_RECORD = 101;
    public static final int VERSIONED_VALUE = 102;
    public static final int MAP_REPLICATION_STATE_HOLDER = 103;
    public static final int WRITE_BEHIND_STATE_HOLDER = 104;
    public static final int AGGREGATION_RESULT = 105;
    public static final int QUERY = 106;
    public static final int TARGET = 107;
    public static final int MAP_INVALIDATION_METADATA = 108;
    public static final int MAP_INVALIDATION_METADATA_RESPONSE = 109;
    public static final int MAP_NEAR_CACHE_STATE_HOLDER = 110;
    public static final int MAP_ASSIGN_AND_GET_UUIDS = 111;
    public static final int MAP_ASSIGN_AND_GET_UUIDS_FACTORY = 112;
    public static final int DESTROY_QUERY_CACHE = 113;
    public static final int MADE_PUBLISHABLE = 114;
    public static final int MADE_PUBLISHABLE_FACTORY = 115;
    public static final int PUBLISHER_CREATE = 116;
    public static final int READ_AND_RESET_ACCUMULATOR = 117;
    public static final int SET_READ_CURSOR = 118;
    public static final int ACCUMULATOR_CONSUMER = 119;
    public static final int LAZY_MAP_ENTRY = 120;
    public static final int TRIGGER_LOAD_IF_NEEDED = 121;
    public static final int IS_KEYLOAD_FINISHED = 122;
    public static final int REMOVE_FROM_LOAD_ALL = 123;
    public static final int ENTRY_REMOVING_PROCESSOR = 124;
    public static final int ENTRY_OFFLOADABLE_SET_UNLOCK = 125;
    public static final int LOCK_AWARE_LAZY_MAP_ENTRY = 126;
    public static final int FETCH_WITH_QUERY = 127;
    public static final int RESULT_SEGMENT = 128;
    public static final int EVICT_BATCH_BACKUP = 129;
    public static final int EVENT_JOURNAL_SUBSCRIBE_OPERATION = 130;
    public static final int EVENT_JOURNAL_READ = 131;
    public static final int EVENT_JOURNAL_DESERIALIZING_MAP_EVENT = 132;
    public static final int EVENT_JOURNAL_INTERNAL_MAP_EVENT = 133;
    public static final int EVENT_JOURNAL_READ_RESULT_SET = 134;
    public static final int MERGE_FACTORY = 135;
    public static final int MERGE = 136;
    public static final int SET_TTL = 137;
    public static final int SET_TTL_BACKUP = 138;
    public static final int MERKLE_TREE_NODE_ENTRIES = 139;
    public static final int ADD_INDEX_BACKUP = 140;
    public static final int TXN_SET_BACKUP = 141;
    public static final int TXN_DELETE_BACKUP = 142;
    public static final int SET_WITH_EXPIRY = 143;
    public static final int PUT_WITH_EXPIRY = 144;
    public static final int PUT_TRANSIENT_WITH_EXPIRY = 145;
    public static final int PUT_IF_ABSENT_WITH_EXPIRY = 146;
    public static final int PUT_TRANSIENT_BACKUP = 147;

    private static final int LEN = PUT_TRANSIENT_BACKUP + 1;

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
                    case ENTRY_VIEW:
                        return new CompatibilityWanMapEntryView<>();
                    case MERKLE_TREE_NODE_ENTRIES:
                        return new CompatibilityMerkleTreeNodeEntries();
                    default:
                        return null;
                }
            }
        };
    }
}
