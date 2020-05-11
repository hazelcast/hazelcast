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

package com.hazelcast.internal.compatibility.cache;

import com.hazelcast.internal.compatibility.serialization.impl.CompatibilityFactoryIdHelper;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.nio.serialization.DataSerializableFactory;

import static com.hazelcast.internal.compatibility.serialization.impl.CompatibilityFactoryIdHelper.CACHE_DS_FACTORY;
import static com.hazelcast.internal.compatibility.serialization.impl.CompatibilityFactoryIdHelper.CACHE_DS_FACTORY_ID;


/**
 * Data serializer hook containing (de)serialization information for
 * JCache-related classes used when communicating with 3.x members over WAN.
 */
public final class CompatibilityCacheDataSerializerHook
        implements DataSerializerHook {

    public static final int F_ID = CompatibilityFactoryIdHelper.getFactoryId(
            CACHE_DS_FACTORY, CACHE_DS_FACTORY_ID);
    public static final short GET = 1;
    public static final short CONTAINS_KEY = 2;
    public static final short PUT = 3;
    public static final short PUT_IF_ABSENT = 4;
    public static final short REMOVE = 5;
    public static final short GET_AND_REMOVE = 6;
    public static final short REPLACE = 7;
    public static final short GET_AND_REPLACE = 8;
    public static final short PUT_BACKUP = 9;
    public static final short PUT_ALL_BACKUP = 10;
    public static final short REMOVE_BACKUP = 11;
    public static final short CLEAR_BACKUP = 12;
    public static final short SIZE = 13;
    public static final short SIZE_FACTORY = 14;
    public static final short CLEAR = 15;
    public static final short CLEAR_FACTORY = 16;
    public static final short GET_ALL = 17;
    public static final short GET_ALL_FACTORY = 18;
    public static final short LOAD_ALL = 19;
    public static final short LOAD_ALL_FACTORY = 20;
    public static final short EXPIRY_POLICY = 21;
    public static final short KEY_ITERATOR = 22;
    public static final short KEY_ITERATION_RESULT = 23;
    public static final short ENTRY_PROCESSOR = 24;
    public static final short CLEAR_RESPONSE = 25;
    public static final short CREATE_CONFIG = 26;
    public static final short GET_CONFIG = 27;
    public static final short MANAGEMENT_CONFIG = 28;
    public static final short LISTENER_REGISTRATION = 29;
    public static final short DESTROY_CACHE = 30;
    public static final short CACHE_EVENT_DATA = 31;
    public static final short CACHE_EVENT_DATA_SET = 32;
    public static final short BACKUP_ENTRY_PROCESSOR = 33;
    public static final short REMOVE_ALL = 34;
    public static final short REMOVE_ALL_BACKUP = 35;
    public static final short REMOVE_ALL_FACTORY = 36;
    public static final short PUT_ALL = 37;
    public static final short LEGACY_MERGE = 38;
    public static final short INVALIDATION_MESSAGE = 39;
    public static final short BATCH_INVALIDATION_MESSAGE = 40;
    public static final short ENTRY_ITERATOR = 41;
    public static final short ENTRY_ITERATION_RESULT = 42;
    public static final short CACHE_PARTITION_LOST_EVENT_FILTER = 43;
    public static final short DEFAULT_CACHE_ENTRY_VIEW = 44;
    public static final short CACHE_REPLICATION = 45;
    public static final short CACHE_POST_JOIN = 46;
    public static final short CACHE_DATA_RECORD = 47;
    public static final short CACHE_OBJECT_RECORD = 48;
    public static final short CACHE_PARTITION_EVENT_DATA = 49;

    public static final short CACHE_INVALIDATION_METADATA = 50;
    public static final short CACHE_INVALIDATION_METADATA_RESPONSE = 51;
    public static final short CACHE_ASSIGN_AND_GET_UUIDS = 52;
    public static final short CACHE_ASSIGN_AND_GET_UUIDS_FACTORY = 53;
    public static final short CACHE_NEAR_CACHE_STATE_HOLDER = 54;
    public static final short CACHE_EVENT_LISTENER_ADAPTOR = 55;
    public static final short EVENT_JOURNAL_SUBSCRIBE_OPERATION = 56;
    public static final short EVENT_JOURNAL_READ_OPERATION = 57;
    public static final short EVENT_JOURNAL_DESERIALIZING_CACHE_EVENT = 58;
    public static final short EVENT_JOURNAL_INTERNAL_CACHE_EVENT = 59;
    public static final short EVENT_JOURNAL_READ_RESULT_SET = 60;
    public static final int PRE_JOIN_CACHE_CONFIG = 61;
    public static final int CACHE_BROWSER_ENTRY_VIEW = 62;
    public static final int GET_CACHE_ENTRY_VIEW_PROCESSOR = 63;

    public static final int MERGE_FACTORY = 64;
    public static final int MERGE = 65;
    public static final int ADD_CACHE_CONFIG_OPERATION = 66;
    public static final int SET_EXPIRY_POLICY = 67;
    public static final int SET_EXPIRY_POLICY_BACKUP = 68;
    public static final int EXPIRE_BATCH_BACKUP = 69;
    public static final int CACHE_DESTROY_EVENT_CONTEXT = 70;

    public int getFactoryId() {
        return F_ID;
    }

    public DataSerializableFactory createFactory() {
        return typeId -> {
            switch (typeId) {
                case DEFAULT_CACHE_ENTRY_VIEW:
                    return new CompatibilityWanCacheEntryView();
                default:
                    return null;
            }
        };
    }
}
