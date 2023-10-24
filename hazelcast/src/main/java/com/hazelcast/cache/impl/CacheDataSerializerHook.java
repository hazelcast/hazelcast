/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.impl;

import com.hazelcast.cache.HazelcastExpiryPolicy;
import com.hazelcast.cache.impl.event.CachePartitionLostEventFilter;
import com.hazelcast.cache.impl.journal.CacheEventJournalReadOperation;
import com.hazelcast.cache.impl.journal.CacheEventJournalReadResultSetImpl;
import com.hazelcast.cache.impl.journal.CacheEventJournalSubscribeOperation;
import com.hazelcast.cache.impl.journal.DeserializingEventJournalCacheEvent;
import com.hazelcast.cache.impl.journal.InternalEventJournalCacheEvent;
import com.hazelcast.cache.impl.merge.entry.DefaultCacheEntryView;
import com.hazelcast.cache.impl.operation.AddCacheConfigOperation;
import com.hazelcast.cache.impl.operation.CacheBackupEntryProcessorOperation;
import com.hazelcast.cache.impl.operation.CacheClearBackupOperation;
import com.hazelcast.cache.impl.operation.CacheClearOperation;
import com.hazelcast.cache.impl.operation.CacheClearOperationFactory;
import com.hazelcast.cache.impl.operation.CacheContainsKeyOperation;
import com.hazelcast.cache.impl.operation.CacheDestroyOperation;
import com.hazelcast.cache.impl.operation.CacheEntryProcessorOperation;
import com.hazelcast.cache.impl.operation.CacheExpireBatchBackupOperation;
import com.hazelcast.cache.impl.operation.CacheFetchEntriesOperation;
import com.hazelcast.cache.impl.operation.CacheFetchKeysOperation;
import com.hazelcast.cache.impl.operation.CacheGetAllOperation;
import com.hazelcast.cache.impl.operation.CacheGetAllOperationFactory;
import com.hazelcast.cache.impl.operation.CacheGetAndRemoveOperation;
import com.hazelcast.cache.impl.operation.CacheGetAndReplaceOperation;
import com.hazelcast.cache.impl.operation.CacheGetConfigOperation;
import com.hazelcast.cache.impl.operation.CacheGetInvalidationMetaDataOperation;
import com.hazelcast.cache.impl.operation.CacheGetInvalidationMetaDataOperation.MetaDataResponse;
import com.hazelcast.cache.impl.operation.CacheGetOperation;
import com.hazelcast.cache.impl.operation.CacheListenerRegistrationOperation;
import com.hazelcast.cache.impl.operation.CacheLoadAllOperation;
import com.hazelcast.cache.impl.operation.CacheLoadAllOperationFactory;
import com.hazelcast.cache.impl.operation.CacheManagementConfigOperation;
import com.hazelcast.cache.impl.operation.CacheMergeOperation;
import com.hazelcast.cache.impl.operation.CacheMergeOperationFactory;
import com.hazelcast.cache.impl.operation.CacheNearCacheStateHolder;
import com.hazelcast.cache.impl.operation.CachePutAllBackupOperation;
import com.hazelcast.cache.impl.operation.CachePutAllOperation;
import com.hazelcast.cache.impl.operation.CachePutBackupOperation;
import com.hazelcast.cache.impl.operation.CachePutIfAbsentOperation;
import com.hazelcast.cache.impl.operation.CachePutOperation;
import com.hazelcast.cache.impl.operation.CacheRemoveAllBackupOperation;
import com.hazelcast.cache.impl.operation.CacheRemoveAllOperation;
import com.hazelcast.cache.impl.operation.CacheRemoveAllOperationFactory;
import com.hazelcast.cache.impl.operation.CacheRemoveBackupOperation;
import com.hazelcast.cache.impl.operation.CacheRemoveOperation;
import com.hazelcast.cache.impl.operation.CacheReplaceOperation;
import com.hazelcast.cache.impl.operation.CacheReplicationOperation;
import com.hazelcast.cache.impl.operation.CacheSetExpiryPolicyBackupOperation;
import com.hazelcast.cache.impl.operation.CacheSetExpiryPolicyOperation;
import com.hazelcast.cache.impl.operation.CacheSizeOperation;
import com.hazelcast.cache.impl.operation.CacheSizeOperationFactory;
import com.hazelcast.cache.impl.operation.OnJoinCacheOperation;
import com.hazelcast.cache.impl.record.CacheDataRecord;
import com.hazelcast.cache.impl.record.CacheObjectRecord;
import com.hazelcast.client.impl.protocol.task.cache.CacheAssignAndGetUuidsOperation;
import com.hazelcast.client.impl.protocol.task.cache.CacheAssignAndGetUuidsOperationFactory;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.internal.management.operation.GetCacheEntryViewEntryProcessor;
import com.hazelcast.internal.management.operation.GetCacheEntryViewEntryProcessor.CacheBrowserEntryView;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.ArrayDataSerializableFactory;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.util.function.Supplier;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.CACHE_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.CACHE_DS_FACTORY_ID;

/**
 * {@link CacheDataSerializerHook} contains all the ID hooks for {@link IdentifiedDataSerializable} classes used
 * inside the JCache framework.
 * <p>CacheProxy operations are mapped here. This factory class is used by internal serialization system to create
 * {@link IdentifiedDataSerializable} classes without using reflection.</p>
 */
public final class CacheDataSerializerHook
        implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(CACHE_DS_FACTORY, CACHE_DS_FACTORY_ID);
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
    public static final short GET_CONFIG = 26;
    public static final short MANAGEMENT_CONFIG = 27;
    public static final short LISTENER_REGISTRATION = 28;
    public static final short DESTROY_CACHE = 29;
    public static final short CACHE_EVENT_DATA = 30;
    public static final short CACHE_EVENT_DATA_SET = 31;
    public static final short BACKUP_ENTRY_PROCESSOR = 32;
    public static final short REMOVE_ALL = 33;
    public static final short REMOVE_ALL_BACKUP = 34;
    public static final short REMOVE_ALL_FACTORY = 35;
    public static final short PUT_ALL = 36;
    public static final short ENTRY_ITERATOR = 37;
    public static final short ENTRY_ITERATION_RESULT = 38;
    public static final short CACHE_PARTITION_LOST_EVENT_FILTER = 39;
    public static final short DEFAULT_CACHE_ENTRY_VIEW = 40;
    public static final short CACHE_REPLICATION = 41;
    public static final short CACHE_POST_JOIN = 42;
    public static final short CACHE_DATA_RECORD = 43;
    public static final short CACHE_OBJECT_RECORD = 44;
    public static final short CACHE_PARTITION_EVENT_DATA = 45;

    public static final short CACHE_INVALIDATION_METADATA = 46;
    public static final short CACHE_INVALIDATION_METADATA_RESPONSE = 47;
    public static final short CACHE_ASSIGN_AND_GET_UUIDS = 48;
    public static final short CACHE_ASSIGN_AND_GET_UUIDS_FACTORY = 49;
    public static final short CACHE_NEAR_CACHE_STATE_HOLDER = 50;
    public static final short CACHE_EVENT_LISTENER_ADAPTOR = 51;
    public static final short EVENT_JOURNAL_SUBSCRIBE_OPERATION = 52;
    public static final short EVENT_JOURNAL_READ_OPERATION = 53;
    public static final short EVENT_JOURNAL_DESERIALIZING_CACHE_EVENT = 54;
    public static final short EVENT_JOURNAL_INTERNAL_CACHE_EVENT = 55;
    public static final short EVENT_JOURNAL_READ_RESULT_SET = 56;
    public static final int PRE_JOIN_CACHE_CONFIG = 57;
    public static final int CACHE_BROWSER_ENTRY_VIEW = 58;
    public static final int GET_CACHE_ENTRY_VIEW_PROCESSOR = 59;

    public static final int MERGE_FACTORY = 60;
    public static final int MERGE = 61;
    public static final int ADD_CACHE_CONFIG_OPERATION = 62;
    public static final int SET_EXPIRY_POLICY = 63;
    public static final int SET_EXPIRY_POLICY_BACKUP = 64;
    public static final int EXPIRE_BATCH_BACKUP = 65;
    public static final int CACHE_CONFIG = 67;

    private static final int LEN = CACHE_CONFIG + 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        Supplier<IdentifiedDataSerializable>[] constructors = new Supplier[LEN];
        constructors[GET] = CacheGetOperation::new;
        constructors[CONTAINS_KEY] = CacheContainsKeyOperation::new;
        constructors[PUT] = CachePutOperation::new;
        constructors[PUT_IF_ABSENT] = CachePutIfAbsentOperation::new;
        constructors[REMOVE] = CacheRemoveOperation::new;
        constructors[GET_AND_REMOVE] = CacheGetAndRemoveOperation::new;
        constructors[REPLACE] = CacheReplaceOperation::new;
        constructors[GET_AND_REPLACE] = CacheGetAndReplaceOperation::new;
        constructors[PUT_BACKUP] = CachePutBackupOperation::new;
        constructors[PUT_ALL_BACKUP] = CachePutAllBackupOperation::new;
        constructors[REMOVE_BACKUP] = CacheRemoveBackupOperation::new;
        constructors[SIZE] = CacheSizeOperation::new;
        constructors[SIZE_FACTORY] = CacheSizeOperationFactory::new;
        constructors[CLEAR_FACTORY] = CacheClearOperationFactory::new;
        constructors[GET_ALL] = CacheGetAllOperation::new;
        constructors[GET_ALL_FACTORY] = CacheGetAllOperationFactory::new;
        constructors[LOAD_ALL] = CacheLoadAllOperation::new;
        constructors[LOAD_ALL_FACTORY] = CacheLoadAllOperationFactory::new;
        constructors[EXPIRY_POLICY] = HazelcastExpiryPolicy::new;
        constructors[KEY_ITERATOR] = CacheFetchKeysOperation::new;
        constructors[KEY_ITERATION_RESULT] = CacheKeysWithCursor::new;
        constructors[ENTRY_PROCESSOR] = CacheEntryProcessorOperation::new;
        constructors[CLEAR_RESPONSE] = CacheClearResponse::new;
        constructors[GET_CONFIG] = CacheGetConfigOperation::new;
        constructors[MANAGEMENT_CONFIG] = CacheManagementConfigOperation::new;
        constructors[LISTENER_REGISTRATION] = CacheListenerRegistrationOperation::new;
        constructors[DESTROY_CACHE] = CacheDestroyOperation::new;
        constructors[CACHE_EVENT_DATA] = CacheEventDataImpl::new;
        constructors[CACHE_EVENT_DATA_SET] = CacheEventSet::new;
        constructors[BACKUP_ENTRY_PROCESSOR] = CacheBackupEntryProcessorOperation::new;
        constructors[CLEAR] = CacheClearOperation::new;
        constructors[CLEAR_BACKUP] = CacheClearBackupOperation::new;
        constructors[REMOVE_ALL] = CacheRemoveAllOperation::new;
        constructors[REMOVE_ALL_BACKUP] = CacheRemoveAllBackupOperation::new;
        constructors[REMOVE_ALL_FACTORY] = CacheRemoveAllOperationFactory::new;
        constructors[PUT_ALL] = CachePutAllOperation::new;
        constructors[ENTRY_ITERATOR] = CacheFetchEntriesOperation::new;
        constructors[ENTRY_ITERATION_RESULT] = CacheEntriesWithCursor::new;
        constructors[CACHE_PARTITION_LOST_EVENT_FILTER] = CachePartitionLostEventFilter::new;
        constructors[DEFAULT_CACHE_ENTRY_VIEW] = DefaultCacheEntryView::new;
        constructors[CACHE_REPLICATION] = CacheReplicationOperation::new;
        constructors[CACHE_POST_JOIN] = OnJoinCacheOperation::new;
        constructors[CACHE_DATA_RECORD] = CacheDataRecord::new;
        constructors[CACHE_OBJECT_RECORD] = CacheObjectRecord::new;
        constructors[CACHE_PARTITION_EVENT_DATA] = CachePartitionEventData::new;
        constructors[CACHE_INVALIDATION_METADATA] = CacheGetInvalidationMetaDataOperation::new;
        constructors[CACHE_INVALIDATION_METADATA_RESPONSE] = MetaDataResponse::new;
        constructors[CACHE_ASSIGN_AND_GET_UUIDS] = CacheAssignAndGetUuidsOperation::new;
        constructors[CACHE_ASSIGN_AND_GET_UUIDS_FACTORY] = CacheAssignAndGetUuidsOperationFactory::new;
        constructors[CACHE_NEAR_CACHE_STATE_HOLDER] = CacheNearCacheStateHolder::new;
        constructors[CACHE_EVENT_LISTENER_ADAPTOR] = CacheEventListenerAdaptor::new;
        constructors[EVENT_JOURNAL_SUBSCRIBE_OPERATION] = CacheEventJournalSubscribeOperation::new;
        constructors[EVENT_JOURNAL_READ_OPERATION] = CacheEventJournalReadOperation::new;
        constructors[EVENT_JOURNAL_DESERIALIZING_CACHE_EVENT] = DeserializingEventJournalCacheEvent::new;
        constructors[EVENT_JOURNAL_INTERNAL_CACHE_EVENT] = InternalEventJournalCacheEvent::new;
        constructors[EVENT_JOURNAL_READ_RESULT_SET] = CacheEventJournalReadResultSetImpl::new;
        constructors[PRE_JOIN_CACHE_CONFIG] = PreJoinCacheConfig::new;
        constructors[CACHE_BROWSER_ENTRY_VIEW] = CacheBrowserEntryView::new;
        constructors[GET_CACHE_ENTRY_VIEW_PROCESSOR] = GetCacheEntryViewEntryProcessor::new;
        constructors[MERGE_FACTORY] = CacheMergeOperationFactory::new;
        constructors[MERGE] = CacheMergeOperation::new;
        constructors[ADD_CACHE_CONFIG_OPERATION] = AddCacheConfigOperation::new;
        constructors[SET_EXPIRY_POLICY] = CacheSetExpiryPolicyOperation::new;
        constructors[SET_EXPIRY_POLICY_BACKUP] = CacheSetExpiryPolicyBackupOperation::new;
        constructors[EXPIRE_BATCH_BACKUP] = CacheExpireBatchBackupOperation::new;
        constructors[CACHE_CONFIG] = CacheConfig::new;

        return new ArrayDataSerializableFactory(constructors);
    }
}
