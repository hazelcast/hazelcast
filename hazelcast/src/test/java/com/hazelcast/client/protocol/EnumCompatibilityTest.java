/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.client.protocol;

import com.hazelcast.cache.CacheEventType;
import com.hazelcast.client.impl.protocol.AuthenticationStatus;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.ListenerConfigHolder;
import com.hazelcast.client.impl.proxy.PartitionServiceProxy;
import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.config.BitmapIndexOptions.UniqueKeyTransformation;
import com.hazelcast.config.CacheDeserializedValues;
import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig.ExpiryPolicyType;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.config.MetadataPolicy;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.DistributedObjectEvent;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.ItemEventType;
import com.hazelcast.cp.internal.datastructures.atomiclong.operation.AlterOp;
import com.hazelcast.cp.internal.datastructures.atomicref.operation.ApplyOp;
import com.hazelcast.instance.ProtocolType;
import com.hazelcast.internal.management.dto.ClientBwListEntryDTO;
import com.hazelcast.internal.util.IterationType;
import com.hazelcast.jet.impl.TerminationMode;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.ringbuffer.OverflowPolicy;
import com.hazelcast.scheduledexecutor.impl.TaskDefinition;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.SqlExpectedResultType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.topic.TopicOverloadPolicy;
import com.hazelcast.transaction.TransactionOptions;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;

/**
 * Ensures that the enums (or enum-like constants) used in the protocol
 * have well known member orderings and there won't be new enum members
 * added without considering the client-side implications of it. The
 * tests use hardcoded values intentionally to make sure that the actual
 * values are unchanged.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EnumCompatibilityTest {

    @Test
    public void testCacheEventType() {
        // Used in CacheEventDataCodec
        Map<CacheEventType, Integer> mappings = new HashMap<>();
        mappings.put(CacheEventType.CREATED, 1);
        mappings.put(CacheEventType.UPDATED, 2);
        mappings.put(CacheEventType.REMOVED, 3);
        mappings.put(CacheEventType.EXPIRED, 4);
        mappings.put(CacheEventType.EVICTED, 5);
        mappings.put(CacheEventType.INVALIDATED, 6);
        mappings.put(CacheEventType.COMPLETED, 7);
        mappings.put(CacheEventType.EXPIRATION_TIME_UPDATED, 8);
        mappings.put(CacheEventType.PARTITION_LOST, 9);
        verifyCompatibility(CacheEventType.values(), CacheEventType::getType, mappings);
    }

    @Test
    public void testIndexType() {
        // Used in IndexConfigCodec
        Map<IndexType, Integer> mappings = new HashMap<>();
        mappings.put(IndexType.SORTED, 0);
        mappings.put(IndexType.HASH, 1);
        mappings.put(IndexType.BITMAP, 2);
        verifyCompatibility(IndexType.values(), IndexType::getId, mappings);
    }

    @Test
    public void testUniqueKeyTransformation() {
        // Used in BitmapIndexOptionsCodec
        Map<UniqueKeyTransformation, Integer> mappings = new HashMap<>();
        mappings.put(UniqueKeyTransformation.OBJECT, 0);
        mappings.put(UniqueKeyTransformation.LONG, 1);
        mappings.put(UniqueKeyTransformation.RAW, 2);
        verifyCompatibility(UniqueKeyTransformation.values(), UniqueKeyTransformation::getId, mappings);
    }

    @Test
    public void testExpiryPolicyType() {
        // Used in TimedExpiryPolicyFactoryConfigCodec
        Map<ExpiryPolicyType, Integer> mappings = new HashMap<>();
        mappings.put(ExpiryPolicyType.CREATED, 0);
        mappings.put(ExpiryPolicyType.MODIFIED, 1);
        mappings.put(ExpiryPolicyType.ACCESSED, 2);
        mappings.put(ExpiryPolicyType.TOUCHED, 3);
        mappings.put(ExpiryPolicyType.ETERNAL, 4);
        verifyCompatibility(ExpiryPolicyType.values(), ExpiryPolicyType::getId, mappings);
    }

    @Test
    public void testProtocolType() {
        // Used in EndpointQualifierCodec
        Map<ProtocolType, Integer> mappings = new HashMap<>();
        mappings.put(ProtocolType.MEMBER, 0);
        mappings.put(ProtocolType.CLIENT, 1);
        mappings.put(ProtocolType.WAN, 2);
        mappings.put(ProtocolType.REST, 3);
        mappings.put(ProtocolType.MEMCACHE, 4);
        verifyCompatibility(ProtocolType.values(), ProtocolType::getId, mappings);
    }

    @Test
    public void testFieldType() {
        // Used in FieldDescriptorCodec
        Map<FieldType, Integer> mappings = new HashMap<>();
        mappings.put(FieldType.PORTABLE, 0);
        mappings.put(FieldType.BYTE, 1);
        mappings.put(FieldType.BOOLEAN, 2);
        mappings.put(FieldType.CHAR, 3);
        mappings.put(FieldType.SHORT, 4);
        mappings.put(FieldType.INT, 5);
        mappings.put(FieldType.LONG, 6);
        mappings.put(FieldType.FLOAT, 7);
        mappings.put(FieldType.DOUBLE, 8);
        mappings.put(FieldType.UTF, 9);
        mappings.put(FieldType.PORTABLE_ARRAY, 10);
        mappings.put(FieldType.BYTE_ARRAY, 11);
        mappings.put(FieldType.BOOLEAN_ARRAY, 12);
        mappings.put(FieldType.CHAR_ARRAY, 13);
        mappings.put(FieldType.SHORT_ARRAY, 14);
        mappings.put(FieldType.INT_ARRAY, 15);
        mappings.put(FieldType.LONG_ARRAY, 16);
        mappings.put(FieldType.FLOAT_ARRAY, 17);
        mappings.put(FieldType.DOUBLE_ARRAY, 18);
        mappings.put(FieldType.UTF_ARRAY, 19);
        mappings.put(FieldType.DECIMAL, 20);
        mappings.put(FieldType.DECIMAL_ARRAY, 21);
        mappings.put(FieldType.TIME, 22);
        mappings.put(FieldType.TIME_ARRAY, 23);
        mappings.put(FieldType.DATE, 24);
        mappings.put(FieldType.DATE_ARRAY, 25);
        mappings.put(FieldType.TIMESTAMP, 26);
        mappings.put(FieldType.TIMESTAMP_ARRAY, 27);
        mappings.put(FieldType.TIMESTAMP_WITH_TIMEZONE, 28);
        mappings.put(FieldType.TIMESTAMP_WITH_TIMEZONE_ARRAY, 29);
        mappings.put(FieldType.COMPOSED, 30);
        mappings.put(FieldType.COMPOSED_ARRAY, 31);
        verifyCompatibility(FieldType.values(), (fieldType -> (int) fieldType.getId()), mappings);
    }

    @Test
    public void testClientBwListEntryDTOType() {
        // Used in ClientBwListEntryCodec
        Map<ClientBwListEntryDTO.Type, Integer> mappings = new HashMap<>();
        mappings.put(ClientBwListEntryDTO.Type.IP_ADDRESS, 0);
        mappings.put(ClientBwListEntryDTO.Type.INSTANCE_NAME, 1);
        mappings.put(ClientBwListEntryDTO.Type.LABEL, 2);
        verifyCompatibility(ClientBwListEntryDTO.Type.values(), ClientBwListEntryDTO.Type::getId, mappings);
    }

    @Test
    public void testSqlColumnType() {
        // Used in SqlColumnMetadataCodec
        Map<SqlColumnType, Integer> mappings = new HashMap<>();
        mappings.put(SqlColumnType.VARCHAR, 0);
        mappings.put(SqlColumnType.BOOLEAN, 1);
        mappings.put(SqlColumnType.TINYINT, 2);
        mappings.put(SqlColumnType.SMALLINT, 3);
        mappings.put(SqlColumnType.INTEGER, 4);
        mappings.put(SqlColumnType.BIGINT, 5);
        mappings.put(SqlColumnType.DECIMAL, 6);
        mappings.put(SqlColumnType.REAL, 7);
        mappings.put(SqlColumnType.DOUBLE, 8);
        mappings.put(SqlColumnType.DATE, 9);
        mappings.put(SqlColumnType.TIME, 10);
        mappings.put(SqlColumnType.TIMESTAMP, 11);
        mappings.put(SqlColumnType.TIMESTAMP_WITH_TIME_ZONE, 12);
        mappings.put(SqlColumnType.OBJECT, 13);
        mappings.put(SqlColumnType.NULL, 14);
        verifyCompatibility(SqlColumnType.values(), SqlColumnType::getId, mappings);
    }

    @Test
    public void testAtomicLongAlterOpAlterResultType() {
        // Used in AtomicLongAlterCodec
        Map<AlterOp.AlterResultType, Integer> mappings = new HashMap<>();
        mappings.put(AlterOp.AlterResultType.OLD_VALUE, 0);
        mappings.put(AlterOp.AlterResultType.NEW_VALUE, 1);
        verifyCompatibility(AlterOp.AlterResultType.values(), AlterOp.AlterResultType::value, mappings);
    }

    @Test
    public void testAtomicRefApplyOpReturnValueType() {
        // Used in AtomicRefApplyCodec
        Map<ApplyOp.ReturnValueType, Integer> mappings = new HashMap<>();
        mappings.put(ApplyOp.ReturnValueType.NO_RETURN_VALUE, 0);
        mappings.put(ApplyOp.ReturnValueType.RETURN_OLD_VALUE, 1);
        mappings.put(ApplyOp.ReturnValueType.RETURN_NEW_VALUE, 2);
        verifyCompatibility(ApplyOp.ReturnValueType.values(), ApplyOp.ReturnValueType::value, mappings);
    }

    @Test
    public void testDistributedObjectEventType() {
        // Used in ClientAddDistributedObjectListenerCodec
        Map<DistributedObjectEvent.EventType, String> mappings = new HashMap<>();
        mappings.put(DistributedObjectEvent.EventType.CREATED, "CREATED");
        mappings.put(DistributedObjectEvent.EventType.DESTROYED, "DESTROYED");
        verifyCompatibility(DistributedObjectEvent.EventType.values(), DistributedObjectEvent.EventType::name, mappings);
    }

    @Test
    public void testMigrationProcessStateType() {
        // Used in ClientAddMigrationListenerCodec
        Map<PartitionServiceProxy.MigrationProcessState, Integer> mappings = new HashMap<>();
        mappings.put(PartitionServiceProxy.MigrationProcessState.STARTED, 0);
        mappings.put(PartitionServiceProxy.MigrationProcessState.FINISHED, 1);
        verifyCompatibility(PartitionServiceProxy.MigrationProcessState.values(),
                PartitionServiceProxy.MigrationProcessState::getId, mappings);
    }

    @Test
    public void testValueCollectionType() {
        // Used in DynamicConfigAddMultiMapConfigCodec
        Map<MultiMapConfig.ValueCollectionType, String> mappings = new HashMap<>();
        mappings.put(MultiMapConfig.ValueCollectionType.SET, "SET");
        mappings.put(MultiMapConfig.ValueCollectionType.LIST, "LIST");
        verifyCompatibility(MultiMapConfig.ValueCollectionType.values(),
                MultiMapConfig.ValueCollectionType::name, mappings);
    }

    @Test
    public void testInMemoryFormat() {
        // Used in DynamicConfigAddReplicatedMapConfigCodec
        Map<InMemoryFormat, String> mappings = new HashMap<>();
        mappings.put(InMemoryFormat.BINARY, "BINARY");
        mappings.put(InMemoryFormat.OBJECT, "OBJECT");
        mappings.put(InMemoryFormat.NATIVE, "NATIVE");
        verifyCompatibility(InMemoryFormat.values(), InMemoryFormat::name, mappings);
    }

    @Test
    public void testCacheDeserializedValues() {
        // Used in DynamicConfigAddMapConfigCodec
        Map<CacheDeserializedValues, String> mappings = new HashMap<>();
        mappings.put(CacheDeserializedValues.NEVER, "NEVER");
        mappings.put(CacheDeserializedValues.ALWAYS, "ALWAYS");
        mappings.put(CacheDeserializedValues.INDEX_ONLY, "INDEX_ONLY");
        verifyCompatibility(CacheDeserializedValues.values(), CacheDeserializedValues::name, mappings);
    }

    @Test
    public void testTopicOverloadPolicy() {
        // Used in DynamicConfigAddReliableTopicConfigCodec
        Map<TopicOverloadPolicy, String> mappings = new HashMap<>();
        mappings.put(TopicOverloadPolicy.DISCARD_OLDEST, "DISCARD_OLDEST");
        mappings.put(TopicOverloadPolicy.DISCARD_NEWEST, "DISCARD_NEWEST");
        mappings.put(TopicOverloadPolicy.BLOCK, "BLOCK");
        mappings.put(TopicOverloadPolicy.ERROR, "ERROR");
        verifyCompatibility(TopicOverloadPolicy.values(), TopicOverloadPolicy::name, mappings);
    }

    @Test
    public void testMetadataPolicy() {
        // Used in DynamicConfigAddMapConfigCodec
        Map<MetadataPolicy, Integer> mappings = new HashMap<>();
        mappings.put(MetadataPolicy.CREATE_ON_UPDATE, 0);
        mappings.put(MetadataPolicy.OFF, 1);
        verifyCompatibility(MetadataPolicy.values(), MetadataPolicy::getId, mappings);
    }

    @Test
    public void testTerminationMode() {
        // Used in JetTerminateJobCodec
        Map<TerminationMode, Integer> mappings = new HashMap<>();
        mappings.put(TerminationMode.RESTART_GRACEFUL, 0);
        mappings.put(TerminationMode.RESTART_FORCEFUL, 1);
        mappings.put(TerminationMode.SUSPEND_GRACEFUL, 2);
        mappings.put(TerminationMode.SUSPEND_FORCEFUL, 3);
        mappings.put(TerminationMode.CANCEL_GRACEFUL, 4);
        mappings.put(TerminationMode.CANCEL_FORCEFUL, 5);
        verifyCompatibility(TerminationMode.values(), TerminationMode::ordinal, mappings);
    }

    @Test
    public void testItemEventType() {
        // Used in ListAddListenerCodec
        Map<ItemEventType, Integer> mappings = new HashMap<>();
        mappings.put(ItemEventType.ADDED, 1);
        mappings.put(ItemEventType.REMOVED, 2);
        verifyCompatibility(ItemEventType.values(), ItemEventType::getType, mappings);
    }

    @Test
    public void testEntryEventType() {
        // Used in MapAddEntryListenerCodec
        Map<EntryEventType, Integer> mappings = new HashMap<>();
        mappings.put(EntryEventType.ADDED, 1);
        mappings.put(EntryEventType.REMOVED, 2);
        mappings.put(EntryEventType.UPDATED, 4);
        mappings.put(EntryEventType.EVICTED, 8);
        mappings.put(EntryEventType.EXPIRED, 16);
        mappings.put(EntryEventType.EVICT_ALL, 32);
        mappings.put(EntryEventType.CLEAR_ALL, 64);
        mappings.put(EntryEventType.MERGED, 128);
        mappings.put(EntryEventType.INVALIDATION, 256);
        mappings.put(EntryEventType.LOADED, 512);
        verifyCompatibility(EntryEventType.values(), EntryEventType::getType, mappings);
    }

    @Test
    public void testOverflowPolicy() {
        // Used in RingbufferAddCodec
        Map<OverflowPolicy, Integer> mappings = new HashMap<>();
        mappings.put(OverflowPolicy.OVERWRITE, 0);
        mappings.put(OverflowPolicy.FAIL, 1);
        verifyCompatibility(OverflowPolicy.values(), OverflowPolicy::getId, mappings);
    }

    @Test
    public void testTransactionType() {
        // Used in TransactionCreateCodec
        Map<TransactionOptions.TransactionType, Integer> mappings = new HashMap<>();
        mappings.put(TransactionOptions.TransactionType.ONE_PHASE, 1);
        mappings.put(TransactionOptions.TransactionType.TWO_PHASE, 2);
        verifyCompatibility(TransactionOptions.TransactionType.values(), TransactionOptions.TransactionType::id, mappings);
    }

    @Test
    public void testMaxSizePolicy() {
        // Used in EvictionConfigHolderCodec
        Map<MaxSizePolicy, String> mappings = new HashMap<>();
        mappings.put(MaxSizePolicy.PER_NODE, "PER_NODE");
        mappings.put(MaxSizePolicy.PER_PARTITION, "PER_PARTITION");
        mappings.put(MaxSizePolicy.USED_HEAP_PERCENTAGE, "USED_HEAP_PERCENTAGE");
        mappings.put(MaxSizePolicy.USED_HEAP_SIZE, "USED_HEAP_SIZE");
        mappings.put(MaxSizePolicy.FREE_HEAP_PERCENTAGE, "FREE_HEAP_PERCENTAGE");
        mappings.put(MaxSizePolicy.FREE_HEAP_SIZE, "FREE_HEAP_SIZE");
        mappings.put(MaxSizePolicy.ENTRY_COUNT, "ENTRY_COUNT");
        mappings.put(MaxSizePolicy.USED_NATIVE_MEMORY_SIZE, "USED_NATIVE_MEMORY_SIZE");
        mappings.put(MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE, "USED_NATIVE_MEMORY_PERCENTAGE");
        mappings.put(MaxSizePolicy.FREE_NATIVE_MEMORY_SIZE, "FREE_NATIVE_MEMORY_SIZE");
        mappings.put(MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE, "FREE_NATIVE_MEMORY_PERCENTAGE");
        verifyCompatibility(MaxSizePolicy.values(), MaxSizePolicy::name, mappings);
    }

    @Test
    public void testEvictionPolicy() {
        // Used in EvictionConfigHolderCodec
        Map<EvictionPolicy, String> mappings = new HashMap<>();
        mappings.put(EvictionPolicy.LRU, "LRU");
        mappings.put(EvictionPolicy.LFU, "LFU");
        mappings.put(EvictionPolicy.NONE, "NONE");
        mappings.put(EvictionPolicy.RANDOM, "RANDOM");
        verifyCompatibility(EvictionPolicy.values(), EvictionPolicy::name, mappings);
    }

    @Test
    public void testInitialLoadMode() {
        // Used in MapStoreConfigHolderCodec
        Map<MapStoreConfig.InitialLoadMode, String> mappings = new HashMap<>();
        mappings.put(MapStoreConfig.InitialLoadMode.LAZY, "LAZY");
        mappings.put(MapStoreConfig.InitialLoadMode.EAGER, "EAGER");
        verifyCompatibility(MapStoreConfig.InitialLoadMode.values(), MapStoreConfig.InitialLoadMode::name, mappings);
    }

    @Test
    public void testLocalUpdatePolicy() {
        // Used in NearCacheConfigHolder
        Map<NearCacheConfig.LocalUpdatePolicy, String> mappings = new HashMap<>();
        mappings.put(NearCacheConfig.LocalUpdatePolicy.INVALIDATE, "INVALIDATE");
        mappings.put(NearCacheConfig.LocalUpdatePolicy.CACHE_ON_UPDATE, "CACHE_ON_UPDATE");
        verifyCompatibility(NearCacheConfig.LocalUpdatePolicy.values(), NearCacheConfig.LocalUpdatePolicy::name, mappings);
    }

    @Test
    public void testListenerConfigType() {
        // Used in ListenerConfigHolderCodec
        Map<ListenerConfigHolder.ListenerConfigType, Integer> mappings = new HashMap<>();
        mappings.put(ListenerConfigHolder.ListenerConfigType.GENERIC, 0);
        mappings.put(ListenerConfigHolder.ListenerConfigType.ITEM, 1);
        mappings.put(ListenerConfigHolder.ListenerConfigType.ENTRY, 2);
        mappings.put(ListenerConfigHolder.ListenerConfigType.SPLIT_BRAIN_PROTECTION, 3);
        mappings.put(ListenerConfigHolder.ListenerConfigType.CACHE_PARTITION_LOST, 4);
        mappings.put(ListenerConfigHolder.ListenerConfigType.MAP_PARTITION_LOST, 5);
        verifyCompatibility(ListenerConfigHolder.ListenerConfigType.values(), ListenerConfigHolder.ListenerConfigType::getType, mappings);
    }

    @Test
    public void testAuthenticationStatus() {
        // Used in ClientAuthenticationCodec
        Map<AuthenticationStatus, Byte> mappings = new HashMap<>();
        mappings.put(AuthenticationStatus.AUTHENTICATED, (byte) 0);
        mappings.put(AuthenticationStatus.CREDENTIALS_FAILED, (byte) 1);
        mappings.put(AuthenticationStatus.SERIALIZATION_VERSION_MISMATCH, (byte) 2);
        mappings.put(AuthenticationStatus.NOT_ALLOWED_IN_CLUSTER, (byte) 3);
        verifyCompatibility(AuthenticationStatus.values(), AuthenticationStatus::getId, mappings);
    }

    @Test
    public void testTaskDefinitionType() {
        // Used in ScheduleexpectedResultTypedExecutorSubmitToPartitionCodec
        Map<TaskDefinition.Type, Byte> mappings = new HashMap<>();
        mappings.put(TaskDefinition.Type.SINGLE_RUN, (byte) 0);
        mappings.put(TaskDefinition.Type.AT_FIXED_RATE, (byte) 1);
        verifyCompatibility(TaskDefinition.Type.values(), TaskDefinition.Type::getId, mappings);
    }

    @Test
    public void testSqlExpectedResultType() {
        // Used in SqlExecuteCodec
        Map<SqlExpectedResultType, Byte> mappings = new HashMap<>();
        mappings.put(SqlExpectedResultType.ANY, (byte) 0);
        mappings.put(SqlExpectedResultType.ROWS, (byte) 1);
        mappings.put(SqlExpectedResultType.UPDATE_COUNT, (byte) 2);
        verifyCompatibility(SqlExpectedResultType.values(), SqlExpectedResultType::getId, mappings);
    }

    @Test
    public void testMembershipEvent() {
        // Used in CPSubsystemAddMembershipListenerCodec
        // We can't convert this to enum as it is defined in the public API.
        // So, there is no way to warn for the new value additions. We just
        // test that the values of the already defined constants do not change.
        Map<Integer, Byte> mappings = new HashMap<>();
        mappings.put(MembershipEvent.MEMBER_ADDED, (byte) 1);
        mappings.put(MembershipEvent.MEMBER_REMOVED, (byte) 2);
        verifyCompatibility(new Integer[]{MembershipEvent.MEMBER_ADDED, MembershipEvent.MEMBER_REMOVED},
                Integer::byteValue, mappings);
    }

    @Test
    public void testIterationType() {
        // Used in PagingPredicateHolderCodec
        Map<IterationType, Byte> mappings = new HashMap<>();
        mappings.put(IterationType.KEY, (byte) 0);
        mappings.put(IterationType.VALUE, (byte) 1);
        mappings.put(IterationType.ENTRY, (byte) 2);
        verifyCompatibility(IterationType.values(), IterationType::getId, mappings);
    }

    private <T, V> void verifyCompatibility(T[] values, Function<T, V> toId, Map<T, V> mappings) {
        assertEquals("New values are added to the enum that is used in the client protocol. "
                        + "Make sure it does not cause compatibility issues in any of the clients "
                        + "and add the hardcoded value of the new enum member to mappings.",
                mappings.size(), values.length);

        for (T member : values) {
            V id = toId.apply(member);
            assertEquals("The id of the " + member + " that is used in the protocol is changed. "
                            + "Make sure it does not cause compatibility issues in any of the clients "
                            + "and update the mappings.",
                    mappings.get(member), id);
        }
    }
}
