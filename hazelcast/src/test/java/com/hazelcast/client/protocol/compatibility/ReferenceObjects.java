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

package com.hazelcast.client.protocol.compatibility;

import com.google.common.collect.ImmutableMap;
import com.hazelcast.cache.impl.CacheEventData;
import com.hazelcast.client.impl.client.DistributedObjectInfo;
import com.hazelcast.client.impl.protocol.codec.builtin.CustomTypeFactory;
import com.hazelcast.client.impl.protocol.codec.holder.AnchorDataListHolder;
import com.hazelcast.client.impl.protocol.codec.holder.CacheConfigHolder;
import com.hazelcast.client.impl.protocol.codec.holder.PagingPredicateHolder;
import com.hazelcast.client.impl.protocol.exception.ErrorHolder;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.EvictionConfigHolder;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.ListenerConfigHolder;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.MapStoreConfigHolder;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.NearCacheConfigHolder;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.PredicateConfigHolder;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.QueryCacheConfigHolder;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.QueueStoreConfigHolder;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.RingbufferStoreConfigHolder;
import com.hazelcast.cluster.Address;
import com.hazelcast.config.AttributeConfig;
import com.hazelcast.config.BitmapIndexOptions;
import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.DurationConfig;
import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig;
import com.hazelcast.config.CacheSimpleEntryListenerConfig;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.config.HotRestartConfig;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.config.MerkleTreeConfig;
import com.hazelcast.config.NearCachePreloaderConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.internal.CPMemberInfo;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.ProtocolType;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.management.dto.ClientBwListEntryDTO;
import com.hazelcast.internal.management.dto.MCEventDTO;
import com.hazelcast.internal.partition.MigrationStateImpl;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.internal.serialization.impl.compact.FieldDescriptor;
import com.hazelcast.internal.serialization.impl.compact.Schema;
import com.hazelcast.map.impl.SimpleEntryView;
import com.hazelcast.map.impl.querycache.event.DefaultQueryCacheEventData;
import com.hazelcast.map.impl.querycache.event.QueryCacheEventData;
import com.hazelcast.partition.MigrationState;
import com.hazelcast.scheduledexecutor.ScheduledTaskHandler;
import com.hazelcast.scheduledexecutor.impl.ScheduledTaskHandlerImpl;
import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.client.SqlError;
import com.hazelcast.sql.impl.client.SqlPage;
import com.hazelcast.transaction.impl.xa.SerializableXID;
import com.hazelcast.version.MemberVersion;

import javax.transaction.xa.Xid;
import java.lang.reflect.Array;
import java.net.UnknownHostException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.UUID;

public class ReferenceObjects {

    public static boolean isEqual(Object a, Object b) {
        if (a == b) {
            return true;
        }
        if (a == null || b == null) {
            return false;
        }
        if (a.getClass().isArray() && b.getClass().isArray()) {

            int length = Array.getLength(a);
            if (length > 0 && !a.getClass().getComponentType().equals(b.getClass().getComponentType())) {
                return false;
            }
            if (Array.getLength(b) != length) {
                return false;
            }
            for (int i = 0; i < length; i++) {

                Object aElement = Array.get(a, i);
                Object bElement = Array.get(b, i);
                if (aElement instanceof StackTraceElement && bElement instanceof StackTraceElement) {
                    if (!isEqualStackTrace((StackTraceElement) aElement, (StackTraceElement) bElement)) {
                        return false;
                    }
                }
                if (!isEqual(aElement, bElement)) {
                    return false;
                }
            }
            return true;
        }
        if (a instanceof List && b instanceof List) {
            ListIterator e1 = ((List) a).listIterator();
            ListIterator e2 = ((List) b).listIterator();
            while (e1.hasNext() && e2.hasNext()) {
                Object o1 = e1.next();
                Object o2 = e2.next();
                if (!isEqual(o1, o2)) {
                    return false;
                }
            }
            return !(e1.hasNext() || e2.hasNext());
        }
        if (a instanceof Entry && b instanceof Entry) {
            final Entry entryA = (Entry) a;
            final Entry entryB = (Entry) b;
            return isEqual(entryA.getKey(), entryB.getKey()) && isEqual(entryA.getValue(), entryB.getValue());
        }
        // following classes are list elements and have to be explicitly cast
        if (a instanceof ListenerConfigHolder && b instanceof ListenerConfigHolder) {
            return isEqual((ListenerConfigHolder) a, (ListenerConfigHolder) b);
        }
        if (a instanceof IndexConfig && b instanceof IndexConfig) {
            return isEqual((IndexConfig) a, (IndexConfig) b);
        }
        if (a instanceof AttributeConfig && b instanceof AttributeConfig) {
            return isEqual((AttributeConfig) a, (AttributeConfig) b);
        }
        if (a instanceof QueryCacheConfigHolder && b instanceof QueryCacheConfigHolder) {
            return isEqual((QueryCacheConfigHolder) a, (QueryCacheConfigHolder) b);
        }
        if (a instanceof CacheSimpleEntryListenerConfig && b instanceof CacheSimpleEntryListenerConfig) {
            return isEqual((CacheSimpleEntryListenerConfig) a, (CacheSimpleEntryListenerConfig) b);
        }
        return a.equals(b);
    }

    public static boolean isEqual(CacheConfigHolder a, CacheConfigHolder b) {
        if (a == b) {
            return true;
        }
        if (b == null) {
            return false;
        }
        if (!a.getName().equals(b.getName())) {
            return false;
        }
        if (!Objects.equals(a.getManagerPrefix(), b.getManagerPrefix())) {
            return false;
        }
        if (!Objects.equals(a.getUriString(), b.getUriString())) {
            return false;
        }
        if (a.getBackupCount() != b.getBackupCount()) {
            return false;
        }
        if (a.getAsyncBackupCount() != b.getAsyncBackupCount()) {
            return false;
        }
        if (!a.getInMemoryFormat().equals(b.getInMemoryFormat())) {
            return false;
        }
        if (!isEqual(a.getEvictionConfigHolder(), b.getEvictionConfigHolder())) {
            return false;
        }
        if (!isEqual(a.getWanReplicationRef(), b.getWanReplicationRef())) {
            return false;
        }
        if (!a.getKeyClassName().equals(b.getKeyClassName())) {
            return false;
        }
        if (!a.getValueClassName().equals(b.getValueClassName())) {
            return false;
        }
        if (!Objects.equals(a.getCacheLoaderFactory(), b.getCacheLoaderFactory())) {
            return false;
        }
        if (!Objects.equals(a.getCacheWriterFactory(), b.getCacheWriterFactory())) {
            return false;
        }
        if (!a.getExpiryPolicyFactory().equals(b.getExpiryPolicyFactory())) {
            return false;
        }
        if (a.isReadThrough() != b.isReadThrough()) {
            return false;
        }
        if (a.isWriteThrough() != b.isWriteThrough()) {
            return false;
        }
        if (a.isStoreByValue() != b.isStoreByValue()) {
            return false;
        }
        if (a.isManagementEnabled() != b.isManagementEnabled()) {
            return false;
        }
        if (a.isStatisticsEnabled() != b.isStatisticsEnabled()) {
            return false;
        }
        if (!isEqual(a.getHotRestartConfig(), b.getHotRestartConfig())) {
            return false;
        }
        if (!isEqual(a.getEventJournalConfig(), b.getEventJournalConfig())) {
            return false;
        }
        if (!Objects.equals(a.getSplitBrainProtectionName(), b.getSplitBrainProtectionName())) {
            return false;
        }
        if (!Objects.equals(a.getListenerConfigurations(), b.getListenerConfigurations())) {
            return false;
        }
        if (!isEqual(a.getMergePolicyConfig(), b.getMergePolicyConfig())) {
            return false;
        }
        return a.isDisablePerEntryInvalidationEvents() == b.isDisablePerEntryInvalidationEvents();
    }

    public static boolean isEqual(WanReplicationRef a, WanReplicationRef b) {
        if (a == b) {
            return true;
        }
        if (b == null) {
            return false;
        }

        if (a.isRepublishingEnabled() != b.isRepublishingEnabled()) {
            return false;
        }
        if (!a.getName().equals(b.getName())) {
            return false;
        }
        if (!a.getMergePolicyClassName().equals(b.getMergePolicyClassName())) {
            return false;
        }
        return a.getFilters() != null ? a.getFilters().equals(b.getFilters()) : b.getFilters() == null;
    }

    public static boolean isEqual(NearCachePreloaderConfig a, NearCachePreloaderConfig b) {
        if (a == b) {
            return true;
        }
        if (b == null) {
            return false;
        }

        if (a.isEnabled() != b.isEnabled()) {
            return false;
        }
        if (a.getStoreInitialDelaySeconds() != b.getStoreInitialDelaySeconds()) {
            return false;
        }
        if (a.getStoreIntervalSeconds() != b.getStoreIntervalSeconds()) {
            return false;
        }
        return a.getDirectory() != null ? a.getDirectory().equals(b.getDirectory()) : b.getDirectory() == null;
    }

    public static boolean isEqual(NearCacheConfigHolder a, NearCacheConfigHolder that) {
        if (a == that) {
            return true;
        }
        if (that == null) {
            return false;
        }

        if (a.isSerializeKeys() != that.isSerializeKeys()) {
            return false;
        }
        if (a.isInvalidateOnChange() != that.isInvalidateOnChange()) {
            return false;
        }
        if (a.getTimeToLiveSeconds() != that.getTimeToLiveSeconds()) {
            return false;
        }
        if (a.getMaxIdleSeconds() != that.getMaxIdleSeconds()) {
            return false;
        }
        if (a.isCacheLocalEntries() != that.isCacheLocalEntries()) {
            return false;
        }
        if (!a.getName().equals(that.getName())) {
            return false;
        }
        if (!a.getInMemoryFormat().equals(that.getInMemoryFormat())) {
            return false;
        }
        if (!isEqual(a.getEvictionConfigHolder(), that.getEvictionConfigHolder())) {
            return false;
        }
        if (!a.getLocalUpdatePolicy().equals(that.getLocalUpdatePolicy())) {
            return false;
        }
        return a.getPreloaderConfig() != null ? isEqual(a.getPreloaderConfig(), that.getPreloaderConfig())
                : that.getPreloaderConfig() == null;
    }

    public static boolean isEqual(EvictionConfigHolder a, EvictionConfigHolder b) {
        if (a == b) {
            return true;
        }
        if (b == null) {
            return false;
        }

        if (a.getSize() != b.getSize()) {
            return false;
        }
        if (!a.getMaxSizePolicy().equals(b.getMaxSizePolicy())) {
            return false;
        }
        if (!a.getEvictionPolicy().equals(b.getEvictionPolicy())) {
            return false;
        }
        if (a.getComparatorClassName() != null ? !a.getComparatorClassName().equals(b.getComparatorClassName()) :
                b.getComparatorClassName() != null) {
            return false;
        }
        return a.getComparator() != null ? a.getComparator().equals(b.getComparator())
                : b.getComparator() == null;
    }

    public static boolean isEqual(ListenerConfigHolder a, ListenerConfigHolder b) {
        if (a == b) {
            return true;
        }
        if (b == null) {
            return false;
        }

        if (a.isIncludeValue() != b.isIncludeValue()) {
            return false;
        }
        if (a.isLocal() != b.isLocal()) {
            return false;
        }
        if (a.getListenerType() != b.getListenerType()) {
            return false;
        }
        if (a.getClassName() != null ? !a.getClassName().equals(b.getClassName())
                : b.getClassName() != null) {
            return false;
        }
        return a.getListenerImplementation() != null
                ? a.getListenerImplementation().equals(b.getListenerImplementation())
                : b.getListenerImplementation() == null;
    }

    public static boolean isEqual(IndexConfig a, IndexConfig that) {
        if (a == that) {
            return true;
        }
        if (that == null) {
            return false;
        }

        if (a.getType() != that.getType()) {
            return false;
        }

        if (a.getName() != null ? !a.getName().equals(that.getName()) : that.getName() != null) {
            return false;
        }

        return a.getAttributes() != null ? a.getAttributes().equals(that.getAttributes()) : that.getAttributes() == null;
    }

    public static boolean isEqual(AttributeConfig a, AttributeConfig that) {
        if (a == that) {
            return true;
        }
        if (that == null) {
            return false;
        }

        if (a.getName() != null ? !a.getName().equals(that.getName()) : that.getName() != null) {
            return false;
        }
        return a.getExtractorClassName() != null ? a.getExtractorClassName().equals(that.getExtractorClassName())
                : that.getExtractorClassName() == null;
    }

    public static boolean isEqual(MapStoreConfigHolder a, MapStoreConfigHolder b) {
        if (a == b) {
            return true;
        }
        if (b == null) {
            return false;
        }

        if (a.isEnabled() != b.isEnabled()) {
            return false;
        }
        if (a.isWriteCoalescing() != b.isWriteCoalescing()) {
            return false;
        }
        if (a.getWriteBatchSize() != b.getWriteBatchSize()) {
            return false;
        }
        if (a.getClassName() != null ? !a.getClassName().equals(b.getClassName()) : b.getClassName() != null) {
            return false;
        }
        if (a.getFactoryClassName() != null ? !a.getFactoryClassName().equals(b.getFactoryClassName())
                : b.getFactoryClassName() != null) {
            return false;
        }
        if (a.getImplementation() != null ? !a.getImplementation().equals(b.getImplementation()) :
                b.getImplementation() != null) {
            return false;
        }
        if (a.getFactoryImplementation() != null ? !a.getFactoryImplementation().equals(b.getFactoryImplementation())
                : b.getFactoryImplementation() != null) {
            return false;
        }
        if (a.getProperties() != null ? !a.getProperties().equals(b.getProperties()) : b.getProperties() != null) {
            return false;
        }
        return a.getInitialLoadMode() != null ? a.getInitialLoadMode().equals(b.getInitialLoadMode()) :
                b.getInitialLoadMode() == null;
    }

    public static boolean isEqual(QueryCacheConfigHolder a, QueryCacheConfigHolder b) {
        if (a == b) {
            return true;
        }
        if (b == null) {
            return false;
        }

        if (a.getBatchSize() != b.getBatchSize()) {
            return false;
        }
        if (a.getBufferSize() != b.getBufferSize()) {
            return false;
        }
        if (a.getDelaySeconds() != b.getDelaySeconds()) {
            return false;
        }
        if (a.isIncludeValue() != b.isIncludeValue()) {
            return false;
        }
        if (a.isPopulate() != b.isPopulate()) {
            return false;
        }
        if (a.isCoalesce() != b.isCoalesce()) {
            return false;
        }
        if (!a.getInMemoryFormat().equals(b.getInMemoryFormat())) {
            return false;
        }
        if (!a.getName().equals(b.getName())) {
            return false;
        }
        if (!isEqual(a.getPredicateConfigHolder(), b.getPredicateConfigHolder())) {
            return false;
        }
        if (!isEqual(a.getEvictionConfigHolder(), b.getEvictionConfigHolder())) {
            return false;
        }
        if (a.getListenerConfigs() != null ? !isEqual(a.getListenerConfigs(), b.getListenerConfigs())
                : b.getListenerConfigs() != null) {
            return false;
        }
        return a.getIndexConfigs() != null ? isEqual(a.getIndexConfigs(), b.getIndexConfigs())
                : b.getIndexConfigs() == null;
    }

    public static boolean isEqual(PredicateConfigHolder a, PredicateConfigHolder b) {
        if (a == b) {
            return true;
        }
        if (b == null) {
            return false;
        }

        if (a.getClassName() != null ? !a.getClassName().equals(b.getClassName()) : a.getClassName() != null) {
            return false;
        }
        if (a.getSql() != null ? !a.getSql().equals(b.getSql()) : b.getSql() != null) {
            return false;
        }
        return a.getImplementation() != null ? a.getImplementation().equals(b.getImplementation()) :
                b.getImplementation() == null;
    }

    public static boolean isEqual(QueueStoreConfigHolder a, QueueStoreConfigHolder b) {
        if (a == b) {
            return true;
        }
        if (b == null) {
            return false;
        }

        if (a.isEnabled() != b.isEnabled()) {
            return false;
        }
        if (a.getClassName() != null ? !a.getClassName().equals(b.getClassName()) : b.getClassName() != null) {
            return false;
        }
        if (a.getFactoryClassName() != null ? !a.getFactoryClassName().equals(b.getFactoryClassName()) :
                b.getFactoryClassName() != null) {
            return false;
        }
        if (a.getImplementation() != null ? !a.getImplementation().equals(b.getImplementation()) :
                b.getImplementation() != null) {
            return false;
        }
        if (a.getFactoryClassName() != null ? !a.getFactoryImplementation().equals(b.getFactoryImplementation())
                : b.getFactoryImplementation() != null) {
            return false;
        }
        return a.getProperties() != null ? a.getProperties().equals(b.getProperties())
                : b.getProperties() == null;
    }

    public static boolean isEqual(HotRestartConfig a, HotRestartConfig b) {
        if (a == b) {
            return true;
        }
        if (b == null) {
            return false;
        }
        if (a.isEnabled() != b.isEnabled()) {
            return false;
        }

        return a.isFsync() == b.isFsync();
    }

    public static boolean isEqual(TimedExpiryPolicyFactoryConfig a, TimedExpiryPolicyFactoryConfig b) {
        if (a == b) {
            return true;
        }
        if (b == null) {
            return false;
        }
        if (a.getExpiryPolicyType() != b.getExpiryPolicyType()) {
            return false;
        }
        if (a.getDurationConfig().getDurationAmount() != b.getDurationConfig().getDurationAmount()) {
            return false;
        }
        return a.getDurationConfig().getTimeUnit() == b.getDurationConfig().getTimeUnit();
    }

    public static boolean isEqual(CacheSimpleEntryListenerConfig a, CacheSimpleEntryListenerConfig b) {
        if (a == b) {
            return true;
        }
        if (b == null) {
            return false;
        }
        if (a.isOldValueRequired() != b.isOldValueRequired()) {
            return false;
        }
        if (a.isSynchronous() != b.isSynchronous()) {
            return false;
        }
        if (!a.getCacheEntryEventFilterFactory().equals(b.getCacheEntryEventFilterFactory())) {
            return false;
        }
        return a.getCacheEntryListenerFactory().equals(b.getCacheEntryListenerFactory());
    }

    public static boolean isEqual(RingbufferStoreConfigHolder a, RingbufferStoreConfigHolder b) {
        if (a == b) {
            return true;
        }
        if (b == null) {
            return false;
        }

        if (a.isEnabled() != b.isEnabled()) {
            return false;
        }
        if (a.getClassName() != null ? !a.getClassName().equals(b.getClassName()) : b.getClassName() != null) {
            return false;
        }
        if (a.getFactoryClassName() != null ? !a.getFactoryClassName().equals(b.getFactoryClassName()) :
                b.getFactoryClassName() != null) {
            return false;
        }
        if (a.getImplementation() != null ? !a.getImplementation().equals(b.getImplementation()) :
                b.getImplementation() != null) {
            return false;
        }
        if (a.getFactoryImplementation() != null ? !a.getFactoryImplementation().equals(b.getFactoryImplementation()) :
                b.getFactoryImplementation() != null) {
            return false;
        }
        return a.getProperties() != null ? a.getProperties().equals(b.getProperties()) : b.getProperties() == null;
    }

    private static boolean isEqualStackTrace(StackTraceElement stackTraceElement1, StackTraceElement stackTraceElement2) {
        //Not using stackTraceElement.equals
        //because in IBM JDK stacktraceElements with null method name are not equal
        if (!isEqual(stackTraceElement1.getClassName(), stackTraceElement2.getClassName())) {
            return false;
        }
        if (!isEqual(stackTraceElement1.getMethodName(), stackTraceElement2.getMethodName())) {
            return false;
        }
        if (!isEqual(stackTraceElement1.getFileName(), stackTraceElement2.getFileName())) {
            return false;
        }
        return isEqual(stackTraceElement1.getLineNumber(), stackTraceElement2.getLineNumber());

    }

    // Static values below should not be a random value, because the values are used when generating compatibility files and
    // when testing against them. Random values causes test failures.
    public static boolean aBoolean = true;
    public static byte aByte = 113;
    public static int anInt = 25;
    public static int anEnum = 1;
    public static long aLong = -50992225L;
    public static UUID aUUID = new UUID(123456789, 987654321);
    public static byte[] aByteArray = new byte[]{aByte};
    public static long[] aLongArray = new long[]{aLong};
    public static String aString = "localhost";
    public static Data aData = new HeapData("111313123131313131".getBytes());
    public static List<Map.Entry<Integer, UUID>> aListOfIntegerToUUID
            = Collections.singletonList(new AbstractMap.SimpleEntry<>(anInt, aUUID));
    public static List<Map.Entry<Integer, Long>> aListOfIntegerToLong
            = Collections.singletonList(new AbstractMap.SimpleEntry<>(anInt, aLong));
    public static List<Map.Entry<Integer, Integer>> aListOfIntegerToInteger
            = Collections.singletonList(new AbstractMap.SimpleEntry<>(anInt, anInt));
    public static List<Map.Entry<UUID, Long>> aListOfUuidToLong
            = Collections.singletonList(new AbstractMap.SimpleEntry<>(aUUID, aLong));
    public static List<Map.Entry<UUID, UUID>> aListOfUUIDToUUID
            = Collections.singletonList(new AbstractMap.SimpleEntry<>(aUUID, aUUID));
    public static List<Integer> aListOfIntegers = Collections.singletonList(anInt);
    public static List<Long> aListOfLongs = Collections.singletonList(aLong);
    public static List<UUID> aListOfUUIDs = Collections.singletonList(aUUID);
    public static Address anAddress;
    public static CPMember aCpMember;
    public static List<CPMember> aListOfCpMembers;
    public static MigrationState aMigrationState = new MigrationStateImpl(aLong, anInt, anInt, aLong);
    public static FieldDescriptor aFieldDescriptor = CustomTypeFactory.createFieldDescriptor(aString, anInt);
    public static List<FieldDescriptor> aListOfFieldDescriptors = Collections.singletonList(aFieldDescriptor);
    public static Schema aSchema = CustomTypeFactory.createSchema(aString, aListOfFieldDescriptors);
    public static List<Schema> aListOfSchemas = Collections.singletonList(aSchema);

    static {
        try {
            anAddress = new Address(aString, anInt);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        aCpMember = new CPMemberInfo(aUUID, anAddress);
        aListOfCpMembers = Collections.singletonList(aCpMember);
    }

    public static List<Map.Entry<UUID, List<Integer>>> aListOfUUIDToListOfIntegers
            = Collections.singletonList(new AbstractMap.SimpleEntry<>(aUUID, aListOfIntegers));
    public static Map<String, String> aMapOfStringToString = Collections.singletonMap(aString, aString);
    public static List<String> aListOfStrings = Collections.singletonList(aString);
    public static StackTraceElement aStackTraceElement = new StackTraceElement(aString, aString, aString, anInt);
    public static List<StackTraceElement> aListOfStackTraceElements = Collections.singletonList(aStackTraceElement);
    public static CacheEventData aCacheEventData
            = CustomTypeFactory.createCacheEventData(aString, anEnum, aData, aData, aData, aBoolean);
    public static DistributedObjectInfo aDistributedObjectInfo = new DistributedObjectInfo(aString, aString);
    public static DefaultQueryCacheEventData aQueryCacheEventData;
    public static MCEventDTO aMCEvent = new MCEventDTO(aLong, anInt, aString);
    public static List<MCEventDTO> aListOfMCEvents = Collections.singletonList(aMCEvent);

    static {
        aQueryCacheEventData = new DefaultQueryCacheEventData();
        aQueryCacheEventData.setDataKey(aData);
        aQueryCacheEventData.setDataNewValue(aData);
        aQueryCacheEventData.setSequence(aLong);
        aQueryCacheEventData.setEventType(anInt);
        aQueryCacheEventData.setPartitionId(anInt);
    }

    public static RaftGroupId aRaftGroupId = new RaftGroupId(aString, aLong, aLong);
    public static ScheduledTaskHandler aScheduledTaskHandler = new ScheduledTaskHandlerImpl(aUUID, anInt, aString, aString);
    public static SimpleEntryView<Data, Data> aSimpleEntryView = new SimpleEntryView<>(aData, aData);

    static {
        aSimpleEntryView.setCost(aLong);
        aSimpleEntryView.setCreationTime(aLong);
        aSimpleEntryView.setExpirationTime(aLong);
        aSimpleEntryView.setHits(aLong);
        aSimpleEntryView.setLastAccessTime(aLong);
        aSimpleEntryView.setLastStoredTime(aLong);
        aSimpleEntryView.setLastUpdateTime(aLong);
        aSimpleEntryView.setVersion(aLong);
        aSimpleEntryView.setTtl(aLong);
        aSimpleEntryView.setMaxIdle(aLong);
    }

    public static WanReplicationRef aWanReplicationRef = new WanReplicationRef(aString, aString, aListOfStrings, aBoolean);
    public static Xid anXid = new SerializableXID(anInt, aByteArray, aByteArray);
    public static ErrorHolder anErrorHolder = new ErrorHolder(anInt, aString, aString, aListOfStackTraceElements);
    public static CacheSimpleEntryListenerConfig aCacheSimpleEntryListenerConfig;

    static {
        aCacheSimpleEntryListenerConfig = new CacheSimpleEntryListenerConfig();
        aCacheSimpleEntryListenerConfig.setOldValueRequired(aBoolean);
        aCacheSimpleEntryListenerConfig.setSynchronous(aBoolean);
        aCacheSimpleEntryListenerConfig.setCacheEntryListenerFactory(aString);
        aCacheSimpleEntryListenerConfig.setCacheEntryEventFilterFactory(aString);
    }

    public static EventJournalConfig anEventJournalConfig;

    static {
        anEventJournalConfig = new EventJournalConfig();
        anEventJournalConfig.setEnabled(aBoolean);
        anEventJournalConfig.setCapacity(anInt);
        anEventJournalConfig.setTimeToLiveSeconds(anInt);
    }

    public static EvictionConfigHolder anEvictionConfigHolder = new EvictionConfigHolder(anInt, aString, aString, aString, aData);
    public static HotRestartConfig aHotRestartConfig;

    static {
        aHotRestartConfig = new HotRestartConfig();
        aHotRestartConfig.setEnabled(aBoolean);
        aHotRestartConfig.setFsync(aBoolean);
    }

    public static MerkleTreeConfig aMerkleTreeConfig;

    static {
        aMerkleTreeConfig = new MerkleTreeConfig();
        aMerkleTreeConfig.setEnabled(aBoolean);
        aMerkleTreeConfig.setDepth(anInt);
    }


    public static ListenerConfigHolder aListenerConfigHolder = new ListenerConfigHolder(ListenerConfigHolder.ListenerConfigType.ITEM, aData, aString, aBoolean, aBoolean);
    public static AttributeConfig anAttributeConfig = new AttributeConfig(aString, aString);
    public static BitmapIndexOptions aBitmapIndexOptions;

    static {
        aBitmapIndexOptions = new BitmapIndexOptions();
        aBitmapIndexOptions.setUniqueKey(aString);
        aBitmapIndexOptions.setUniqueKeyTransformation(BitmapIndexOptions.UniqueKeyTransformation.LONG);
    }

    public static IndexConfig anIndexConfig = CustomTypeFactory.createIndexConfig(aString, anEnum, aListOfStrings, aBitmapIndexOptions);
    public static MapStoreConfigHolder aMapStoreConfigHolder = new MapStoreConfigHolder(aBoolean, aBoolean, anInt, anInt, aString, aData, aString, aData, aMapOfStringToString, aString);

    public static NearCachePreloaderConfig aNearCachePreloaderConfig = new NearCachePreloaderConfig(aBoolean, aString);

    static {
        aNearCachePreloaderConfig.setStoreInitialDelaySeconds(anInt);
        aNearCachePreloaderConfig.setStoreIntervalSeconds(anInt);
    }

    public static NearCacheConfigHolder aNearCacheConfigHolder = new NearCacheConfigHolder(aString, aString, aBoolean, aBoolean, anInt, anInt, anEvictionConfigHolder, aBoolean, aString, aNearCachePreloaderConfig);
    public static PredicateConfigHolder aPredicateConfigHolder = new PredicateConfigHolder(aString, aString, aData);
    public static List<ListenerConfigHolder> aListOfListenerConfigHolders = Collections.singletonList(aListenerConfigHolder);
    public static List<IndexConfig> aListOfIndexConfigs = Collections.singletonList(anIndexConfig);
    public static QueryCacheConfigHolder aQueryCacheConfigHolder = new QueryCacheConfigHolder(anInt, anInt, anInt, aBoolean, aBoolean, aBoolean, aString, aString, aPredicateConfigHolder, anEvictionConfigHolder, aListOfListenerConfigHolders, aListOfIndexConfigs, aBoolean, aBoolean);
    public static QueueStoreConfigHolder aQueueStoreConfigHolder = new QueueStoreConfigHolder(aString, aString, aData, aData, aMapOfStringToString, aBoolean);
    public static RingbufferStoreConfigHolder aRingbufferStoreConfigHolder = new RingbufferStoreConfigHolder(aString, aString, aData, aData, aMapOfStringToString, aBoolean);
    public static DurationConfig aDurationConfig = CustomTypeFactory.createDurationConfig(aLong, anEnum);
    public static TimedExpiryPolicyFactoryConfig aTimedExpiryPolicyFactoryConfig = CustomTypeFactory.createTimedExpiryPolicyFactoryConfig(anEnum, aDurationConfig);
    public static ClientBwListEntryDTO aClientBwListEntry = CustomTypeFactory.createClientBwListEntry(anEnum, aString);
    public static List<Map.Entry<String, String>> aListOfStringToString
            = Collections.singletonList(new AbstractMap.SimpleEntry<>(aString, aString));
    public static List<Map.Entry<String, byte[]>> aListOfStringToByteArray
            = Collections.singletonList(new AbstractMap.SimpleEntry<>(aString, aByteArray));
    public static List<Map.Entry<Long, byte[]>> aListOfLongToByteArray
            = Collections.singletonList(new AbstractMap.SimpleEntry<>(aLong, aByteArray));
    public static List<Map.Entry<String, List<Map.Entry<Integer, Long>>>> aListOfStringToListOfIntegerToLong
            = Collections.singletonList(new AbstractMap.SimpleEntry<>(aString, aListOfIntegerToLong));
    public static List<Map.Entry<Data, Data>> aListOfDataToData
            = Collections.singletonList(new AbstractMap.SimpleEntry<>(aData, aData));

    public static List<CacheEventData> aListOfCacheEventData = Collections.singletonList(aCacheEventData);
    public static List<CacheSimpleEntryListenerConfig> aListOfCacheSimpleEntryListenerConfigs
            = Collections.singletonList(aCacheSimpleEntryListenerConfig);
    public static List<Data> aListOfData = Collections.singletonList(aData);
    public static List<Object> aListOfObject = Collections.singletonList(anInt);
    public static List<Collection<Data>> aListOfListOfData = Collections.singletonList(aListOfData);
    public static List<Collection<Object>> aListOfListOfObject = Collections.singletonList(aListOfObject);
    public static Collection<Map.Entry<Data, Collection<Data>>> aListOfDataToListOfData
            = Collections.singletonList(new AbstractMap.SimpleEntry<>(aData, aListOfData));
    public static List<DistributedObjectInfo> aListOfDistributedObjectInfo = Collections.singletonList(aDistributedObjectInfo);
    public static List<AttributeConfig> aListOfAttributeConfigs = Collections.singletonList(anAttributeConfig);
    public static List<QueryCacheConfigHolder> aListOfQueryCacheConfigHolders = Collections.singletonList(aQueryCacheConfigHolder);
    public static List<QueryCacheEventData> aListOfQueryCacheEventData = Collections.singletonList(aQueryCacheEventData);
    public static List<ScheduledTaskHandler> aListOfScheduledTaskHandler = Collections.singletonList(aScheduledTaskHandler);
    public static List<Xid> aListOfXids = Collections.singletonList(anXid);
    public static List<ClientBwListEntryDTO> aListOfClientBwListEntries = Collections.singletonList(aClientBwListEntry);
    public static MergePolicyConfig aMergePolicyConfig = new MergePolicyConfig(aString, anInt);
    public static CacheConfigHolder aCacheConfigHolder = new CacheConfigHolder(aString, aString, aString, anInt, anInt,
            aString, anEvictionConfigHolder, aWanReplicationRef, aString, aString, aData, aData, aData, aBoolean,
            aBoolean, aBoolean, aBoolean, aBoolean, aHotRestartConfig, anEventJournalConfig, aString, aListOfData,
            aMergePolicyConfig, aBoolean, aListOfListenerConfigHolders, aBoolean, aMerkleTreeConfig);
    private static MemberVersion aMemberVersion = new MemberVersion(aByte, aByte, aByte);
    public static Collection<MemberInfo> aListOfMemberInfos = Collections.singletonList(new MemberInfo(anAddress, aUUID, aMapOfStringToString, aBoolean, aMemberVersion,
            ImmutableMap.of(EndpointQualifier.resolve(ProtocolType.WAN, "localhost"), anAddress)));

    public static AnchorDataListHolder anAnchorDataListHolder = new AnchorDataListHolder(aListOfIntegers, aListOfDataToData);
    public static PagingPredicateHolder aPagingPredicateHolder = new PagingPredicateHolder(anAnchorDataListHolder, aData, aData,
            anInt, anInt, aByte, aData);

    public static QueryId anSqlQueryId = new QueryId(aLong, aLong, aLong, aLong);
    public static SqlColumnMetadata anSqlColumnMetadata = CustomTypeFactory.createSqlColumnMetadata(aString, SqlColumnType.BOOLEAN.getId(), aBoolean, aBoolean);
    public static List<SqlColumnMetadata> aListOfSqlColumnMetadata = Collections.singletonList(anSqlColumnMetadata);
    public static SqlError anSqlError = new SqlError(anInt, aString, aUUID, aBoolean, aString);
    public static SqlPage aSqlPage = SqlPage.fromColumns(Collections.singletonList(SqlColumnType.INTEGER), Collections.singletonList(Arrays.asList(1, 2, 3, 4)), true);
}
