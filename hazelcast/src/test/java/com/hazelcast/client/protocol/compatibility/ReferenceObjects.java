/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cache.CacheEventType;
import com.hazelcast.cache.CacheNotExistsException;
import com.hazelcast.cache.impl.CacheEventData;
import com.hazelcast.cache.impl.CacheEventDataImpl;
import com.hazelcast.cache.impl.event.CachePartitionLostEvent;
import com.hazelcast.cache.impl.event.CachePartitionLostListener;
import com.hazelcast.client.impl.MemberImpl;
import com.hazelcast.client.impl.StubAuthenticationException;
import com.hazelcast.client.impl.client.DistributedObjectInfo;
import com.hazelcast.client.impl.protocol.exception.MaxMessageSizeExceeded;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.EvictionConfigHolder;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.ListenerConfigHolder;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.MapStoreConfigHolder;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.NearCacheConfigHolder;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.PredicateConfigHolder;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.QueryCacheConfigHolder;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.QueueStoreConfigHolder;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.RingbufferStoreConfigHolder;
import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.DurationConfig;
import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig;
import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig.ExpiryPolicyType;
import com.hazelcast.config.CacheSimpleEntryListenerConfig;
import com.hazelcast.config.HotRestartConfig;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.MapAttributeConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.config.NearCachePreloaderConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.HazelcastOverloadException;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.durableexecutor.StaleTaskIdException;
import com.hazelcast.internal.cluster.impl.ConfigMismatchException;
import com.hazelcast.internal.eviction.EvictableEntryView;
import com.hazelcast.internal.eviction.EvictionPolicyComparator;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.BigEndianSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.map.QueryResultSizeExceededException;
import com.hazelcast.map.ReachedMaxSizeException;
import com.hazelcast.map.impl.SimpleEntryView;
import com.hazelcast.map.impl.querycache.event.DefaultQueryCacheEventData;
import com.hazelcast.map.impl.querycache.event.QueryCacheEventData;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.partition.NoDataMemberInClusterException;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.QueryException;
import com.hazelcast.quorum.QuorumException;
import com.hazelcast.replicatedmap.ReplicatedMapCantBeCreatedOnLiteMemberException;
import com.hazelcast.ringbuffer.StaleSequenceException;
import com.hazelcast.scheduledexecutor.ScheduledTaskHandler;
import com.hazelcast.scheduledexecutor.impl.ScheduledTaskHandlerImpl;
import com.hazelcast.spi.exception.CallerNotMemberException;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.spi.exception.PartitionMigratingException;
import com.hazelcast.spi.exception.ResponseAlreadySentException;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.exception.RetryableIOException;
import com.hazelcast.spi.exception.ServiceNotFoundException;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.exception.WrongTargetException;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.topic.TopicOverloadException;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionNotActiveException;
import com.hazelcast.transaction.TransactionTimedOutException;
import com.hazelcast.transaction.impl.xa.SerializableXID;
import com.hazelcast.util.AddressUtil;
import com.hazelcast.version.MemberVersion;
import com.hazelcast.wan.WANReplicationQueueFullException;

import javax.cache.CacheException;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import javax.cache.processor.EntryProcessorException;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginException;
import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;
import java.io.EOFException;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.Serializable;
import java.io.UTFDataFormatException;
import java.lang.reflect.Array;
import java.net.SocketException;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.security.AccessControlException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.client.impl.protocol.task.dynamicconfig.ListenerConfigHolder.TYPE_CACHE_PARTITION_LOST_LISTENER_CONFIG;
import static com.hazelcast.client.impl.protocol.task.dynamicconfig.ListenerConfigHolder.TYPE_ENTRY_LISTENER_CONFIG;
import static com.hazelcast.client.impl.protocol.task.dynamicconfig.ListenerConfigHolder.TYPE_ITEM_LISTENER_CONFIG;
import static com.hazelcast.client.impl.protocol.task.dynamicconfig.ListenerConfigHolder.TYPE_LISTENER_CONFIG;
import static com.hazelcast.client.impl.protocol.task.dynamicconfig.ListenerConfigHolder.TYPE_MAP_PARTITION_LOST_LISTENER_CONFIG;
import static com.hazelcast.client.impl.protocol.task.dynamicconfig.ListenerConfigHolder.TYPE_QUORUM_LISTENER_CONFIG;

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
        if (a instanceof MapIndexConfig && b instanceof MapIndexConfig) {
            return isEqual((MapIndexConfig) a, (MapIndexConfig) b);
        }
        if (a instanceof MapAttributeConfig && b instanceof MapAttributeConfig) {
            return isEqual((MapAttributeConfig) a, (MapAttributeConfig) b);
        }
        if (a instanceof QueryCacheConfigHolder && b instanceof QueryCacheConfigHolder) {
            return isEqual((QueryCacheConfigHolder) a, (QueryCacheConfigHolder) b);
        }
        if (a instanceof CacheSimpleEntryListenerConfig && b instanceof CacheSimpleEntryListenerConfig) {
            return isEqual((CacheSimpleEntryListenerConfig) a, (CacheSimpleEntryListenerConfig) b);
        }
        return a.equals(b);
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
        if (!a.getMergePolicy().equals(b.getMergePolicy())) {
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

    public static boolean isEqual(MapIndexConfig a, MapIndexConfig that) {
        if (a == that) {
            return true;
        }
        if (that == null) {
            return false;
        }

        if (a.isOrdered() != that.isOrdered()) {
            return false;
        }
        return a.getAttribute() != null ? a.getAttribute().equals(that.getAttribute())
                : that.getAttribute() == null;
    }

    public static boolean isEqual(MapAttributeConfig a, MapAttributeConfig that) {
        if (a == that) {
            return true;
        }
        if (that == null) {
            return false;
        }

        if (a.getName() != null ? !a.getName().equals(that.getName()) : that.getName() != null) {
            return false;
        }
        return a.getExtractor() != null ? a.getExtractor().equals(that.getExtractor())
                : that.getExtractor() == null;
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
    public static int anInt = 56789;
    public static long aLong = -50992225L;
    public static String aString = "SampleString";
    public static UUID aUUID = new UUID(123456789, 987654321);
    public static Throwable aThrowable = new HazelcastException(aString);
    public static Data aData = new HeapData("111313123131313131".getBytes());
    public static Address anAddress;

    static {
        try {
            anAddress = new Address("127.0.0.1", 5701);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    public static Member aMember = new MemberImpl(anAddress, MemberVersion.UNKNOWN, aString,
            Collections.singletonMap(aString, aString), false);
    public static Collection<Map.Entry<Address, List<Integer>>> aPartitionTable;

    static {
        Map<Address, List<Integer>> partitionsMap = new HashMap<Address, List<Integer>>();
        partitionsMap.put(anAddress, Collections.singletonList(1));
        aPartitionTable = new LinkedList<Map.Entry<Address, List<Integer>>>(partitionsMap.entrySet());
    }

    public static Collection<Map.Entry<Member, List<ScheduledTaskHandler>>> taskHandlers;

    static {
        Map<Member, List<ScheduledTaskHandler>> membersMap = new HashMap<Member, List<ScheduledTaskHandler>>();
        ScheduledTaskHandler scheduledTaskHandler = ScheduledTaskHandlerImpl.of(anAddress, "sche", "task");
        membersMap.put(aMember, Collections.singletonList(scheduledTaskHandler));
        taskHandlers = new LinkedList<Map.Entry<Member, List<ScheduledTaskHandler>>>(membersMap.entrySet());
    }

    public static SimpleEntryView<Data, Data> anEntryView = new SimpleEntryView<Data, Data>(aData, aData);
    public static List<DistributedObjectInfo> distributedObjectInfos = Collections
            .singletonList(new DistributedObjectInfo(aString, aString));
    public static QueryCacheEventData aQueryCacheEventData = new DefaultQueryCacheEventData();
    public static Collection<QueryCacheEventData> queryCacheEventDatas = Collections.singletonList(aQueryCacheEventData);
    public static Collection<CacheEventData> cacheEventDatas = Collections
            .singletonList((CacheEventData) new CacheEventDataImpl(aString, CacheEventType.COMPLETED, aData, aData, aData, true));
    public static Collection<Data> datas = Collections.singletonList(aData);
    public static Collection<Member> members = Collections.singletonList(aMember);
    public static Collection<String> strings = Collections.singletonList(aString);
    public static Collection<Long> longs = Collections.singletonList(aLong);
    public static Collection<UUID> uuids = Collections.singletonList(aUUID);
    public static Xid anXid = new SerializableXID(1, aString.getBytes(), aString.getBytes());
    public static List<Map.Entry<Data, Data>> aListOfEntry = Collections.<Map.Entry<Data, Data>>singletonList(
            new AbstractMap.SimpleEntry<Data, Data>(aData, aData));
    public static Map.Entry<String, byte[]> aStringToByteArrEntry =
            new AbstractMap.SimpleEntry<String, byte[]>(aString, new byte[]{aByte});
    public static List<Map.Entry<String, byte[]>> aListOfStringToByteArrEntry
            = Arrays.asList(aStringToByteArrEntry, aStringToByteArrEntry);

    public static List<Map.Entry<String, List<Map.Entry<Integer, Long>>>> aNamePartitionSequenceList;
    public static long[] arrLongs = new long[]{aLong};
    public static List<Map.Entry<String, Long>> aListOfStringToLong =
            Collections.<Map.Entry<String, Long>>singletonList(new AbstractMap.SimpleEntry<String, Long>("test", 3141592L));
    public static List<Map.Entry<String, String>> aListOfStringToString =
            Collections.<Map.Entry<String, String>>singletonList(new AbstractMap.SimpleEntry<String, String>("test", "testValue"));

    static {
        List<Map.Entry<Integer, Long>> list = Collections.<Map.Entry<Integer, Long>>singletonList(
                new AbstractMap.SimpleEntry<Integer, Long>(anInt, aLong));
        aNamePartitionSequenceList = Collections.<Map.Entry<String, List<Map.Entry<Integer, Long>>>>singletonList(
                new AbstractMap.SimpleEntry<String, List<Map.Entry<Integer, Long>>>(aString, list));
    }

    public static List<Map.Entry<Integer, UUID>> aPartitionUuidList = Collections.<Map.Entry<Integer, UUID>>singletonList(
            new AbstractMap.SimpleEntry<Integer, UUID>(anInt, aUUID));

    public static RingbufferStoreConfigHolder ringbufferStore;
    public static QueueStoreConfigHolder queueStoreConfig;

    public static Map<String, String> props;
    public static List<ListenerConfigHolder> listenerConfigs;

    public static WanReplicationRef wanReplicationRef;
    public static MapStoreConfigHolder mapStoreConfig;

    public static EvictionConfigHolder evictionConfig;
    public static NearCachePreloaderConfig nearCachePreloaderConfig;
    public static NearCacheConfigHolder nearCacheConfig;
    public static List<MapIndexConfig> mapIndexConfigs;
    public static List<MapAttributeConfig> mapAttributeConfigs;
    public static List<QueryCacheConfigHolder> queryCacheConfigs;
    public static TimedExpiryPolicyFactoryConfig timedExpiryPolicyFactoryConfig;
    public static HotRestartConfig hotRestartConfig;
    public static List<CacheSimpleEntryListenerConfig> cacheEntryListenerConfigs;

    static {
        props = new HashMap<>();
        props.put("a", "b");

        BigEndianSerializationServiceBuilder defaultSerializationServiceBuilder = new BigEndianSerializationServiceBuilder();
        SerializationService serializationService = defaultSerializationServiceBuilder
                .setVersion(InternalSerializationService.VERSION_1).build();

        listenerConfigs = new ArrayList<ListenerConfigHolder>();
        ListenerConfigHolder holder1 = new ListenerConfigHolder(TYPE_LISTENER_CONFIG, "listener.By.ClassName");
        //noinspection RedundantCast
        ListenerConfigHolder holder2 = new ListenerConfigHolder(TYPE_CACHE_PARTITION_LOST_LISTENER_CONFIG,
                (Data) serializationService.toData(new TestCachePartitionLostEventListener()), true, false);
        ListenerConfigHolder holder3 = new ListenerConfigHolder(TYPE_ENTRY_LISTENER_CONFIG, "listener.By.ClassName", true, true);
        ListenerConfigHolder holder4 = new ListenerConfigHolder(TYPE_ITEM_LISTENER_CONFIG, "listener.By.ClassName");
        ListenerConfigHolder holder5 = new ListenerConfigHolder(TYPE_MAP_PARTITION_LOST_LISTENER_CONFIG, "listener.By.ClassName");
        ListenerConfigHolder holder6 = new ListenerConfigHolder(TYPE_QUORUM_LISTENER_CONFIG, "listener.By.ClassName");
        listenerConfigs.add(holder1);
        listenerConfigs.add(holder2);
        listenerConfigs.add(holder3);
        listenerConfigs.add(holder4);
        listenerConfigs.add(holder5);
        listenerConfigs.add(holder6);

        ringbufferStore = new RingbufferStoreConfigHolder("com.hazelcast.RingbufferStore", null, null, null,
                props, true);
        queueStoreConfig = new QueueStoreConfigHolder("com.hazelcast.QueueStore", null, null, null, props, true);

        wanReplicationRef = new WanReplicationRef("wan-target", "com.hazelcast.MergePolicy",
                Collections.singletonList("com.hazelcast.WanFilter"), true);

        mapStoreConfig = new MapStoreConfigHolder();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setWriteCoalescing(true);
        mapStoreConfig.setFactoryClassName("com.hazelcast.MapStoreFactory");
        mapStoreConfig.setWriteDelaySeconds(101);
        mapStoreConfig.setWriteBatchSize(20);
        mapStoreConfig.setInitialLoadMode("EAGER");

        EvictionPolicyComparator comparatorImpl = new EvictionPolicyComparator() {
            @Override
            public int compare(EvictableEntryView e1, EvictableEntryView e2) {
                return 0;
            }
        };
        evictionConfig = new EvictionConfigHolder(100, "ENTRY_COUNT", "LRU", "com.hazelcast.ComparatorClassName",
                serializationService.toData(comparatorImpl));
        nearCachePreloaderConfig = new NearCachePreloaderConfig(true, "/root/");
        nearCacheConfig = new NearCacheConfigHolder("nearCache", "BINARY", false, true, 139, 156, evictionConfig,
                false, "INVALIDATE", nearCachePreloaderConfig);

        mapIndexConfigs = new ArrayList<MapIndexConfig>();
        mapIndexConfigs.add(new MapIndexConfig("attr", false));

        mapAttributeConfigs = new ArrayList<MapAttributeConfig>();
        mapAttributeConfigs.add(new MapAttributeConfig("attr", "com.hazelcast.AttributeExtractor"));

        queryCacheConfigs = new ArrayList<QueryCacheConfigHolder>();
        QueryCacheConfigHolder queryCacheConfig = new QueryCacheConfigHolder();
        queryCacheConfig.setPredicateConfigHolder(new PredicateConfigHolder("com.hazelcast.Predicate", "name LIKE 'Fred%'",
                serializationService.toData(Predicates.alwaysTrue())));
        queryCacheConfig.setIndexConfigs(mapIndexConfigs);
        queryCacheConfig.setListenerConfigs(listenerConfigs);
        queryCacheConfig.setEvictionConfigHolder(evictionConfig);
        queryCacheConfig.setInMemoryFormat("BINARY");
        queryCacheConfig.setName("queryCacheName");
        queryCacheConfig.setCoalesce(true);
        queryCacheConfig.setPopulate(true);
        queryCacheConfig.setDelaySeconds(10);
        queryCacheConfig.setBatchSize(15);
        queryCacheConfig.setBufferSize(3000);
        queryCacheConfigs.add(queryCacheConfig);

        timedExpiryPolicyFactoryConfig = new TimedExpiryPolicyFactoryConfig(ExpiryPolicyType.CREATED,
                new DurationConfig(30, TimeUnit.SECONDS));

        hotRestartConfig = new HotRestartConfig();
        hotRestartConfig.setFsync(true);
        hotRestartConfig.setEnabled(true);

        cacheEntryListenerConfigs = new ArrayList<CacheSimpleEntryListenerConfig>();
        CacheSimpleEntryListenerConfig cacheEntryListenerConfig = new CacheSimpleEntryListenerConfig();
        cacheEntryListenerConfig.setCacheEntryEventFilterFactory("com.hazelcast.EntryEventFactory");
        cacheEntryListenerConfig.setCacheEntryListenerFactory("com.hazelcast.EntryListenerFactory");
        cacheEntryListenerConfig.setOldValueRequired(true);
        cacheEntryListenerConfig.setSynchronous(true);
        cacheEntryListenerConfigs.add(cacheEntryListenerConfig);
    }

    public static class TestCachePartitionLostEventListener implements CachePartitionLostListener,
            Serializable {
        @Override
        public void partitionLost(CachePartitionLostEvent event) {

        }
    }

    public static Throwable[] throwables_1_0 = {new CacheException(aString), new CacheLoaderException(
            aString), new CacheWriterException(aString), new EntryProcessorException(aString), new ArrayIndexOutOfBoundsException(
            aString), new ArrayStoreException(aString), new StubAuthenticationException(aString), new CacheNotExistsException(
            aString), new CallerNotMemberException(aString), new CancellationException(aString), new ClassCastException(
            aString), new ClassNotFoundException(aString), new ConcurrentModificationException(
            aString), new ConfigMismatchException(aString), new DistributedObjectDestroyedException(aString), new EOFException(
            aString), new ExecutionException(new IOException()), new HazelcastException(
            aString), new HazelcastInstanceNotActiveException(aString), new HazelcastOverloadException(
            aString), new HazelcastSerializationException(aString), new IOException(aString), new IllegalArgumentException(
            aString), new IllegalAccessException(aString), new IllegalAccessError(aString), new IllegalMonitorStateException(
            aString), new IllegalStateException(aString), new IllegalThreadStateException(aString), new IndexOutOfBoundsException(
            aString), new InterruptedException(aString), new AddressUtil.InvalidAddressException(
            aString), new InvalidConfigurationException(aString), new MemberLeftException(
            aString), new NegativeArraySizeException(aString), new NoSuchElementException(aString), new NotSerializableException(
            aString), new NullPointerException(aString), new OperationTimeoutException(aString), new PartitionMigratingException(
            aString), new QueryException(aString), new QueryResultSizeExceededException(aString), new QuorumException(
            aString), new ReachedMaxSizeException(aString), new RejectedExecutionException(aString), new ResponseAlreadySentException(
            aString), new RetryableHazelcastException(aString), new RetryableIOException(aString), new RuntimeException(
            aString), new SecurityException(aString), new SocketException(aString), new StaleSequenceException(aString,
            1), new TargetDisconnectedException(aString), new TargetNotMemberException(aString), new TimeoutException(
            aString), new TopicOverloadException(aString), new TransactionException(
            aString), new TransactionNotActiveException(aString), new TransactionTimedOutException(
            aString), new URISyntaxException(aString, aString), new UTFDataFormatException(
            aString), new UnsupportedOperationException(aString), new WrongTargetException(aString), new XAException(
            aString), new AccessControlException(aString), new LoginException(aString), new UnsupportedCallbackException(
            new Callback() {
            }), new NoDataMemberInClusterException(aString), new ReplicatedMapCantBeCreatedOnLiteMemberException(
            aString), new MaxMessageSizeExceeded(), new WANReplicationQueueFullException(aString), new AssertionError(
            aString), new OutOfMemoryError(aString), new StackOverflowError(aString), new NativeOutOfMemoryError(aString)};

    public static Throwable[] throwables_1_1 = {new StaleTaskIdException(aString), new ServiceNotFoundException(aString)};

    public static Throwable[] throwables_1_2 = {};

    public static Map<String, Throwable[]> throwables = new HashMap<String, Throwable[]>();

    static {
        throwables.put("1.0", throwables_1_0);
        throwables.put("1.1", throwables_1_1);
        throwables.put("1.2", throwables_1_2);
    }
}
