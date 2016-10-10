/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cache.CacheNotExistsException;
import com.hazelcast.cache.impl.CacheEventData;
import com.hazelcast.cache.impl.CacheEventDataImpl;
import com.hazelcast.cache.impl.CacheEventType;
import com.hazelcast.client.AuthenticationException;
import com.hazelcast.client.impl.MemberImpl;
import com.hazelcast.client.impl.client.DistributedObjectInfo;
import com.hazelcast.client.impl.protocol.exception.MaxMessageSizeExceeded;
import com.hazelcast.config.ConfigurationException;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.core.DuplicateInstanceNameException;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.HazelcastOverloadException;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.durableexecutor.StaleTaskIdException;
import com.hazelcast.internal.cluster.impl.ConfigMismatchException;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.map.QueryResultSizeExceededException;
import com.hazelcast.map.ReachedMaxSizeException;
import com.hazelcast.map.impl.SimpleEntryView;
import com.hazelcast.map.impl.querycache.event.DefaultQueryCacheEventData;
import com.hazelcast.map.impl.querycache.event.QueryCacheEventData;
import com.hazelcast.mapreduce.JobPartitionState;
import com.hazelcast.mapreduce.RemoteMapReduceException;
import com.hazelcast.mapreduce.TopologyChangedException;
import com.hazelcast.mapreduce.impl.task.JobPartitionStateImpl;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.partition.NoDataMemberInClusterException;
import com.hazelcast.query.QueryException;
import com.hazelcast.quorum.QuorumException;
import com.hazelcast.replicatedmap.ReplicatedMapCantBeCreatedOnLiteMemberException;
import com.hazelcast.ringbuffer.StaleSequenceException;
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
import com.hazelcast.topic.TopicOverloadException;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionNotActiveException;
import com.hazelcast.transaction.TransactionTimedOutException;
import com.hazelcast.transaction.impl.xa.SerializableXID;
import com.hazelcast.util.AddressUtil;
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
import java.io.UTFDataFormatException;
import java.lang.reflect.Array;
import java.net.SocketException;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.security.AccessControlException;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;

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
        return a.equals(b);
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

    public static boolean aBoolean = true;
    public static byte aByte = 113;
    public static int anInt = 56789;
    public static long aLong = -50992225L;
    public static String aString = "SampleString";
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

    public static Member aMember = new MemberImpl(anAddress, aString, Collections.singletonMap(aString, (Object) aString), false);
    public static Collection<Map.Entry<Address, List<Integer>>> aPartitionTable;

    static {
        Map<Address, List<Integer>> partitionsMap = new HashMap<Address, List<Integer>>();
        partitionsMap.put(anAddress, Collections.singletonList(1));
        aPartitionTable = new LinkedList<Map.Entry<Address, List<Integer>>>(partitionsMap.entrySet());
    }

    public static SimpleEntryView<Data, Data> anEntryView = new SimpleEntryView<Data, Data>(aData, aData);
    public static Collection<JobPartitionState> jobPartitionStates = Collections
            .singletonList((JobPartitionState) new JobPartitionStateImpl(anAddress, JobPartitionState.State.MAPPING));
    public static List<DistributedObjectInfo> distributedObjectInfos = Collections
            .singletonList(new DistributedObjectInfo(aString, aString));
    public static QueryCacheEventData aQueryCacheEventData = new DefaultQueryCacheEventData();
    public static Collection<QueryCacheEventData> queryCacheEventDatas = Collections.singletonList(aQueryCacheEventData);
    public static Collection<CacheEventData> cacheEventDatas = Collections
            .singletonList((CacheEventData) new CacheEventDataImpl(aString, CacheEventType.COMPLETED, aData, aData, aData, true));
    public static Collection<Data> datas = Collections.singletonList(aData);
    public static Collection<Member> members = Collections.singletonList(aMember);
    public static Collection<String> strings = Collections.singletonList(aString);
    public static Xid anXid = new SerializableXID(1, aString.getBytes(), aString.getBytes());
    public static List<Map.Entry<Data, Data>> aListOfEntry = Collections.<Map.Entry<Data, Data>>singletonList(
            new AbstractMap.SimpleEntry<Data, Data>(aData, aData));

    public static Throwable[] throwables_1_0 = {new CacheException(aString),
            new CacheLoaderException(aString),
            new CacheWriterException(aString),
            new EntryProcessorException(aString),
            new ArrayIndexOutOfBoundsException(aString),
            new ArrayStoreException(aString),
            new AuthenticationException(aString),
            new CacheNotExistsException(aString),
            new CallerNotMemberException(aString),
            new CancellationException(aString),
            new ClassCastException(aString),
            new ClassNotFoundException(aString),
            new ConcurrentModificationException(aString),
            new ConfigMismatchException(aString),
            new ConfigurationException(aString),
            new DistributedObjectDestroyedException(aString),
            new DuplicateInstanceNameException(aString),
            new EOFException(aString),
            new ExecutionException(new IOException()),
            new HazelcastException(aString),
            new HazelcastInstanceNotActiveException(aString),
            new HazelcastOverloadException(aString),
            new HazelcastSerializationException(aString),
            new IOException(aString),
            new IllegalArgumentException(aString),
            new IllegalAccessException(aString),
            new IllegalAccessError(aString),
            new IllegalMonitorStateException(aString),
            new IllegalStateException(aString),
            new IllegalThreadStateException(aString),
            new IndexOutOfBoundsException(aString),
            new InterruptedException(aString),
            new AddressUtil.InvalidAddressException(aString),
            new InvalidConfigurationException(aString),
            new MemberLeftException(aString),
            new NegativeArraySizeException(aString),
            new NoSuchElementException(aString),
            new NotSerializableException(aString),
            new NullPointerException(aString),
            new OperationTimeoutException(aString),
            new PartitionMigratingException(aString),
            new QueryException(aString),
            new QueryResultSizeExceededException(aString),
            new QuorumException(aString),
            new ReachedMaxSizeException(aString),
            new RejectedExecutionException(aString),
            new RemoteMapReduceException(aString, Collections.<Exception>emptyList()),
            new ResponseAlreadySentException(aString),
            new RetryableHazelcastException(aString),
            new RetryableIOException(aString),
            new RuntimeException(aString),
            new SecurityException(aString),
            new SocketException(aString),
            new StaleSequenceException(aString, 1),
            new TargetDisconnectedException(aString),
            new TargetNotMemberException(aString),
            new TimeoutException(aString),
            new TopicOverloadException(aString),
            new TopologyChangedException(aString),
            new TransactionException(aString),
            new TransactionNotActiveException(aString),
            new TransactionTimedOutException(aString),
            new URISyntaxException(aString, aString),
            new UTFDataFormatException(aString),
            new UnsupportedOperationException(aString),
            new WrongTargetException(aString),
            new XAException(aString),
            new AccessControlException(aString),
            new LoginException(aString),
            new UnsupportedCallbackException(new Callback() {
            }),
            new NoDataMemberInClusterException(aString),
            new ReplicatedMapCantBeCreatedOnLiteMemberException(aString),
            new MaxMessageSizeExceeded(),
            new WANReplicationQueueFullException(aString),
            new AssertionError(aString),
            new OutOfMemoryError(aString),
            new StackOverflowError(aString),
            new NativeOutOfMemoryError(aString)};

    public static Throwable[] throwables_1_1 = {
            new StaleTaskIdException(aString),
            new ServiceNotFoundException(aString)
    };

    public static Throwable[] throwables_1_2 = {};

    public static Map<String, Throwable[]> throwables = new HashMap<String, Throwable[]>();

    static {
        throwables.put("1.0", throwables_1_0);
        throwables.put("1.1", throwables_1_1);
        throwables.put("1.2", throwables_1_2);
    }
}
