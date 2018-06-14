/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol;

import com.hazelcast.cache.CacheNotExistsException;
import com.hazelcast.client.impl.StubAuthenticationException;
import com.hazelcast.client.impl.protocol.codec.ErrorCodec;
import com.hazelcast.client.impl.protocol.exception.MaxMessageSizeExceeded;
import com.hazelcast.config.ConfigurationException;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.core.ConsistencyLostException;
import com.hazelcast.core.DuplicateInstanceNameException;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.HazelcastOverloadException;
import com.hazelcast.core.IndeterminateOperationStateException;
import com.hazelcast.core.LocalMemberResetException;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.crdt.MutationDisallowedException;
import com.hazelcast.crdt.TargetNotReplicaException;
import com.hazelcast.durableexecutor.StaleTaskIdException;
import com.hazelcast.flakeidgen.impl.NodeIdOutOfRangeException;
import com.hazelcast.internal.cluster.impl.ConfigMismatchException;
import com.hazelcast.map.QueryResultSizeExceededException;
import com.hazelcast.map.ReachedMaxSizeException;
import com.hazelcast.mapreduce.RemoteMapReduceException;
import com.hazelcast.mapreduce.TopologyChangedException;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.partition.NoDataMemberInClusterException;
import com.hazelcast.query.QueryException;
import com.hazelcast.quorum.QuorumException;
import com.hazelcast.replicatedmap.ReplicatedMapCantBeCreatedOnLiteMemberException;
import com.hazelcast.ringbuffer.StaleSequenceException;
import com.hazelcast.scheduledexecutor.DuplicateTaskException;
import com.hazelcast.scheduledexecutor.StaleTaskException;
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
import com.hazelcast.util.AddressUtil;
import com.hazelcast.wan.WANReplicationQueueFullException;

import javax.cache.CacheException;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import javax.cache.processor.EntryProcessorException;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginException;
import javax.transaction.xa.XAException;
import java.io.EOFException;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.UTFDataFormatException;
import java.net.SocketException;
import java.net.URISyntaxException;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * This class has the error codes and means of
 * 1) creating exception from error code
 * 2) getting the error code of given exception
 */
public class ClientExceptions {

    private static final String CAUSED_BY_STACKTRACE_MARKER = "###### Caused by:";

    private final Map<Class, Integer> classToInt = new HashMap<Class, Integer>();

    public ClientExceptions(boolean jcacheAvailable) {
        if (jcacheAvailable) {
            register(ClientProtocolErrorCodes.CACHE, CacheException.class);
            register(ClientProtocolErrorCodes.CACHE_LOADER, CacheLoaderException.class);
            register(ClientProtocolErrorCodes.CACHE_WRITER, CacheWriterException.class);

            register(ClientProtocolErrorCodes.ENTRY_PROCESSOR, EntryProcessorException.class);
        }

        register(ClientProtocolErrorCodes.ARRAY_INDEX_OUT_OF_BOUNDS, ArrayIndexOutOfBoundsException.class);
        register(ClientProtocolErrorCodes.ARRAY_STORE, ArrayStoreException.class);
        register(ClientProtocolErrorCodes.AUTHENTICATION, StubAuthenticationException.class);
        register(ClientProtocolErrorCodes.CACHE_NOT_EXISTS, CacheNotExistsException.class);
        register(ClientProtocolErrorCodes.CALLER_NOT_MEMBER, CallerNotMemberException.class);
        register(ClientProtocolErrorCodes.CANCELLATION, CancellationException.class);
        register(ClientProtocolErrorCodes.CLASS_CAST, ClassCastException.class);
        register(ClientProtocolErrorCodes.CLASS_NOT_FOUND, ClassNotFoundException.class);
        register(ClientProtocolErrorCodes.CONCURRENT_MODIFICATION, ConcurrentModificationException.class);
        register(ClientProtocolErrorCodes.CONFIG_MISMATCH, ConfigMismatchException.class);
        register(ClientProtocolErrorCodes.CONFIGURATION, ConfigurationException.class);
        register(ClientProtocolErrorCodes.DISTRIBUTED_OBJECT_DESTROYED, DistributedObjectDestroyedException.class);
        register(ClientProtocolErrorCodes.DUPLICATE_INSTANCE_NAME, DuplicateInstanceNameException.class);
        register(ClientProtocolErrorCodes.EOF, EOFException.class);
        register(ClientProtocolErrorCodes.EXECUTION, ExecutionException.class);
        register(ClientProtocolErrorCodes.HAZELCAST, HazelcastException.class);
        register(ClientProtocolErrorCodes.HAZELCAST_INSTANCE_NOT_ACTIVE, HazelcastInstanceNotActiveException.class);
        register(ClientProtocolErrorCodes.HAZELCAST_OVERLOAD, HazelcastOverloadException.class);
        register(ClientProtocolErrorCodes.HAZELCAST_SERIALIZATION, HazelcastSerializationException.class);
        register(ClientProtocolErrorCodes.IO, IOException.class);
        register(ClientProtocolErrorCodes.ILLEGAL_ARGUMENT, IllegalArgumentException.class);
        register(ClientProtocolErrorCodes.ILLEGAL_ACCESS_EXCEPTION, IllegalAccessException.class);
        register(ClientProtocolErrorCodes.ILLEGAL_ACCESS_ERROR, IllegalAccessError.class);
        register(ClientProtocolErrorCodes.ILLEGAL_MONITOR_STATE, IllegalMonitorStateException.class);
        register(ClientProtocolErrorCodes.ILLEGAL_STATE, IllegalStateException.class);
        register(ClientProtocolErrorCodes.ILLEGAL_THREAD_STATE, IllegalThreadStateException.class);
        register(ClientProtocolErrorCodes.INDEX_OUT_OF_BOUNDS, IndexOutOfBoundsException.class);
        register(ClientProtocolErrorCodes.INTERRUPTED, InterruptedException.class);
        register(ClientProtocolErrorCodes.INVALID_ADDRESS, AddressUtil.InvalidAddressException.class);
        register(ClientProtocolErrorCodes.INVALID_CONFIGURATION, InvalidConfigurationException.class);
        register(ClientProtocolErrorCodes.MEMBER_LEFT, MemberLeftException.class);
        register(ClientProtocolErrorCodes.NEGATIVE_ARRAY_SIZE, NegativeArraySizeException.class);
        register(ClientProtocolErrorCodes.NO_SUCH_ELEMENT, NoSuchElementException.class);
        register(ClientProtocolErrorCodes.NOT_SERIALIZABLE, NotSerializableException.class);
        register(ClientProtocolErrorCodes.NULL_POINTER, NullPointerException.class);
        register(ClientProtocolErrorCodes.OPERATION_TIMEOUT, OperationTimeoutException.class);
        register(ClientProtocolErrorCodes.PARTITION_MIGRATING, PartitionMigratingException.class);
        register(ClientProtocolErrorCodes.QUERY, QueryException.class);
        register(ClientProtocolErrorCodes.QUERY_RESULT_SIZE_EXCEEDED, QueryResultSizeExceededException.class);
        register(ClientProtocolErrorCodes.QUORUM, QuorumException.class);
        register(ClientProtocolErrorCodes.REACHED_MAX_SIZE, ReachedMaxSizeException.class);
        register(ClientProtocolErrorCodes.REJECTED_EXECUTION, RejectedExecutionException.class);
        register(ClientProtocolErrorCodes.REMOTE_MAP_REDUCE, RemoteMapReduceException.class);
        register(ClientProtocolErrorCodes.RESPONSE_ALREADY_SENT, ResponseAlreadySentException.class);
        register(ClientProtocolErrorCodes.RETRYABLE_HAZELCAST, RetryableHazelcastException.class);
        register(ClientProtocolErrorCodes.RETRYABLE_IO, RetryableIOException.class);
        register(ClientProtocolErrorCodes.RUNTIME, RuntimeException.class);

        register(ClientProtocolErrorCodes.SECURITY, SecurityException.class);
        register(ClientProtocolErrorCodes.SOCKET, SocketException.class);
        register(ClientProtocolErrorCodes.STALE_SEQUENCE, StaleSequenceException.class);
        register(ClientProtocolErrorCodes.TARGET_DISCONNECTED, TargetDisconnectedException.class);
        register(ClientProtocolErrorCodes.TARGET_NOT_MEMBER, TargetNotMemberException.class);
        register(ClientProtocolErrorCodes.TIMEOUT, TimeoutException.class);
        register(ClientProtocolErrorCodes.TOPIC_OVERLOAD, TopicOverloadException.class);
        register(ClientProtocolErrorCodes.TOPOLOGY_CHANGED, TopologyChangedException.class);
        register(ClientProtocolErrorCodes.TRANSACTION, TransactionException.class);
        register(ClientProtocolErrorCodes.TRANSACTION_NOT_ACTIVE, TransactionNotActiveException.class);
        register(ClientProtocolErrorCodes.TRANSACTION_TIMED_OUT, TransactionTimedOutException.class);
        register(ClientProtocolErrorCodes.URI_SYNTAX, URISyntaxException.class);
        register(ClientProtocolErrorCodes.UTF_DATA_FORMAT, UTFDataFormatException.class);
        register(ClientProtocolErrorCodes.UNSUPPORTED_OPERATION, UnsupportedOperationException.class);
        register(ClientProtocolErrorCodes.WRONG_TARGET, WrongTargetException.class);
        register(ClientProtocolErrorCodes.XA, XAException.class);
        register(ClientProtocolErrorCodes.ACCESS_CONTROL, AccessControlException.class);
        register(ClientProtocolErrorCodes.LOGIN, LoginException.class);
        register(ClientProtocolErrorCodes.UNSUPPORTED_CALLBACK, UnsupportedCallbackException.class);
        register(ClientProtocolErrorCodes.NO_DATA_MEMBER, NoDataMemberInClusterException.class);
        register(ClientProtocolErrorCodes.REPLICATED_MAP_CANT_BE_CREATED, ReplicatedMapCantBeCreatedOnLiteMemberException.class);
        register(ClientProtocolErrorCodes.MAX_MESSAGE_SIZE_EXCEEDED, MaxMessageSizeExceeded.class);
        register(ClientProtocolErrorCodes.WAN_REPLICATION_QUEUE_FULL, WANReplicationQueueFullException.class);

        register(ClientProtocolErrorCodes.ASSERTION_ERROR, AssertionError.class);
        register(ClientProtocolErrorCodes.OUT_OF_MEMORY_ERROR, OutOfMemoryError.class);
        register(ClientProtocolErrorCodes.STACK_OVERFLOW_ERROR, StackOverflowError.class);
        register(ClientProtocolErrorCodes.NATIVE_OUT_OF_MEMORY_ERROR, NativeOutOfMemoryError.class);
        register(ClientProtocolErrorCodes.SERVICE_NOT_FOUND, ServiceNotFoundException.class);
        register(ClientProtocolErrorCodes.STALE_TASK_ID, StaleTaskIdException.class);
        register(ClientProtocolErrorCodes.DUPLICATE_TASK, DuplicateTaskException.class);
        register(ClientProtocolErrorCodes.STALE_TASK, StaleTaskException.class);
        register(ClientProtocolErrorCodes.LOCAL_MEMBER_RESET, LocalMemberResetException.class);
        register(ClientProtocolErrorCodes.INDETERMINATE_OPERATION_STATE, IndeterminateOperationStateException.class);
        register(ClientProtocolErrorCodes.FLAKE_ID_NODE_ID_OUT_OF_RANGE_EXCEPTION, NodeIdOutOfRangeException.class);
        register(ClientProtocolErrorCodes.TARGET_NOT_REPLICA_EXCEPTION, TargetNotReplicaException.class);
        register(ClientProtocolErrorCodes.MUTATION_DISALLOWED_EXCEPTION, MutationDisallowedException.class);
        register(ClientProtocolErrorCodes.CONSISTENCY_LOST_EXCEPTION, ConsistencyLostException.class);
    }



    public ClientMessage createExceptionMessage(Throwable throwable) {
        int errorCode = getErrorCode(throwable);
        String message = throwable.getMessage();

        // Combine the stack traces of causes recursively into one long stack trace.
        List<StackTraceElement> combinedStackTrace = new ArrayList<StackTraceElement>();
        Throwable t = throwable;
        while (t != null) {
            combinedStackTrace.addAll(Arrays.asList(t.getStackTrace()));
            t = t.getCause();
            // add separator, if there is one more cause
            if (t != null) {
                // don't rely on Throwable.toString(), which contains the same logic, but rather use our own, as it might be overridden
                String throwableToString = t.getClass().getName() + (t.getLocalizedMessage() != null ? ": " + t.getLocalizedMessage() : "");

                combinedStackTrace.add(new StackTraceElement(CAUSED_BY_STACKTRACE_MARKER
                        + " (" + getErrorCode(t) + ") " + throwableToString
                        + " ------", "", null, -1));
            }
        }

        final int causeErrorCode;
        final String causeClassName;

        Throwable cause = throwable.getCause();
        if (cause != null) {
            causeErrorCode = getErrorCode(cause);
            causeClassName = cause.getClass().getName();
        } else {
            causeErrorCode = ClientProtocolErrorCodes.UNDEFINED;
            causeClassName = null;
        }

        StackTraceElement[] combinedStackTraceArray = combinedStackTrace.toArray(new StackTraceElement[combinedStackTrace.size()]);
        return ErrorCodec.encode(errorCode, throwable.getClass().getName(), message, combinedStackTraceArray,
                causeErrorCode, causeClassName);
    }


    public void register(int errorCode, Class clazz) {
        Integer currentCode = classToInt.get(clazz);

        if (currentCode != null) {
            throw new HazelcastException("Class " + clazz.getName() + " already added with code: " + currentCode);
        }

        classToInt.put(clazz, errorCode);
    }

    private int getErrorCode(Throwable e) {
        Integer errorCode = classToInt.get(e.getClass());
        if (errorCode == null) {
            return ClientProtocolErrorCodes.UNDEFINED;
        }
        return errorCode;
    }

    // package-access for test
    boolean isKnownClass(Class<? extends Throwable> aClass) {
        return classToInt.containsKey(aClass);
    }


}
