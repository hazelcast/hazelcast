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

package com.hazelcast.client.impl.protocol;

import com.hazelcast.cache.CacheNotExistsException;
import com.hazelcast.client.AuthenticationException;
import com.hazelcast.client.UndefinedErrorCodeException;
import com.hazelcast.client.impl.protocol.codec.builtin.ErrorsCodec;
import com.hazelcast.client.impl.protocol.exception.ErrorHolder;
import com.hazelcast.client.impl.protocol.exception.MaxMessageSizeExceeded;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.core.ConsistencyLostException;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.HazelcastOverloadException;
import com.hazelcast.core.IndeterminateOperationStateException;
import com.hazelcast.core.LocalMemberResetException;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.cp.exception.CPGroupDestroyedException;
import com.hazelcast.cp.exception.CannotReplicateException;
import com.hazelcast.cp.exception.LeaderDemotedException;
import com.hazelcast.cp.exception.NotLeaderException;
import com.hazelcast.cp.exception.StaleAppendRequestException;
import com.hazelcast.cp.internal.datastructures.exception.WaitKeyCancelledException;
import com.hazelcast.cp.internal.session.SessionExpiredException;
import com.hazelcast.cp.lock.exception.LockAcquireLimitReachedException;
import com.hazelcast.cp.lock.exception.LockOwnershipLostException;
import com.hazelcast.crdt.MutationDisallowedException;
import com.hazelcast.crdt.TargetNotReplicaException;
import com.hazelcast.durableexecutor.StaleTaskIdException;
import com.hazelcast.flakeidgen.impl.NodeIdOutOfRangeException;
import com.hazelcast.internal.cluster.impl.ConfigMismatchException;
import com.hazelcast.internal.cluster.impl.VersionMismatchException;
import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.internal.util.AddressUtil;
import com.hazelcast.internal.util.EmptyStatement;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.map.QueryResultSizeExceededException;
import com.hazelcast.map.ReachedMaxSizeException;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.partition.NoDataMemberInClusterException;
import com.hazelcast.query.QueryException;
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
import com.hazelcast.splitbrainprotection.SplitBrainProtectionException;
import com.hazelcast.topic.TopicOverloadException;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionNotActiveException;
import com.hazelcast.transaction.TransactionTimedOutException;
import com.hazelcast.wan.WanQueueFullException;

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
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.ACCESS_CONTROL;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.ARRAY_INDEX_OUT_OF_BOUNDS;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.ARRAY_STORE;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.ASSERTION_ERROR;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.AUTHENTICATION;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.CACHE;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.CACHE_LOADER;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.CACHE_NOT_EXISTS;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.CACHE_WRITER;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.CALLER_NOT_MEMBER;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.CANCELLATION;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.CANNOT_REPLICATE_EXCEPTION;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.CLASS_CAST;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.CLASS_NOT_FOUND;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.CONCURRENT_MODIFICATION;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.CONFIG_MISMATCH;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.CONSISTENCY_LOST_EXCEPTION;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.CP_GROUP_DESTROYED_EXCEPTION;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.DISTRIBUTED_OBJECT_DESTROYED;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.DUPLICATE_TASK;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.ENTRY_PROCESSOR;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.EOF;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.EXECUTION;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.FLAKE_ID_NODE_ID_OUT_OF_RANGE_EXCEPTION;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.HAZELCAST;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.HAZELCAST_INSTANCE_NOT_ACTIVE;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.HAZELCAST_OVERLOAD;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.HAZELCAST_SERIALIZATION;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.ILLEGAL_ACCESS_ERROR;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.ILLEGAL_ACCESS_EXCEPTION;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.ILLEGAL_ARGUMENT;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.ILLEGAL_MONITOR_STATE;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.ILLEGAL_STATE;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.ILLEGAL_THREAD_STATE;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.INDETERMINATE_OPERATION_STATE;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.INDEX_OUT_OF_BOUNDS;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.INTERRUPTED;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.INVALID_ADDRESS;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.INVALID_CONFIGURATION;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.IO;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.LEADER_DEMOTED_EXCEPTION;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.LOCAL_MEMBER_RESET;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.LOCK_ACQUIRE_LIMIT_REACHED_EXCEPTION;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.LOCK_OWNERSHIP_LOST_EXCEPTION;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.LOGIN;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.MAX_MESSAGE_SIZE_EXCEEDED;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.MEMBER_LEFT;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.MUTATION_DISALLOWED_EXCEPTION;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.NATIVE_OUT_OF_MEMORY_ERROR;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.NEGATIVE_ARRAY_SIZE;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.NOT_LEADER_EXCEPTION;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.NOT_SERIALIZABLE;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.NO_CLASS_DEF_FOUND_ERROR;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.NO_DATA_MEMBER;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.NO_SUCH_ELEMENT;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.NO_SUCH_FIELD_ERROR;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.NO_SUCH_FIELD_EXCEPTION;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.NO_SUCH_METHOD_ERROR;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.NO_SUCH_METHOD_EXCEPTION;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.NULL_POINTER;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.OPERATION_TIMEOUT;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.OUT_OF_MEMORY_ERROR;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.PARTITION_MIGRATING;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.QUERY;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.QUERY_RESULT_SIZE_EXCEEDED;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.REACHED_MAX_SIZE;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.REJECTED_EXECUTION;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.REPLICATED_MAP_CANT_BE_CREATED;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.RESPONSE_ALREADY_SENT;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.RETRYABLE_HAZELCAST;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.RETRYABLE_IO;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.RUNTIME;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.SECURITY;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.SERVICE_NOT_FOUND;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.SESSION_EXPIRED_EXCEPTION;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.SOCKET;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.SPLIT_BRAIN_PROTECTION;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.STACK_OVERFLOW_ERROR;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.STALE_APPEND_REQUEST_EXCEPTION;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.STALE_SEQUENCE;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.STALE_TASK;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.STALE_TASK_ID;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.TARGET_DISCONNECTED;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.TARGET_NOT_MEMBER;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.TARGET_NOT_REPLICA_EXCEPTION;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.TIMEOUT;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.TOPIC_OVERLOAD;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.TRANSACTION;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.TRANSACTION_NOT_ACTIVE;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.TRANSACTION_TIMED_OUT;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.UNSUPPORTED_CALLBACK;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.UNSUPPORTED_OPERATION;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.URI_SYNTAX;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.UTF_DATA_FORMAT;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.VERSION_MISMATCH_EXCEPTION;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.WAIT_KEY_CANCELLED_EXCEPTION;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.WAN_REPLICATION_QUEUE_FULL;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.WRONG_TARGET;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.XA;

/**
 * This class has the error codes and means of
 * 1) creating exception from error code
 * 2) getting the error code of given exception
 */
public class ClientExceptionFactory {

    public interface ExceptionFactory {
        Throwable createException(String message, Throwable cause);
    }

    private final Map<Integer, ExceptionFactory> intToFactory = new HashMap<>();
    private final Map<Class, Integer> classToInt = new HashMap<>();
    private final ClassLoader classLoader;

    public ClientExceptionFactory(boolean jcacheAvailable, ClassLoader classLoader) {
        this.classLoader = classLoader;
        if (jcacheAvailable) {
            register(CACHE, CacheException.class, CacheException::new);
            register(CACHE_LOADER, CacheLoaderException.class, CacheLoaderException::new);
            register(CACHE_WRITER, CacheWriterException.class, CacheWriterException::new);
            register(ENTRY_PROCESSOR, EntryProcessorException.class, EntryProcessorException::new);
        }

        register(ARRAY_INDEX_OUT_OF_BOUNDS, ArrayIndexOutOfBoundsException.class, (message, cause) -> new ArrayIndexOutOfBoundsException(message));
        register(ARRAY_STORE, ArrayStoreException.class, (message, cause) -> new ArrayStoreException(message));
        register(AUTHENTICATION, AuthenticationException.class, (message, cause) -> new AuthenticationException(message));
        register(CACHE_NOT_EXISTS, CacheNotExistsException.class, (message, cause) -> new CacheNotExistsException(message));
        register(CALLER_NOT_MEMBER, CallerNotMemberException.class, (message, cause) -> new CallerNotMemberException(message));
        register(CANCELLATION, CancellationException.class, (message, cause) -> new CancellationException(message));
        register(CLASS_CAST, ClassCastException.class, (message, cause) -> new ClassCastException(message));
        register(CLASS_NOT_FOUND, ClassNotFoundException.class, ClassNotFoundException::new);
        register(CONCURRENT_MODIFICATION, ConcurrentModificationException.class, (message, cause) -> new ConcurrentModificationException(message));
        register(CONFIG_MISMATCH, ConfigMismatchException.class, (message, cause) -> new ConfigMismatchException(message));
        register(DISTRIBUTED_OBJECT_DESTROYED, DistributedObjectDestroyedException.class, (message, cause) -> new DistributedObjectDestroyedException(message));
        register(EOF, EOFException.class, (message, cause) -> new EOFException(message));
        register(EXECUTION, ExecutionException.class, ExecutionException::new);
        register(HAZELCAST, HazelcastException.class, HazelcastException::new);
        register(HAZELCAST_INSTANCE_NOT_ACTIVE, HazelcastInstanceNotActiveException.class, (message, cause) -> new HazelcastInstanceNotActiveException(message));
        register(HAZELCAST_OVERLOAD, HazelcastOverloadException.class, (message, cause) -> new HazelcastOverloadException(message));
        register(HAZELCAST_SERIALIZATION, HazelcastSerializationException.class, HazelcastSerializationException::new);
        register(IO, IOException.class, IOException::new);
        register(ILLEGAL_ARGUMENT, IllegalArgumentException.class, IllegalArgumentException::new);
        register(ILLEGAL_ACCESS_EXCEPTION, IllegalAccessException.class, (message, cause) -> new IllegalAccessException(message));
        register(ILLEGAL_ACCESS_ERROR, IllegalAccessError.class, (message, cause) -> new IllegalAccessError(message));
        register(ILLEGAL_MONITOR_STATE, IllegalMonitorStateException.class, (message, cause) -> new IllegalMonitorStateException(message));
        register(ILLEGAL_STATE, IllegalStateException.class, IllegalStateException::new);
        register(ILLEGAL_THREAD_STATE, IllegalThreadStateException.class, (message, cause) -> new IllegalThreadStateException(message));
        register(INDEX_OUT_OF_BOUNDS, IndexOutOfBoundsException.class, (message, cause) -> new IndexOutOfBoundsException(message));
        register(INTERRUPTED, InterruptedException.class, (message, cause) -> new InterruptedException(message));
        register(INVALID_ADDRESS, AddressUtil.InvalidAddressException.class, (message, cause) -> new AddressUtil.InvalidAddressException(message, false));
        register(INVALID_CONFIGURATION, InvalidConfigurationException.class, InvalidConfigurationException::new);
        register(MEMBER_LEFT, MemberLeftException.class, (message, cause) -> new MemberLeftException(message));
        register(NEGATIVE_ARRAY_SIZE, NegativeArraySizeException.class, (message, cause) -> new NegativeArraySizeException(message));
        register(NO_SUCH_ELEMENT, NoSuchElementException.class, (message, cause) -> new NoSuchElementException(message));
        register(NOT_SERIALIZABLE, NotSerializableException.class, (message, cause) -> new NotSerializableException(message));
        register(NULL_POINTER, NullPointerException.class, (message, cause) -> new NullPointerException(message));
        register(OPERATION_TIMEOUT, OperationTimeoutException.class, (message, cause) -> new OperationTimeoutException(message));
        register(PARTITION_MIGRATING, PartitionMigratingException.class, (message, cause) -> new PartitionMigratingException(message));
        register(QUERY, QueryException.class, QueryException::new);
        register(QUERY_RESULT_SIZE_EXCEEDED, QueryResultSizeExceededException.class, (message, cause) -> new QueryResultSizeExceededException(message));
        register(SPLIT_BRAIN_PROTECTION, SplitBrainProtectionException.class, (message, cause) -> new SplitBrainProtectionException(message));
        register(REACHED_MAX_SIZE, ReachedMaxSizeException.class, (message, cause) -> new ReachedMaxSizeException(message));
        register(REJECTED_EXECUTION, RejectedExecutionException.class, RejectedExecutionException::new);
        register(RESPONSE_ALREADY_SENT, ResponseAlreadySentException.class, (message, cause) -> new ResponseAlreadySentException(message));
        register(RETRYABLE_HAZELCAST, RetryableHazelcastException.class, RetryableHazelcastException::new);
        register(RETRYABLE_IO, RetryableIOException.class, RetryableIOException::new);
        register(RUNTIME, RuntimeException.class, RuntimeException::new);
        register(SECURITY, SecurityException.class, SecurityException::new);
        register(SOCKET, SocketException.class, (message, cause) -> new SocketException(message));
        register(STALE_SEQUENCE, StaleSequenceException.class, (message, cause) -> new StaleSequenceException(message, 0));
        register(TARGET_DISCONNECTED, TargetDisconnectedException.class, (message, cause) -> new TargetDisconnectedException(message));
        register(TARGET_NOT_MEMBER, TargetNotMemberException.class, (message, cause) -> new TargetNotMemberException(message));
        register(TIMEOUT, TimeoutException.class, (message, cause) -> new TimeoutException(message));
        register(TOPIC_OVERLOAD, TopicOverloadException.class, (message, cause) -> new TopicOverloadException(message));
        register(TRANSACTION, TransactionException.class, TransactionException::new);
        register(TRANSACTION_NOT_ACTIVE, TransactionNotActiveException.class, (message, cause) -> new TransactionNotActiveException(message));
        register(TRANSACTION_TIMED_OUT, TransactionTimedOutException.class, TransactionTimedOutException::new);
        register(URI_SYNTAX, URISyntaxException.class, (message, cause) -> new URISyntaxException("not available", message));
        register(UTF_DATA_FORMAT, UTFDataFormatException.class, (message, cause) -> new UTFDataFormatException(message));
        register(UNSUPPORTED_OPERATION, UnsupportedOperationException.class, UnsupportedOperationException::new);
        register(WRONG_TARGET, WrongTargetException.class, (message, cause) -> new WrongTargetException(message));
        register(XA, XAException.class, (message, cause) -> new XAException(message));
        register(ACCESS_CONTROL, AccessControlException.class, (message, cause) -> new AccessControlException(message));
        register(LOGIN, LoginException.class, (message, cause) -> new LoginException(message));
        register(UNSUPPORTED_CALLBACK, UnsupportedCallbackException.class, (message, cause) -> new UnsupportedCallbackException(null, message));
        register(NO_DATA_MEMBER, NoDataMemberInClusterException.class, (message, cause) -> new NoDataMemberInClusterException(message));
        register(REPLICATED_MAP_CANT_BE_CREATED, ReplicatedMapCantBeCreatedOnLiteMemberException.class, (message, cause) -> new ReplicatedMapCantBeCreatedOnLiteMemberException(message));
        register(MAX_MESSAGE_SIZE_EXCEEDED, MaxMessageSizeExceeded.class, (message, cause) -> new MaxMessageSizeExceeded(message));
        register(WAN_REPLICATION_QUEUE_FULL, WanQueueFullException.class, (message, cause) -> new WanQueueFullException(message));
        register(ASSERTION_ERROR, AssertionError.class, (message, cause) -> new AssertionError(message));
        register(OUT_OF_MEMORY_ERROR, OutOfMemoryError.class, (message, cause) -> new OutOfMemoryError(message));
        register(STACK_OVERFLOW_ERROR, StackOverflowError.class, (message, cause) -> new StackOverflowError(message));
        register(NATIVE_OUT_OF_MEMORY_ERROR, NativeOutOfMemoryError.class, NativeOutOfMemoryError::new);
        register(SERVICE_NOT_FOUND, ServiceNotFoundException.class, (message, cause) -> new ServiceNotFoundException(message));
        register(STALE_TASK_ID, StaleTaskIdException.class, (message, cause) -> new StaleTaskIdException(message));
        register(DUPLICATE_TASK, DuplicateTaskException.class, (message, cause) -> new DuplicateTaskException(message));
        register(STALE_TASK, StaleTaskException.class, (message, cause) -> new StaleTaskException(message));
        register(LOCAL_MEMBER_RESET, LocalMemberResetException.class, (message, cause) -> new LocalMemberResetException(message));
        register(INDETERMINATE_OPERATION_STATE, IndeterminateOperationStateException.class, IndeterminateOperationStateException::new);
        register(FLAKE_ID_NODE_ID_OUT_OF_RANGE_EXCEPTION, NodeIdOutOfRangeException.class, (message, cause) -> new NodeIdOutOfRangeException(message));
        register(TARGET_NOT_REPLICA_EXCEPTION, TargetNotReplicaException.class, (message, cause) -> new TargetNotReplicaException(message));
        register(MUTATION_DISALLOWED_EXCEPTION, MutationDisallowedException.class, (message, cause) -> new MutationDisallowedException(message));
        register(CONSISTENCY_LOST_EXCEPTION, ConsistencyLostException.class, (message, cause) -> new ConsistencyLostException(message));
        register(SESSION_EXPIRED_EXCEPTION, SessionExpiredException.class, SessionExpiredException::new);
        register(WAIT_KEY_CANCELLED_EXCEPTION, WaitKeyCancelledException.class, WaitKeyCancelledException::new);
        register(LOCK_ACQUIRE_LIMIT_REACHED_EXCEPTION, LockAcquireLimitReachedException.class, (message, cause) -> new LockAcquireLimitReachedException(message));
        register(LOCK_OWNERSHIP_LOST_EXCEPTION, LockOwnershipLostException.class, (message, cause) -> new LockOwnershipLostException(message));
        register(CP_GROUP_DESTROYED_EXCEPTION, CPGroupDestroyedException.class, (message, cause) -> new CPGroupDestroyedException());
        register(CANNOT_REPLICATE_EXCEPTION, CannotReplicateException.class, (message, cause) -> new CannotReplicateException(null));
        register(LEADER_DEMOTED_EXCEPTION, LeaderDemotedException.class, (message, cause) -> new LeaderDemotedException(null, null));
        register(STALE_APPEND_REQUEST_EXCEPTION, StaleAppendRequestException.class, (message, cause) -> new StaleAppendRequestException(null));
        register(NOT_LEADER_EXCEPTION, NotLeaderException.class, (message, cause) -> new NotLeaderException(null, null, null));
        register(VERSION_MISMATCH_EXCEPTION, VersionMismatchException.class, ((message, cause) -> new VersionMismatchException(message)));
        register(NO_SUCH_METHOD_ERROR, NoSuchMethodError.class, ((message, cause) -> new NoSuchMethodError(message)));
        register(NO_SUCH_METHOD_EXCEPTION, NoSuchMethodException.class, ((message, cause) -> new NoSuchMethodException(message)));
        register(NO_SUCH_FIELD_ERROR, NoSuchFieldError.class, ((message, cause) -> new NoSuchFieldError(message)));
        register(NO_SUCH_FIELD_EXCEPTION, NoSuchFieldException.class, ((message, cause) -> new NoSuchFieldException(message)));
        register(NO_CLASS_DEF_FOUND_ERROR, NoClassDefFoundError.class, ((message, cause) -> new NoClassDefFoundError(message)));
    }

    public Throwable createException(ClientMessage clientMessage) {
        List<ErrorHolder> errorHolders = ErrorsCodec.decode(clientMessage);
        return createException(errorHolders.iterator());
    }

    private Throwable createException(Iterator<ErrorHolder> iterator) {
        if (!iterator.hasNext()) {
            return null;
        }
        ErrorHolder errorHolder = iterator.next();
        ExceptionFactory exceptionFactory = intToFactory.get(errorHolder.getErrorCode());
        Throwable throwable = null;
        if (exceptionFactory == null) {
            String className = errorHolder.getClassName();
            try {
                Class<? extends Throwable> exceptionClass =
                        (Class<? extends Throwable>) ClassLoaderUtil.loadClass(classLoader, className);
                throwable = ExceptionUtil.tryCreateExceptionWithMessageAndCause(exceptionClass, errorHolder.getMessage(),
                        createException(iterator));
            } catch (ClassNotFoundException e) {
                EmptyStatement.ignore(e);
            }
            if (throwable == null) {
                throwable = new UndefinedErrorCodeException(errorHolder.getMessage(), className, createException(iterator));
            }
        } else {
            throwable = exceptionFactory.createException(errorHolder.getMessage(), createException(iterator));
        }
        throwable.setStackTrace(errorHolder.getStackTraceElements().toArray(new StackTraceElement[0]));
        return throwable;
    }

    /**
     * hazelcast and jdk exceptions should always be defined
     * in {@link com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes} and
     * in {@link ClientExceptionFactory}
     * so that a well defined error code could be delivered to non-java clients.
     * So we don't try to load them via ClassLoader to be able to catch the missing exceptions
     */
    private boolean checkClassNameForValidity(String exceptionClassName) {
        return !exceptionClassName.startsWith("com.hazelcast") && !exceptionClassName.startsWith("java");
    }

    // method is used by Jet
    @SuppressWarnings("WeakerAccess")
    public void register(int errorCode, Class clazz, ExceptionFactory exceptionFactory) {
        if (intToFactory.putIfAbsent(errorCode, exceptionFactory) != null) {
            throw new HazelcastException("Code " + errorCode + " already used");
        }

        if (!clazz.equals(exceptionFactory.createException("", null).getClass())) {
            throw new HazelcastException("Exception factory did not produce an instance of expected class");
        }

        Integer currentCode = classToInt.putIfAbsent(clazz, errorCode);
        if (currentCode != null) {
            throw new HazelcastException("Class " + clazz.getName() + " already added with code: " + currentCode);
        }
    }

    public ClientMessage createExceptionMessage(Throwable throwable) {
        List<ErrorHolder> errorHolders = new LinkedList<>();
        errorHolders.add(convertToErrorHolder(throwable));
        Throwable cause = throwable.getCause();
        while (cause != null) {
            errorHolders.add(convertToErrorHolder(cause));
            cause = cause.getCause();
        }

        return ErrorsCodec.encode(errorHolders);
    }

    private ErrorHolder convertToErrorHolder(Throwable t) {
        Integer errorCode = classToInt.get(t.getClass());
        if (errorCode == null) {
            errorCode = ClientProtocolErrorCodes.UNDEFINED;
        }
        return new ErrorHolder(errorCode, t.getClass().getName(), t.getMessage(), Arrays.asList(t.getStackTrace()));
    }

    // package-access for test
    boolean isKnownClass(Class<? extends Throwable> aClass) {
        return classToInt.containsKey(aClass);
    }
}
