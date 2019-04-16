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

package com.hazelcast.client.impl.clientside;

import com.hazelcast.cache.CacheNotExistsException;
import com.hazelcast.client.AuthenticationException;
import com.hazelcast.client.UndefinedErrorCodeException;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes;
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
import com.hazelcast.map.QueryResultSizeExceededException;
import com.hazelcast.map.ReachedMaxSizeException;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.partition.NoDataMemberInClusterException;
import com.hazelcast.query.QueryException;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionException;
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
import com.hazelcast.internal.util.AddressUtil;
import com.hazelcast.wan.WanReplicationQueueFullException;

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
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.CANNOT_REPLICATE_EXCEPTION;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.CP_GROUP_DESTROYED_EXCEPTION;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.LEADER_DEMOTED_EXCEPTION;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.LOCK_ACQUIRE_LIMIT_REACHED_EXCEPTION;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.LOCK_OWNERSHIP_LOST_EXCEPTION;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.NOT_LEADER_EXCEPTION;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.SESSION_EXPIRED_EXCEPTION;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.STALE_APPEND_REQUEST_EXCEPTION;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.WAIT_KEY_CANCELLED_EXCEPTION;

/**
 * This class has the error codes and means of
 * 1) creating exception from error code
 * 2) getting the error code of given exception
 */
public class ClientExceptionFactory {

    private final Map<Integer, ExceptionFactory> intToFactory = new HashMap<Integer, ExceptionFactory>();

    public ClientExceptionFactory(boolean jcacheAvailable) {
        if (jcacheAvailable) {
            register(ClientProtocolErrorCodes.CACHE, CacheException.class, new ExceptionFactory() {
                @Override
                public Throwable createException(String message, Throwable cause) {
                    return new CacheException(message, cause);
                }
            });
            register(ClientProtocolErrorCodes.CACHE_LOADER, CacheLoaderException.class, new ExceptionFactory() {
                @Override
                public Throwable createException(String message, Throwable cause) {
                    return new CacheLoaderException(message, cause);
                }
            });
            register(ClientProtocolErrorCodes.CACHE_WRITER, CacheWriterException.class, new ExceptionFactory() {
                @Override
                public Throwable createException(String message, Throwable cause) {
                    return new CacheWriterException(message, cause);
                }
            });

            register(ClientProtocolErrorCodes.ENTRY_PROCESSOR, EntryProcessorException.class, new ExceptionFactory() {
                @Override
                public Throwable createException(String message, Throwable cause) {
                    return new EntryProcessorException(message, cause);
                }
            });
        }

        register(ClientProtocolErrorCodes.ARRAY_INDEX_OUT_OF_BOUNDS, ArrayIndexOutOfBoundsException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new ArrayIndexOutOfBoundsException(message);
            }
        });
        register(ClientProtocolErrorCodes.ARRAY_STORE, ArrayStoreException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new ArrayStoreException(message);
            }
        });
        register(ClientProtocolErrorCodes.AUTHENTICATION, AuthenticationException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new AuthenticationException(message);
            }
        });
        register(ClientProtocolErrorCodes.CACHE_NOT_EXISTS, CacheNotExistsException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new CacheNotExistsException(message);
            }
        });
        register(ClientProtocolErrorCodes.CALLER_NOT_MEMBER, CallerNotMemberException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new CallerNotMemberException(message);
            }
        });
        register(ClientProtocolErrorCodes.CANCELLATION, CancellationException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new CancellationException(message);
            }
        });
        register(ClientProtocolErrorCodes.CLASS_CAST, ClassCastException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new ClassCastException(message);
            }
        });
        register(ClientProtocolErrorCodes.CLASS_NOT_FOUND, ClassNotFoundException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new ClassNotFoundException(message, cause);
            }
        });
        register(ClientProtocolErrorCodes.CONCURRENT_MODIFICATION, ConcurrentModificationException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new ConcurrentModificationException(message);
            }
        });
        register(ClientProtocolErrorCodes.CONFIG_MISMATCH, ConfigMismatchException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new ConfigMismatchException(message);
            }
        });
        register(ClientProtocolErrorCodes.DISTRIBUTED_OBJECT_DESTROYED, DistributedObjectDestroyedException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new DistributedObjectDestroyedException(message);
            }
        });
        register(ClientProtocolErrorCodes.EOF, EOFException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new EOFException(message);
            }
        });
        register(ClientProtocolErrorCodes.EXECUTION, ExecutionException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new ExecutionException(message, cause);
            }
        });
        register(ClientProtocolErrorCodes.HAZELCAST, HazelcastException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new HazelcastException(message, cause);
            }
        });
        register(ClientProtocolErrorCodes.HAZELCAST_INSTANCE_NOT_ACTIVE, HazelcastInstanceNotActiveException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new HazelcastInstanceNotActiveException(message);
            }
        });
        register(ClientProtocolErrorCodes.HAZELCAST_OVERLOAD, HazelcastOverloadException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new HazelcastOverloadException(message);
            }
        });
        register(ClientProtocolErrorCodes.HAZELCAST_SERIALIZATION, HazelcastSerializationException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new HazelcastSerializationException(message, cause);
            }
        });
        register(ClientProtocolErrorCodes.IO, IOException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new IOException(message, cause);
            }
        });
        register(ClientProtocolErrorCodes.ILLEGAL_ARGUMENT, IllegalArgumentException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new IllegalArgumentException(message, cause);
            }
        });
        register(ClientProtocolErrorCodes.ILLEGAL_ACCESS_EXCEPTION, IllegalAccessException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new IllegalAccessException(message);
            }
        });
        register(ClientProtocolErrorCodes.ILLEGAL_ACCESS_ERROR, IllegalAccessError.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new IllegalAccessError(message);
            }
        });
        register(ClientProtocolErrorCodes.ILLEGAL_MONITOR_STATE, IllegalMonitorStateException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new IllegalMonitorStateException(message);
            }
        });
        register(ClientProtocolErrorCodes.ILLEGAL_STATE, IllegalStateException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new IllegalStateException(message, cause);
            }
        });
        register(ClientProtocolErrorCodes.ILLEGAL_THREAD_STATE, IllegalThreadStateException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new IllegalThreadStateException(message);
            }
        });
        register(ClientProtocolErrorCodes.INDEX_OUT_OF_BOUNDS, IndexOutOfBoundsException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new IndexOutOfBoundsException(message);
            }
        });
        register(ClientProtocolErrorCodes.INTERRUPTED, InterruptedException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new InterruptedException(message);
            }
        });
        register(ClientProtocolErrorCodes.INVALID_ADDRESS, AddressUtil.InvalidAddressException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new AddressUtil.InvalidAddressException(message, false);
            }
        });
        register(ClientProtocolErrorCodes.INVALID_CONFIGURATION, InvalidConfigurationException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new InvalidConfigurationException(message, cause);
            }
        });
        register(ClientProtocolErrorCodes.MEMBER_LEFT, MemberLeftException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new MemberLeftException(message);
            }
        });
        register(ClientProtocolErrorCodes.NEGATIVE_ARRAY_SIZE, NegativeArraySizeException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new NegativeArraySizeException(message);
            }
        });
        register(ClientProtocolErrorCodes.NO_SUCH_ELEMENT, NoSuchElementException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new NoSuchElementException(message);
            }
        });
        register(ClientProtocolErrorCodes.NOT_SERIALIZABLE, NotSerializableException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new NotSerializableException(message);
            }
        });
        register(ClientProtocolErrorCodes.NULL_POINTER, NullPointerException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new NullPointerException(message);
            }
        });
        register(ClientProtocolErrorCodes.OPERATION_TIMEOUT, OperationTimeoutException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new OperationTimeoutException(message);
            }
        });
        register(ClientProtocolErrorCodes.PARTITION_MIGRATING, PartitionMigratingException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new PartitionMigratingException(message);
            }
        });
        register(ClientProtocolErrorCodes.QUERY, QueryException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new QueryException(message, cause);
            }
        });
        register(ClientProtocolErrorCodes.QUERY_RESULT_SIZE_EXCEEDED, QueryResultSizeExceededException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new QueryResultSizeExceededException(message);
            }
        });
        register(ClientProtocolErrorCodes.SPLIT_BRAIN_PROTECTION, SplitBrainProtectionException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new SplitBrainProtectionException(message);
            }
        });
        register(ClientProtocolErrorCodes.REACHED_MAX_SIZE, ReachedMaxSizeException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new ReachedMaxSizeException(message);
            }
        });
        register(ClientProtocolErrorCodes.REJECTED_EXECUTION, RejectedExecutionException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new RejectedExecutionException(message, cause);
            }
        });
        register(ClientProtocolErrorCodes.RESPONSE_ALREADY_SENT, ResponseAlreadySentException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new ResponseAlreadySentException(message);
            }
        });
        register(ClientProtocolErrorCodes.RETRYABLE_HAZELCAST, RetryableHazelcastException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new RetryableHazelcastException(message, cause);
            }
        });
        register(ClientProtocolErrorCodes.RETRYABLE_IO, RetryableIOException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new RetryableIOException(message, cause);
            }
        });
        register(ClientProtocolErrorCodes.RUNTIME, RuntimeException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new RuntimeException(message, cause);
            }
        });

        register(ClientProtocolErrorCodes.SECURITY, SecurityException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new SecurityException(message, cause);
            }
        });
        register(ClientProtocolErrorCodes.SOCKET, SocketException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new SocketException(message);
            }
        });
        register(ClientProtocolErrorCodes.STALE_SEQUENCE, StaleSequenceException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new StaleSequenceException(message, 0);
            }
        });
        register(ClientProtocolErrorCodes.TARGET_DISCONNECTED, TargetDisconnectedException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new TargetDisconnectedException(message);
            }
        });
        register(ClientProtocolErrorCodes.TARGET_NOT_MEMBER, TargetNotMemberException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new TargetNotMemberException(message);
            }
        });
        register(ClientProtocolErrorCodes.TIMEOUT, TimeoutException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new TimeoutException(message);
            }
        });
        register(ClientProtocolErrorCodes.TOPIC_OVERLOAD, TopicOverloadException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new TopicOverloadException(message);
            }
        });
        register(ClientProtocolErrorCodes.TRANSACTION, TransactionException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new TransactionException(message, cause);
            }
        });
        register(ClientProtocolErrorCodes.TRANSACTION_NOT_ACTIVE, TransactionNotActiveException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new TransactionNotActiveException(message);
            }
        });
        register(ClientProtocolErrorCodes.TRANSACTION_TIMED_OUT, TransactionTimedOutException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new TransactionTimedOutException(message, cause);
            }
        });
        register(ClientProtocolErrorCodes.URI_SYNTAX, URISyntaxException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new URISyntaxException("not available", message);
            }
        });
        register(ClientProtocolErrorCodes.UTF_DATA_FORMAT, UTFDataFormatException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new UTFDataFormatException(message);
            }
        });
        register(ClientProtocolErrorCodes.UNSUPPORTED_OPERATION, UnsupportedOperationException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new UnsupportedOperationException(message, cause);
            }
        });
        register(ClientProtocolErrorCodes.WRONG_TARGET, WrongTargetException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new WrongTargetException(message);
            }
        });
        register(ClientProtocolErrorCodes.XA, XAException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new XAException(message);
            }
        });
        register(ClientProtocolErrorCodes.ACCESS_CONTROL, AccessControlException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new AccessControlException(message);
            }
        });
        register(ClientProtocolErrorCodes.LOGIN, LoginException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new LoginException(message);
            }
        });
        register(ClientProtocolErrorCodes.UNSUPPORTED_CALLBACK, UnsupportedCallbackException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new UnsupportedCallbackException(null, message);
            }
        });
        register(ClientProtocolErrorCodes.NO_DATA_MEMBER, NoDataMemberInClusterException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new NoDataMemberInClusterException(message);
            }
        });
        register(ClientProtocolErrorCodes.REPLICATED_MAP_CANT_BE_CREATED, ReplicatedMapCantBeCreatedOnLiteMemberException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new ReplicatedMapCantBeCreatedOnLiteMemberException(message);
            }
        });
        register(ClientProtocolErrorCodes.MAX_MESSAGE_SIZE_EXCEEDED, MaxMessageSizeExceeded.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new MaxMessageSizeExceeded(message);
            }
        });
        register(ClientProtocolErrorCodes.WAN_REPLICATION_QUEUE_FULL, WanReplicationQueueFullException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new WanReplicationQueueFullException(message);
            }
        });

        register(ClientProtocolErrorCodes.ASSERTION_ERROR, AssertionError.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new AssertionError(message);
            }
        });
        register(ClientProtocolErrorCodes.OUT_OF_MEMORY_ERROR, OutOfMemoryError.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new OutOfMemoryError(message);
            }
        });
        register(ClientProtocolErrorCodes.STACK_OVERFLOW_ERROR, StackOverflowError.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new StackOverflowError(message);
            }
        });
        register(ClientProtocolErrorCodes.NATIVE_OUT_OF_MEMORY_ERROR, NativeOutOfMemoryError.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new NativeOutOfMemoryError(message, cause);
            }
        });
        register(ClientProtocolErrorCodes.SERVICE_NOT_FOUND, ServiceNotFoundException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new ServiceNotFoundException(message);
            }
        });
        register(ClientProtocolErrorCodes.STALE_TASK_ID, StaleTaskIdException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new StaleTaskIdException(message);
            }
        });
        register(ClientProtocolErrorCodes.DUPLICATE_TASK, DuplicateTaskException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new DuplicateTaskException(message);
            }
        });
        register(ClientProtocolErrorCodes.STALE_TASK, StaleTaskException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new StaleTaskException(message);
            }
        });
        register(ClientProtocolErrorCodes.LOCAL_MEMBER_RESET, LocalMemberResetException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new LocalMemberResetException(message);
            }
        });
        register(ClientProtocolErrorCodes.INDETERMINATE_OPERATION_STATE, IndeterminateOperationStateException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new IndeterminateOperationStateException(message, cause);
            }
        });
        register(ClientProtocolErrorCodes.FLAKE_ID_NODE_ID_OUT_OF_RANGE_EXCEPTION, NodeIdOutOfRangeException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new NodeIdOutOfRangeException(message);
            }
        });
        register(ClientProtocolErrorCodes.TARGET_NOT_REPLICA_EXCEPTION, TargetNotReplicaException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new TargetNotReplicaException(message);
            }
        });
        register(ClientProtocolErrorCodes.MUTATION_DISALLOWED_EXCEPTION, MutationDisallowedException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new MutationDisallowedException(message);
            }
        });
        register(ClientProtocolErrorCodes.CONSISTENCY_LOST_EXCEPTION, ConsistencyLostException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new ConsistencyLostException(message);
            }
        });
        register(SESSION_EXPIRED_EXCEPTION, SessionExpiredException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new SessionExpiredException(message, cause);
            }
        });
        register(WAIT_KEY_CANCELLED_EXCEPTION, WaitKeyCancelledException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new WaitKeyCancelledException(message, cause);
            }
        });
        register(LOCK_ACQUIRE_LIMIT_REACHED_EXCEPTION, LockAcquireLimitReachedException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new LockAcquireLimitReachedException(message);
            }
        });
        register(LOCK_OWNERSHIP_LOST_EXCEPTION, LockOwnershipLostException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new LockOwnershipLostException(message);
            }
        });
        register(CP_GROUP_DESTROYED_EXCEPTION, CPGroupDestroyedException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new CPGroupDestroyedException();
            }
        });
        register(CANNOT_REPLICATE_EXCEPTION, CannotReplicateException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new CannotReplicateException(null);
            }
        });
        register(LEADER_DEMOTED_EXCEPTION, LeaderDemotedException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new LeaderDemotedException(null, null);
            }
        });
        register(STALE_APPEND_REQUEST_EXCEPTION, StaleAppendRequestException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new StaleAppendRequestException(null);
            }
        });
        register(NOT_LEADER_EXCEPTION, NotLeaderException.class, (message, cause) -> new NotLeaderException(null, null, null));
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
        Throwable throwable;
        if (exceptionFactory == null) {
            throwable = new UndefinedErrorCodeException(errorHolder.getMessage(), errorHolder.getClassName());
        } else {
            throwable = exceptionFactory.createException(errorHolder.getMessage(), createException(iterator));
        }
        throwable.setStackTrace(errorHolder.getStackTraceElements().toArray(new StackTraceElement[0]));
        return throwable;
    }

    // method is used by Jet
    @SuppressWarnings("WeakerAccess")
    public void register(int errorCode, Class clazz, ExceptionFactory exceptionFactory) {
        if (intToFactory.containsKey(errorCode)) {
            throw new HazelcastException("Code " + errorCode + " already used");
        }

        if (!clazz.equals(exceptionFactory.createException("", null).getClass())) {
            throw new HazelcastException("Exception factory did not produce an instance of expected class");
        }

        intToFactory.put(errorCode, exceptionFactory);
    }

    public interface ExceptionFactory {
        Throwable createException(String message, Throwable cause);

    }
}
