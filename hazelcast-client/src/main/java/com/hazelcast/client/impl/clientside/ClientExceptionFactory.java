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

package com.hazelcast.client.impl.clientside;

import com.hazelcast.cache.CacheNotExistsException;
import com.hazelcast.client.AuthenticationException;
import com.hazelcast.client.UndefinedErrorCodeException;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class has the error codes and means of
 * 1) creating exception from error code
 * 2) getting the error code of given exception
 */
public class ClientExceptionFactory {

    private static final String CAUSED_BY_STACKTRACE_MARKER = "###### Caused by:";

    /**
     * This pattern extracts errorCode and exception message from the encoded Caused-by marker.
     * It has the form:
     * <pre>    ###### Caused by: (&lt;errorCode>) &lt;cause.toString()> ------</pre>
     *
     * As per {@link Throwable#toString()}, this has the form
     * <pre>&lt;exception class>: &lt;message></pre>
     *
     * if message is present, or just {@code &lt;exception class>}, if message is null.
     *
     * <p>Commonly, exceptions with causes are created like this:
     * <pre>new RuntimeException("Additional message: " + e, e);</pre>
     *
     * Thus, this pattern matches the marker, error code in parentheses, text up to the semicolon
     * (reluctantly, as to find the first one), and optional semicolon and the rest of message.
     */
    private static final Pattern CAUSED_BY_STACKTRACE_PARSER = Pattern.compile(Pattern.quote(CAUSED_BY_STACKTRACE_MARKER)
            + " \\((-?[0-9]+)\\) (.+?)(: (.*))? ------", Pattern.DOTALL);
    private static final int CAUSED_BY_STACKTRACE_PARSER_ERROR_CODE_GROUP = 1;
    private static final int CAUSED_BY_STACKTRACE_PARSER_CLASS_NAME_GROUP = 2;
    private static final int CAUSED_BY_STACKTRACE_PARSER_MESSAGE_GROUP = 4;

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
        register(ClientProtocolErrorCodes.CONFIGURATION, ConfigurationException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new ConfigurationException(message);
            }
        });
        register(ClientProtocolErrorCodes.DISTRIBUTED_OBJECT_DESTROYED, DistributedObjectDestroyedException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new DistributedObjectDestroyedException(message);
            }
        });
        register(ClientProtocolErrorCodes.DUPLICATE_INSTANCE_NAME, DuplicateInstanceNameException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new DuplicateInstanceNameException(message);
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
        register(ClientProtocolErrorCodes.QUORUM, QuorumException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new QuorumException(message);
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
        register(ClientProtocolErrorCodes.REMOTE_MAP_REDUCE, RemoteMapReduceException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new RemoteMapReduceException(message, Collections.<Exception>emptyList());
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
        register(ClientProtocolErrorCodes.TOPOLOGY_CHANGED, TopologyChangedException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new TopologyChangedException(message);
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
                return new MaxMessageSizeExceeded();
            }
        });
        register(ClientProtocolErrorCodes.WAN_REPLICATION_QUEUE_FULL, WANReplicationQueueFullException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new WANReplicationQueueFullException(message);
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
    }

    public Throwable createException(ClientMessage clientMessage) {
        ErrorCodec parameters = ErrorCodec.decode(clientMessage);

        // first, try to search for the marker to see, if there are any "hidden" causes
        boolean causedByMarkerFound = false;
        for (int i = 0; ! causedByMarkerFound && i < parameters.stackTrace.length; i++) {
            causedByMarkerFound = parameters.stackTrace[i].getClassName().startsWith(CAUSED_BY_STACKTRACE_MARKER);
        }

        if (causedByMarkerFound) {
            // This exception has a cause and is from a 3.8+ node
            StackTraceElement[] st = parameters.stackTrace;

            // Iterate from the end
            int pos = st.length;
            int lastPos = pos;
            Throwable t = null;
            while (pos >= 0) {
                Throwable t1 = null;
                if (pos == 0) {
                    // the root exception
                    t1 = createException(parameters.errorCode, parameters.className, parameters.message, t);
                } else if (st[pos - 1].getClassName().startsWith(CAUSED_BY_STACKTRACE_MARKER)) {
                    Matcher matcher = CAUSED_BY_STACKTRACE_PARSER.matcher(st[pos - 1].getClassName());
                    if (matcher.find()) {
                        int errorCode = Integer.parseInt(matcher.group(CAUSED_BY_STACKTRACE_PARSER_ERROR_CODE_GROUP));
                        String className = matcher.group(CAUSED_BY_STACKTRACE_PARSER_CLASS_NAME_GROUP);
                        String message = matcher.group(CAUSED_BY_STACKTRACE_PARSER_MESSAGE_GROUP);
                        t1 = createException(errorCode, className, message, t);
                    } else {
                        // unexpected text, just parse somehow
                        t1 = createException(ClientProtocolErrorCodes.UNDEFINED, st[pos - 1].toString(), null, t);
                    }
                }
                if (t1 != null) {
                    t1.setStackTrace(Arrays.copyOfRange(st, pos, lastPos));
                    pos--;
                    lastPos = pos;
                    t = t1;
                }
                pos--;
            }
            return t;
        } else {
            // In this case, the exception does not have a cause, or is from a pre-3.8 node (3.7 or older)
            Throwable cause = null;
            // this is for backwards compatibility, currently not used (causes and their causes are hidden in the root stacktrace)
            if (parameters.causeClassName != null) {
                cause = createException(parameters.causeErrorCode, parameters.causeClassName, null, null);
            }

            Throwable throwable = createException(parameters.errorCode, parameters.className, parameters.message, cause);

            throwable.setStackTrace(parameters.stackTrace);
            return throwable;
        }
    }

    private Throwable createException(int errorCode, String className, String message, Throwable cause) {
        ExceptionFactory exceptionFactory = intToFactory.get(errorCode);
        Throwable throwable;
        if (exceptionFactory == null) {
            throwable = new UndefinedErrorCodeException(message, className);
        } else {
            throwable = exceptionFactory.createException(message, cause);
        }
        return throwable;
    }

    private void register(int errorCode, Class clazz, ExceptionFactory exceptionFactory) {

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
