/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.cluster.impl.ConfigMismatchException;
import com.hazelcast.config.ConfigurationException;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.core.DuplicateInstanceNameException;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.HazelcastOverloadException;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.core.RuntimeInterruptedException;
import com.hazelcast.map.QueryResultSizeExceededException;
import com.hazelcast.map.ReachedMaxSizeException;
import com.hazelcast.mapreduce.RemoteMapReduceException;
import com.hazelcast.mapreduce.TopologyChangedException;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.query.QueryException;
import com.hazelcast.quorum.QuorumException;
import com.hazelcast.ringbuffer.StaleSequenceException;
import com.hazelcast.spi.exception.CallerNotMemberException;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.spi.exception.PartitionMigratingException;
import com.hazelcast.spi.exception.ResponseAlreadySentException;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.exception.RetryableIOException;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.exception.WrongTargetException;
import com.hazelcast.topic.TopicOverloadException;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionNotActiveException;
import com.hazelcast.transaction.TransactionTimedOutException;
import com.hazelcast.util.AddressUtil;

import javax.cache.CacheException;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import javax.cache.processor.EntryProcessorException;
import javax.transaction.xa.XAException;
import java.io.EOFException;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.UTFDataFormatException;
import java.net.SocketException;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * Each exception that are defined in client protocol have unique identifier which are error code.
 * This class has the error codes and means of
 * 1) creating exception from error code
 * 2) getting the error code of given exception
 */
public final class ClientProtocolErrorCodes {

    public static final int UNDEFINED = 0;
    public static final int ARRAY_INDEX_OUT_OF_BOUNDS = 1;
    public static final int ARRAY_STORE = 2;
    public static final int AUTHENTICATION = 3;
    public static final int CACHE = 4;
    public static final int CACHE_LOADER = 5;
    public static final int CACHE_NOT_EXISTS = 6;
    public static final int CACHE_WRITER = 7;
    public static final int CALLER_NOT_MEMBER = 8;
    public static final int CANCELLATION = 9;
    public static final int CLASS_CAST = 10;
    public static final int CLASS_NOT_FOUND = 11;
    public static final int CONCURRENT_MODIFICATION = 12;
    public static final int CONFIG_MISMATCH = 13;
    public static final int CONFIGURATION = 14;
    public static final int DISTRIBUTED_OBJECT_DESTROYED = 15;
    public static final int DUPLICATE_INSTANCE_NAME = 16;
    public static final int EOF = 17;
    public static final int ENTRY_PROCESSOR = 18;
    public static final int EXECUTION = 19;
    public static final int HAZELCAST = 20;
    public static final int HAZELCAST_INSTANCE_NOT_ACTIVE = 21;
    public static final int HAZELCAST_OVERLOAD = 22;
    public static final int HAZELCAST_SERIALIZATION = 23;
    public static final int IO = 24;
    public static final int ILLEGAL_ARGUMENT = 25;
    public static final int ILLEGAL_MONITOR_STATE = 26;
    public static final int ILLEGAL_STATE = 27;
    public static final int ILLEGAL_THREAD_STATE = 28;
    public static final int INDEX_OUT_OF_BOUNDS = 29;
    public static final int INTERRUPTED = 30;
    public static final int INVALID_ADDRESS = 31;
    public static final int INVALID_CONFIGURATION = 32;
    public static final int MEMBER_LEFT = 33;
    public static final int NEGATIVE_ARRAY_SIZE = 34;
    public static final int NO_SUCH_ELEMENT = 35;
    public static final int NOT_SERIALIZABLE = 36;
    public static final int NULL_POINTER = 37;
    public static final int OPERATION_TIMEOUT = 38;
    public static final int PARTITION_MIGRATING = 39;
    public static final int QUERY = 40;
    public static final int QUERY_RESULT_SIZE_EXCEEDED = 41;
    public static final int QUORUM = 42;
    public static final int REACHED_MAX_SIZE = 43;
    public static final int REJECTED_EXECUTION = 44;
    public static final int REMOTE_MAP_REDUCE = 45;
    public static final int RESPONSE_ALREADY_SENT = 46;
    public static final int RETRYABLE_HAZELCAST = 47;
    public static final int RETRYABLE_IO = 48;
    public static final int RUNTIME = 49;
    public static final int RUNTIME_INTERRUPTED = 50;
    public static final int SECURITY = 51;
    public static final int SOCKET = 52;
    public static final int STALE_SEQUENCE = 53;
    public static final int TARGET_DISCONNECTED = 54;
    public static final int TARGET_NOT_MEMBER = 55;
    public static final int TIMEOUT = 56;
    public static final int TOPIC_OVERLOAD = 57;
    public static final int TOPOLOGY_CHANGED = 58;
    public static final int TRANSACTION = 59;
    public static final int TRANSACTION_NOT_ACTIVE = 60;
    public static final int TRANSACTION_TIMED_OUT = 61;
    public static final int URI_SYNTAX = 62;
    public static final int UTF_DATA_FORMAT = 63;
    public static final int UNSUPPORTED_OPERATION = 64;
    public static final int WRONG_TARGET = 65;
    public static final int XA = 66;

    private Map<Class, Integer> classToInt = new ConcurrentHashMap<Class, Integer>();
    private Map<Integer, ExceptionFactory> intToFactory = new ConcurrentHashMap<Integer, ExceptionFactory>();

    public ClientProtocolErrorCodes() {
        register(ARRAY_INDEX_OUT_OF_BOUNDS, ArrayIndexOutOfBoundsException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new ArrayIndexOutOfBoundsException(message);
            }
        });
        register(ARRAY_STORE, ArrayStoreException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new ArrayStoreException(message);
            }
        });
        register(AUTHENTICATION, AuthenticationException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new AuthenticationException(message);
            }
        });
        register(CACHE, CacheException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new CacheException(message);
            }
        });
        register(CACHE_LOADER, CacheLoaderException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new CacheLoaderException(message);
            }
        });
        register(CACHE_NOT_EXISTS, CacheNotExistsException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new CacheNotExistsException(message);
            }
        });
        register(CACHE_WRITER, CacheWriterException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new CacheWriterException(message);
            }
        });
        register(CALLER_NOT_MEMBER, CallerNotMemberException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new CallerNotMemberException(message);
            }
        });
        register(CANCELLATION, CancellationException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new CancellationException(message);
            }
        });
        register(CLASS_CAST, ClassCastException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new ClassCastException(message);
            }
        });
        register(CLASS_NOT_FOUND, ClassNotFoundException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new ClassNotFoundException(message);
            }
        });
        register(CONCURRENT_MODIFICATION, ConcurrentModificationException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new ConcurrentModificationException(message);
            }
        });
        register(CONFIG_MISMATCH, ConfigMismatchException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new ConfigMismatchException(message);
            }
        });
        register(CONFIGURATION, ConfigurationException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new ConfigurationException(message, null, null);
            }
        });
        register(DISTRIBUTED_OBJECT_DESTROYED, DistributedObjectDestroyedException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new DistributedObjectDestroyedException(message);
            }
        });
        register(DUPLICATE_INSTANCE_NAME, DuplicateInstanceNameException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new DuplicateInstanceNameException(message);
            }
        });
        register(EOF, EOFException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new EOFException(message);
            }
        });
        register(ENTRY_PROCESSOR, EntryProcessorException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new EntryProcessorException(message);
            }
        });
        register(EXECUTION, ExecutionException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new ExecutionException(message, null);
            }
        });
        register(HAZELCAST, HazelcastException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new HazelcastException(message);
            }
        });
        register(HAZELCAST_INSTANCE_NOT_ACTIVE, HazelcastInstanceNotActiveException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new HazelcastInstanceNotActiveException(message);
            }
        });
        register(HAZELCAST_OVERLOAD, HazelcastOverloadException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new HazelcastOverloadException(message);
            }
        });
        register(HAZELCAST_SERIALIZATION, HazelcastSerializationException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new HazelcastSerializationException(message);
            }
        });
        register(IO, IOException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new IOException(message);
            }
        });
        register(ILLEGAL_ARGUMENT, IllegalArgumentException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new IllegalArgumentException(message);
            }
        });
        register(ILLEGAL_MONITOR_STATE, IllegalMonitorStateException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new IllegalMonitorStateException(message);
            }
        });
        register(ILLEGAL_STATE, IllegalStateException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new IllegalStateException(message);
            }
        });
        register(ILLEGAL_THREAD_STATE, IllegalThreadStateException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new IllegalThreadStateException(message);
            }
        });
        register(INDEX_OUT_OF_BOUNDS, IndexOutOfBoundsException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new IndexOutOfBoundsException(message);
            }
        });
        register(INTERRUPTED, InterruptedException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new InterruptedException(message);
            }
        });
        register(INVALID_ADDRESS, AddressUtil.InvalidAddressException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new AddressUtil.InvalidAddressException(message);
            }
        });
        register(INVALID_CONFIGURATION, InvalidConfigurationException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new InvalidConfigurationException(message);
            }
        });
        register(MEMBER_LEFT, MemberLeftException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new MemberLeftException(message);
            }
        });
        register(NEGATIVE_ARRAY_SIZE, NegativeArraySizeException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new NegativeArraySizeException(message);
            }
        });
        register(NO_SUCH_ELEMENT, NoSuchElementException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new NoSuchElementException(message);
            }
        });
        register(NOT_SERIALIZABLE, NotSerializableException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new NotSerializableException(message);
            }
        });
        register(NULL_POINTER, NullPointerException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new NullPointerException(message);
            }
        });
        register(OPERATION_TIMEOUT, OperationTimeoutException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new OperationTimeoutException(message);
            }
        });
        register(PARTITION_MIGRATING, PartitionMigratingException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new PartitionMigratingException(message);
            }
        });
        register(QUERY, QueryException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new QueryException(message);
            }
        });
        register(QUERY_RESULT_SIZE_EXCEEDED, QueryResultSizeExceededException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new QueryResultSizeExceededException(message);
            }
        });
        register(QUORUM, QuorumException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new QuorumException(message);
            }
        });
        register(REACHED_MAX_SIZE, ReachedMaxSizeException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new ReachedMaxSizeException(message);
            }
        });
        register(REJECTED_EXECUTION, RejectedExecutionException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new RejectedExecutionException(message);
            }
        });
        register(REMOTE_MAP_REDUCE, RemoteMapReduceException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new RemoteMapReduceException(message, Collections.<Exception>emptyList());
            }
        });
        register(RESPONSE_ALREADY_SENT, ResponseAlreadySentException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new ResponseAlreadySentException(message);
            }
        });
        register(RETRYABLE_HAZELCAST, RetryableHazelcastException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new RetryableHazelcastException(message);
            }
        });
        register(RETRYABLE_IO, RetryableIOException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new RetryableIOException(message);
            }
        });
        register(RUNTIME, RuntimeException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new RuntimeException(message);
            }
        });
        register(RUNTIME_INTERRUPTED, RuntimeInterruptedException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new RuntimeInterruptedException(message);
            }
        });
        register(SECURITY, SecurityException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new SecurityException(message);
            }
        });
        register(SOCKET, SocketException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new SocketException(message);
            }
        });
        register(STALE_SEQUENCE, StaleSequenceException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new StaleSequenceException(message, 0);
            }
        });
        register(TARGET_DISCONNECTED, TargetDisconnectedException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new TargetDisconnectedException(message);
            }
        });
        register(TARGET_NOT_MEMBER, TargetNotMemberException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new TargetNotMemberException(message);
            }
        });
        register(TIMEOUT, TimeoutException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new TimeoutException(message);
            }
        });
        register(TOPIC_OVERLOAD, TopicOverloadException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new TopicOverloadException(message);
            }
        });
        register(TOPOLOGY_CHANGED, TopologyChangedException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new TopologyChangedException(message);
            }
        });
        register(TRANSACTION, TransactionException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new TransactionException(message);
            }
        });
        register(TRANSACTION_NOT_ACTIVE, TransactionNotActiveException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new TransactionNotActiveException(message);
            }
        });
        register(TRANSACTION_TIMED_OUT, TransactionTimedOutException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new TransactionTimedOutException(message);
            }
        });
        register(URI_SYNTAX, URISyntaxException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new URISyntaxException(null, message, -1);
            }
        });
        register(UTF_DATA_FORMAT, UTFDataFormatException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new UTFDataFormatException(message);
            }
        });
        register(UNSUPPORTED_OPERATION, UnsupportedOperationException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new UnsupportedOperationException(message);
            }
        });
        register(WRONG_TARGET, WrongTargetException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new WrongTargetException(message);
            }
        });
        register(XA, XAException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new XAException(message);
            }
        });

    }

    interface ExceptionFactory {
        Throwable createException(String message);

    }

    public Throwable createException(int errorCode, String className, String message, StackTraceElement[] stackTrace
            , int causeErrorCode, String causeClassName) {
        Throwable throwable = createException(errorCode, className, message);
        throwable.setStackTrace(stackTrace);
        if (causeClassName != null) {
            throwable.initCause(createException(causeErrorCode, causeClassName, null));
        }
        return throwable;
    }

    private Throwable createException(int errorCode, String className, String message) {
        ExceptionFactory exceptionFactory = intToFactory.get(errorCode);
        Throwable throwable;
        if (exceptionFactory == null) {
            throwable = new UndefinedErrorCodeException(message, className);
        } else {
            throwable = exceptionFactory.createException(message);
        }
        return throwable;
    }

    public void register(int errorCode, Class clazz, ExceptionFactory exceptionFactory) {
        classToInt.put(clazz, errorCode);
        intToFactory.put(errorCode, exceptionFactory);
    }


    public int getErrorCode(Throwable e) {
        Integer errorCode = classToInt.get(e.getClass());
        if (errorCode == null) {
            return UNDEFINED;
        }
        return errorCode;
    }
}
