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
import java.util.HashMap;
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
public class ClientExceptionFactory {

    private final Map<Class, Integer> classToInt = new HashMap<Class, Integer>();
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
                return new AddressUtil.InvalidAddressException(message);
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
                return new URISyntaxException(null, message, -1);
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

    }

    interface ExceptionFactory {
        Throwable createException(String message, Throwable cause);

    }

    public Throwable createException(int errorCode, String className, String message, StackTraceElement[] stackTrace
            , int causeErrorCode, String causeClassName) {
        Throwable cause = null;
        if (causeClassName != null) {
            cause = createException(causeErrorCode, causeClassName, null, null);
        }

        Throwable throwable = createException(errorCode, className, message, cause);
        throwable.setStackTrace(stackTrace);
        return throwable;
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
        classToInt.put(clazz, errorCode);
        intToFactory.put(errorCode, exceptionFactory);
    }


    public int getErrorCode(Throwable e) {
        Integer errorCode = classToInt.get(e.getClass());
        if (errorCode == null) {
            return ClientProtocolErrorCodes.UNDEFINED;
        }
        return errorCode;
    }
}
