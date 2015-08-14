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

import com.hazelcast.client.AuthenticationException;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.quorum.QuorumException;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * Each exception that are defined in client protocol have unique identifier which are error code.
 * This class has the error codes and means of
 * 1) creating exception from error code
 * 2) getting the error code of given exception
 */
public final class ClientProtocolErrorCodes {

    public static final int UNDEFINED = 0;
    public static final int HAZELCAST = 1;
    public static final int IO = 2;
    public static final int QUORUM = 3;
    public static final int AUTHENTICATION = 4;
    public static final int HAZELCAST_INSTANCE_NOT_ACTIVE = 5;
    public static final int ILLEGAL_ARGUMENT = 6;
    public static final int EXECUTION = 7;
    public static final int TIMEOUT = 8;
    public static final int INTERRUPTED = 10;
    public static final int ILLEGAL_MONITOR_STATE = 11;
    public static final int DISTRIBUTED_OBJECT_DESTROYED = 12;
    public static final int ILLEGAL_STATE = 13;
    private static Map<Class, Integer> classToInt = new ConcurrentHashMap<Class, Integer>();
    private static Map<Integer, ExceptionFactory> intToFactory = new ConcurrentHashMap<Integer, ExceptionFactory>();

    static {
        register(IO, IOException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new IOException(message);
            }
        });

        register(QUORUM, QuorumException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new QuorumException(message);
            }
        });
        register(AUTHENTICATION, AuthenticationException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new AuthenticationException(message);
            }
        });
        register(HAZELCAST_INSTANCE_NOT_ACTIVE, HazelcastInstanceNotActiveException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new HazelcastInstanceNotActiveException(message);
            }
        });
        register(ILLEGAL_STATE, IllegalStateException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new IllegalStateException(message);
            }
        });
        register(ILLEGAL_ARGUMENT, IllegalArgumentException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new IllegalArgumentException(message);
            }
        });
        register(EXECUTION, ExecutionException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new ExecutionException(message, null);
            }
        });
        register(TIMEOUT, TimeoutException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new TimeoutException(message);
            }
        });
        register(INTERRUPTED, InterruptedException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new InterruptedException(message);
            }
        });
        register(ILLEGAL_MONITOR_STATE, IllegalMonitorStateException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new IllegalMonitorStateException(message);
            }
        });
        register(DISTRIBUTED_OBJECT_DESTROYED, DistributedObjectDestroyedException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new DistributedObjectDestroyedException(message);
            }
        });
        register(ILLEGAL_STATE, IllegalStateException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new IllegalStateException(message);
            }
        });
        register(HAZELCAST, HazelcastException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message) {
                return new HazelcastException(message);
            }
        });
        //TODO following list will be registered !?!?!
        /*
        Total of 64 exceptions

        ArrayIndexOutOfBoundsException
        ArrayStoreException
        AssertionError
        AuthenticationException
        CacheException
        CacheLoaderException
        CacheNotExistsException
        CacheWriterException
        ClassCastException
        ClassNotFoundException
        ConcurrentModificationException
        ConfigMismatchException
        ConfigurationException
        DuplicateInstanceNameException
        EOFException
        EntryProcessorException
        Error
        Exception
        ExecutionException
        HazelcastClientNotActiveException
        HazelcastException
        HazelcastInstanceNotActiveException
        HazelcastOverloadException
        HazelcastSerializationException
        IOException
        IllegalArgumentException
        IllegalMonitorStateException
        IllegalStateException
        IllegalThreadStateException
        IncompatibleClassChangeError
        IndexOutOfBoundsException
        InvalidAddressException
        InvalidConfigurationException
        NegativeArraySizeException
        NoSuchElementException
        NotSerializableException
        NullPointerException
        OperationTimeoutException
        OutOfMemoryError
        PartitionMigratingException
        QueryException
        QueryResultSizeExceededException
        QuorumException
        ReachedMaxSizeException
        ReflectionException
        RejectedExecutionException
        RemoteMapReduceException
        ResponseAlreadySentException
        RetryableHazelcastException
        RuntimeException
        SecurityException
        StaleSequenceException
        TargetNotMemberException
        TimeoutException
        TopicOverloadException
        TransactionException
        TransactionNotActiveException
        TransactionTimedOutException
        URISyntaxException
        UTFDataFormatException
        UnsupportedOperationException
        WrongTargetException
        XAException

        * */

    }

    interface ExceptionFactory {
        Throwable createException(String message);

    }

    private ClientProtocolErrorCodes() {

    }

    public static void register(int errorCode, Class clazz, ExceptionFactory exceptionFactory) {
        classToInt.put(clazz, errorCode);
        intToFactory.put(errorCode, exceptionFactory);
    }

    public static Throwable createException(int errorCode, String message, StackTraceElement[] stackTrace) {
        ExceptionFactory exceptionFactory = intToFactory.get(errorCode);
        Throwable throwable;
        if (exceptionFactory == null) {
            throwable = new HazelcastException(message);
        } else {
            throwable = exceptionFactory.createException(message);
        }
        throwable.setStackTrace(stackTrace);
        return throwable;
    }

    public static int getErrorCode(Throwable e) {
        Integer errorCode = classToInt.get(e.getClass());
        if (errorCode == null) {
            return UNDEFINED;
        }
        return errorCode;
    }
}
