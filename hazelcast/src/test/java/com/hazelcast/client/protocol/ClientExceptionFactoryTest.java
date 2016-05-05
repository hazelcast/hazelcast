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

package com.hazelcast.client.protocol;

import com.hazelcast.cache.CacheNotExistsException;
import com.hazelcast.client.AuthenticationException;
import com.hazelcast.client.impl.protocol.ClientExceptionFactory;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.exception.MaxMessageSizeExceeded;
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
import com.hazelcast.memory.NativeOutOfMemoryError;
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
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.exception.WrongTargetException;
import com.hazelcast.test.HazelcastTestRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.RunParallel;
import com.hazelcast.topic.TopicOverloadException;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionNotActiveException;
import com.hazelcast.transaction.TransactionTimedOutException;
import com.hazelcast.util.AddressUtil;
import com.hazelcast.wan.WANReplicationQueueFullException;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.cache.CacheException;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import javax.cache.processor.EntryProcessorException;
import javax.security.auth.callback.Callback;
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
import java.util.NoSuchElementException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;

import static junit.framework.Assert.assertNull;
import static junit.framework.TestCase.assertEquals;

@RunParallel
@RunWith(HazelcastTestRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientExceptionFactoryTest extends HazelcastTestSupport {

    ClientExceptionFactory exceptionFactory = new ClientExceptionFactory(true);

    @Parameterized.Parameter
    public Throwable throwable;

    @Parameterized.Parameters(name = "Throwable:{0}")
    public static Iterable<Object[]> parameters() {
        return Arrays.asList(
                new Object[]{new CacheException(randomString())},
                new Object[]{new CacheLoaderException(randomString())},
                new Object[]{new CacheWriterException(randomString())},
                new Object[]{new EntryProcessorException(randomString())},
                new Object[]{new ArrayIndexOutOfBoundsException(randomString())},
                new Object[]{new ArrayStoreException(randomString())},
                new Object[]{new AuthenticationException(randomString())},
                new Object[]{new CacheNotExistsException(randomString())},
                new Object[]{new CallerNotMemberException(randomString())},
                new Object[]{new CancellationException(randomString())},
                new Object[]{new ClassCastException(randomString())},
                new Object[]{new ClassNotFoundException(randomString())},
                new Object[]{new ConcurrentModificationException(randomString())},
                new Object[]{new ConfigMismatchException(randomString())},
                new Object[]{new ConfigurationException(randomString())},
                new Object[]{new DistributedObjectDestroyedException(randomString())},
                new Object[]{new DuplicateInstanceNameException(randomString())},
                new Object[]{new EOFException(randomString())},
                new Object[]{new ExecutionException(new IOException())},
                new Object[]{new HazelcastException(randomString())},
                new Object[]{new HazelcastInstanceNotActiveException(randomString())},
                new Object[]{new HazelcastOverloadException(randomString())},
                new Object[]{new HazelcastSerializationException(randomString())},
                new Object[]{new IOException(randomString())},
                new Object[]{new IllegalArgumentException(randomString())},
                new Object[]{new IllegalAccessException(randomString())},
                new Object[]{new IllegalAccessError(randomString())},
                new Object[]{new IllegalMonitorStateException(randomString())},
                new Object[]{new IllegalStateException(randomString())},
                new Object[]{new IllegalThreadStateException(randomString())},
                new Object[]{new IndexOutOfBoundsException(randomString())},
                new Object[]{new InterruptedException(randomString())},
                new Object[]{new AddressUtil.InvalidAddressException(randomString())},
                new Object[]{new InvalidConfigurationException(randomString())},
                new Object[]{new MemberLeftException(randomString())},
                new Object[]{new NegativeArraySizeException(randomString())},
                new Object[]{new NoSuchElementException(randomString())},
                new Object[]{new NotSerializableException(randomString())},
                new Object[]{new NullPointerException(randomString())},
                new Object[]{new OperationTimeoutException(randomString())},
                new Object[]{new PartitionMigratingException(randomString())},
                new Object[]{new QueryException(randomString())},
                new Object[]{new QueryResultSizeExceededException(randomString())},
                new Object[]{new QuorumException(randomString())},
                new Object[]{new ReachedMaxSizeException(randomString())},
                new Object[]{new RejectedExecutionException(randomString())},
                new Object[]{new RemoteMapReduceException(randomMapName(), Collections.<Exception>emptyList())},
                new Object[]{new ResponseAlreadySentException(randomString())},
                new Object[]{new RetryableHazelcastException(randomString())},
                new Object[]{new RetryableIOException(randomString())},
                new Object[]{new RuntimeException(randomString())},
                new Object[]{new SecurityException(randomString())},
                new Object[]{new SocketException(randomString())},
                new Object[]{new StaleSequenceException(randomString(), 1)},
                new Object[]{new TargetDisconnectedException(randomString())},
                new Object[]{new TargetNotMemberException(randomString())},
                new Object[]{new TimeoutException(randomString())},
                new Object[]{new TopicOverloadException(randomString())},
                new Object[]{new TopologyChangedException(randomString())},
                new Object[]{new TransactionException(randomString())},
                new Object[]{new TransactionNotActiveException(randomString())},
                new Object[]{new TransactionTimedOutException(randomString())},
                new Object[]{new URISyntaxException(randomString(), randomString())},
                new Object[]{new UTFDataFormatException(randomString())},
                new Object[]{new UnsupportedOperationException(randomString())},
                new Object[]{new WrongTargetException(randomString())},
                new Object[]{new XAException(randomString())},
                new Object[]{new AccessControlException(randomString())},
                new Object[]{new LoginException(randomString())},
                new Object[]{new UnsupportedCallbackException(new Callback() {
                })},
                new Object[]{new NoDataMemberInClusterException(randomString())},
                new Object[]{new ReplicatedMapCantBeCreatedOnLiteMemberException(randomString())},
                new Object[]{new MaxMessageSizeExceeded()},
                new Object[]{new WANReplicationQueueFullException(randomString())},
                new Object[]{new AssertionError(randomString())},
                new Object[]{new OutOfMemoryError(randomString())},
                new Object[]{new StackOverflowError(randomString())},
                new Object[]{new NativeOutOfMemoryError(randomString())});

    }

    @Test
    public void testException() {
        ClientMessage exceptionMessage = exceptionFactory.createExceptionMessage(throwable);
        ClientMessage responseMessage = ClientMessage.createForDecode(exceptionMessage.buffer(), 0);
        Throwable resurrectedThrowable = exceptionFactory.createException(responseMessage);
        assertEquals(throwable.getClass(), resurrectedThrowable.getClass());
        assertStackTraceArrayEquals(throwable.getStackTrace(), resurrectedThrowable.getStackTrace());
        Throwable cause = throwable.getCause();
        if (cause == null) {
            assertNull(resurrectedThrowable.getCause());
        } else {
            assertEquals(cause.getClass(), resurrectedThrowable.getCause().getClass());
        }
    }

    private void assertStackTraceArrayEquals(StackTraceElement[] stackTrace1, StackTraceElement[] stackTrace2) {
        assertEquals(stackTrace1.length, stackTrace2.length);
        for (int i = 0; i < stackTrace1.length; i++) {
            StackTraceElement stackTraceElement1 = stackTrace1[i];
            StackTraceElement stackTraceElement2 = stackTrace2[i];
            //Not using stackTraceElement.equals
            //because in IBM JDK stacktraceElements with null method name are not equal
            assertEquals(stackTraceElement1.getClassName(), stackTraceElement2.getClassName());
            assertEquals(stackTraceElement1.getMethodName(), stackTraceElement2.getMethodName());
            assertEquals(stackTraceElement1.getFileName(), stackTraceElement2.getFileName());
            assertEquals(stackTraceElement1.getLineNumber(), stackTraceElement2.getLineNumber());
        }

    }

}