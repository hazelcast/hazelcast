/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.transaction.impl;

import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.AbstractLogger;
import com.hazelcast.logging.LogEvent;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionOptions.TransactionType;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class TransactionImplTest {

    @Test
    public void testTransactionBegin_whenBeginThrowsException() throws Exception {

        TransactionImpl transaction;
        TransactionManagerServiceImpl transactionManagerService = mock(TransactionManagerServiceImpl.class);
        RuntimeException expectedException = new RuntimeException("example exception");
        when(transactionManagerService.pickBackupAddresses(anyInt()))
                .thenThrow(expectedException);

        NodeEngine nodeEngine = mock(NodeEngine.class);
        when(nodeEngine.getLocalMember()).thenReturn(new MemberImpl());
        when(nodeEngine.getLogger(TransactionImpl.class)).thenReturn(new DummyLogger());

        TransactionOptions options = TransactionOptions.getDefault();
        transaction = new TransactionImpl(transactionManagerService, nodeEngine, options, null);
        try {
            transaction.begin();
            fail("Transaction expected to fail");
        } catch (Exception e) {
            assertEquals(expectedException, e);
        }

        // other independent transaction in same thread
        // should behave identically
        transaction = new TransactionImpl(transactionManagerService, nodeEngine, options, "123");
        try {
            transaction.begin();
            fail("Transaction expected to fail");
        } catch (Exception e) {
            assertEquals(expectedException, e);
        }
    }

    @Test(expected = TransactionException.class)
    public void testLocalTransaction_ThrowsExceptionDuringCommit() throws Exception {
        TransactionImpl transaction;
        TransactionManagerServiceImpl transactionManagerService = mock(TransactionManagerServiceImpl.class);

        NodeEngine nodeEngine = mock(NodeEngine.class);
        when(nodeEngine.getLocalMember()).thenReturn(new MemberImpl());
        when(nodeEngine.getLogger(TransactionImpl.class)).thenReturn(new DummyLogger());

        TransactionOptions options = new TransactionOptions().setTransactionType(TransactionType.LOCAL);
        transaction = new TransactionImpl(transactionManagerService, nodeEngine, options, "dummy-uuid");
        transaction.begin();
        transaction.addTransactionLog(new FailingTransactionLog(false, true, false));
        transaction.commit();
    }

    @Test(expected = TransactionException.class)
    public void test2PhaseTransaction_ThrowsExceptionDuringPrepare() throws Exception {
        TransactionImpl transaction;
        TransactionManagerServiceImpl transactionManagerService = mock(TransactionManagerServiceImpl.class);

        NodeEngine nodeEngine = mock(NodeEngine.class);
        when(nodeEngine.getLocalMember()).thenReturn(new MemberImpl());
        when(nodeEngine.getLogger(TransactionImpl.class)).thenReturn(new DummyLogger());

        TransactionOptions options = new TransactionOptions()
                .setTransactionType(TransactionType.TWO_PHASE).setDurability(0);
        transaction = new TransactionImpl(transactionManagerService, nodeEngine, options, "dummy-uuid");

        transaction.begin();
        transaction.addTransactionLog(new FailingTransactionLog(true, true, false));
        transaction.prepare();
    }

    @Test
    public void test2PhaseTransaction_ThrowsExceptionDuringCommit() throws Exception {
        TransactionImpl transaction;
        TransactionManagerServiceImpl transactionManagerService = mock(TransactionManagerServiceImpl.class);

        NodeEngine nodeEngine = mock(NodeEngine.class);
        when(nodeEngine.getLocalMember()).thenReturn(new MemberImpl());
        when(nodeEngine.getLogger(TransactionImpl.class)).thenReturn(new DummyLogger());

        TransactionOptions options = new TransactionOptions()
                .setTransactionType(TransactionType.TWO_PHASE).setDurability(0);
        transaction = new TransactionImpl(transactionManagerService, nodeEngine, options, "dummy-uuid");
        transaction.begin();
        transaction.addTransactionLog(new FailingTransactionLog(false, true, false));
        transaction.prepare();
        transaction.commit();
    }

    private static class FailingTransactionLog implements TransactionLog {
        final boolean failPrepare;
        final boolean failCommit;
        final boolean failRollback;

        public FailingTransactionLog(boolean failPrepare, boolean failCommit, boolean failRollback) {
            this.failPrepare = failPrepare;
            this.failCommit = failCommit;
            this.failRollback = failRollback;
        }

        @Override
        public Future prepare(NodeEngine nodeEngine) {
            return failPrepare ? new FailingFuture() : new FutureAdapter();
        }

        @Override
        public Future commit(NodeEngine nodeEngine) {
            return failCommit ? new FailingFuture() : new FutureAdapter();
        }

        @Override
        public Future rollback(NodeEngine nodeEngine) {
            return failRollback ? new FailingFuture() : new FutureAdapter();
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
        }
    }

    private static class FutureAdapter implements Future {
        @Override
        public Object get() throws InterruptedException, ExecutionException {
            return null;
        }
        @Override
        public Object get(long timeout, TimeUnit unit)
                throws InterruptedException, ExecutionException, TimeoutException {
            return null;
        }
        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }
        @Override
        public boolean isCancelled() {
            return false;
        }
        @Override
        public boolean isDone() {
            return false;
        }
    }

    private static class FailingFuture extends FutureAdapter {
        @Override
        public Object get() throws InterruptedException, ExecutionException {
            throw new TransactionException();
        }
        @Override
        public Object get(long timeout, TimeUnit unit)
                throws InterruptedException, ExecutionException, TimeoutException {
            return get();
        }
    }

    private class DummyLogger extends AbstractLogger {
        @Override
        public void log(Level level, String message) {
        }

        @Override
        public void log(Level level, String message, Throwable thrown) {
        }

        @Override
        public void log(LogEvent logEvent) {
        }

        @Override
        public Level getLevel() {
            return Level.INFO;
        }

        @Override
        public boolean isLoggable(Level level) {
            return false;
        }
    }
}
