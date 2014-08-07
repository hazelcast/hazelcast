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
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionOptions;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

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

        transaction = new TransactionImpl(transactionManagerService, nodeEngine, TransactionOptions.getDefault(), null);
        try {
            transaction.begin();
            fail("Transaction expected to fail");
        } catch (Exception e) {
            assertEquals(expectedException, e);
        }

        // other independent transaction in same thread
        // should behave identically
        transaction = new TransactionImpl(transactionManagerService, nodeEngine, TransactionOptions.getDefault(), "123");
        try {
            transaction.begin();
            fail("Transaction expected to fail");
        } catch (Exception e) {
            assertEquals(expectedException, e);
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
