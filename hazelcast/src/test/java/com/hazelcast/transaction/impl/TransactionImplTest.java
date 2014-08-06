package com.hazelcast.transaction.impl;

import com.hazelcast.instance.MemberImpl;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionOptions;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class TransactionImplTest {

    @Test
    public void testSequentialTransactions() throws Exception {

        TransactionImpl transaction;
        TransactionManagerServiceImpl transactionManagerService = mock(TransactionManagerServiceImpl.class);
        when(transactionManagerService.pickBackupAddresses(anyInt())).thenThrow(new RuntimeException("example exception"));

        NodeEngine nodeEngine = mock(NodeEngine.class);
        when(nodeEngine.getLocalMember()).thenReturn(new MemberImpl());

        transaction = new TransactionImpl(transactionManagerService, nodeEngine, TransactionOptions.getDefault(), null);
        try {
            transaction.begin();
            fail("Transaction expected to fail");
        } catch (Exception e) {
            assertEquals("example exception", e.getMessage());
        }

        // other independent transaction in same thread
        // should behave identically

        transaction = new TransactionImpl(transactionManagerService, nodeEngine, TransactionOptions.getDefault(), "123");
        try {
            transaction.begin();
            fail("Transaction expected to fail");
        } catch (Exception e) {
            assertEquals("example exception", e.getMessage());
        }

    }
}
