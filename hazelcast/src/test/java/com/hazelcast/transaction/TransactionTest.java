package com.hazelcast.transaction;


import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class TransactionTest extends HazelcastTestSupport {

    private HazelcastInstance hz;

    @Before
    public void setup() {
        hz = createHazelcastInstance();
    }

    @Test(expected = IllegalStateException.class)
    public void beginTransaction_whenAlreadyBegin() {
        TransactionContext transactionContext = hz.newTransactionContext();
        transactionContext.beginTransaction();

        transactionContext.beginTransaction();
    }

    @Test(expected = IllegalStateException.class)
    public void beginTransaction_whenAlreadyRolledBack() {
        TransactionContext transactionContext = hz.newTransactionContext();
        transactionContext.rollbackTransaction();

        transactionContext.beginTransaction();
    }

    @Test(expected = TransactionNotActiveException.class)
    public void beginTransaction_whenAlreadyCommitted() {
        TransactionContext transactionContext = hz.newTransactionContext();
        transactionContext.commitTransaction();

        transactionContext.beginTransaction();
    }


    // ======== rollbackTransaction

    @Test(expected = TransactionNotActiveException.class)
    public void rollbackTransaction_whenAlreadyCommitted() {
        TransactionContext transactionContext = hz.newTransactionContext();
        transactionContext.commitTransaction();

        transactionContext.rollbackTransaction();
    }

    @Test(expected = IllegalStateException.class)
    public void rollbackTransaction_whenAlreadyRolledBack() {
        TransactionContext transactionContext = hz.newTransactionContext();
        transactionContext.rollbackTransaction();

        transactionContext.rollbackTransaction();
    }

    // ======== commitTransaction

    @Test(expected = TransactionNotActiveException.class)
    public void commitTransaction_whenAlreadyCommitted() {
        TransactionContext transactionContext = hz.newTransactionContext();
        transactionContext.commitTransaction();

        transactionContext.commitTransaction();
    }

    @Test(expected = IllegalStateException.class)
    public void commitTransaction_whenAlreadyRolledBack() {
        TransactionContext transactionContext = hz.newTransactionContext();
        transactionContext.rollbackTransaction();

        transactionContext.commitTransaction();
    }
}
