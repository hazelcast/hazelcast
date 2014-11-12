package com.hazelcast.jca;


import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * This test class is to ensure that lifecycle and transaction methods are not exposed in {@link com.hazelcast.jca.HazelcastConnection}
 */
@Category(QuickTest.class)
public class HazecastConnectionContractTest {

    static HazelcastConnectionImpl connection;

    @BeforeClass
    public static void setup() {
       connection = new HazelcastConnectionImpl(null,null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetLifecycleService() {
        connection.getLifecycleService();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testShutdown() {
        connection.shutdown();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testExecuteTransaction() {
        connection.executeTransaction(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testExecuteTransactionTwoArgs() {
        connection.executeTransaction(null,null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testNewTransactionContext() {
        connection.newTransactionContext();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testNewTransactionContext2() {
        connection.newTransactionContext(null);
    }


}