package com.hazelcast.jca;


import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * This test class is to ensure a contract of {@link com.hazelcast.jca.HazelcastConnection} is aligned with
 * {@link com.hazelcast.core.HazelcastInstance}
 *
 */
@Category(QuickTest.class)
public class HazecastConnectionContractTest {

    static HazelcastConnectionImpl connection;

    @BeforeClass
    public static void setup() {
       connection = new HazelcastConnectionImpl(null,null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAddDistributedObjectListener() {
        connection.addDistributedObjectListener(null);
   }

    @Test(expected = UnsupportedOperationException.class)
    public void testRemoveDistributedObjectListener() {
        connection.removeDistributedObjectListener(null);
    }
    @Test(expected = UnsupportedOperationException.class)
    public void testGetClientService() {
        connection.getClientService();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetLoggingService() {
        connection.getLoggingService();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetLifecycleService() {
        connection.getLifecycleService();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDistributedObject() {
        connection.getDistributedObject(null,null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDistributedObjectAsString() {
        connection.getDistributedObject(null,"");
    }


    @Test(expected = UnsupportedOperationException.class)
    public void testGetUserContext() {
        connection.getUserContext();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testShutdown() {
        connection.shutdown();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetConfig() {
        connection.getConfig();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetPartitionService() {
        connection.getPartitionService();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDistributedObjects() {
        connection.getDistributedObjects();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetCluster() {
        connection.getCluster();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetLocalEndpoint() {
        connection.getLocalEndpoint();
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