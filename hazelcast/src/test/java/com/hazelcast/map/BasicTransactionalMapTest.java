package com.hazelcast.map;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionOptions;
import org.junit.Test;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertTrue;

public class BasicTransactionalMapTest {

    @Test
    public void testContainsValue(){
        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();

        TransactionOptions options = new TransactionOptions().setTransactionType(
                TransactionOptions.TransactionType.ONE_PHASE);

        TransactionContext context = hazelcastInstance.newTransactionContext(options);
        context.beginTransaction();
        TransactionalMap<Integer,Integer> map = context.getMap("mymap");
        map.put(1, 1);
        map.put(2, 2);
        context.commitTransaction();
        context.beginTransaction();
        assertTrue(map.containsValue(1));
    }

    @Test
    public void testDoesNotContainValue(){
        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();

        TransactionOptions options = new TransactionOptions().setTransactionType(
                TransactionOptions.TransactionType.ONE_PHASE);

        TransactionContext context = hazelcastInstance.newTransactionContext(options);
        context.beginTransaction();
        TransactionalMap<Integer,Integer> map = context.getMap("mymap");
        map.put(1, 1);
        map.put(2, 2);
        map.put(3, 3);
        map.put(4, 4);
        context.commitTransaction();
        context.beginTransaction();
        assertFalse(map.containsValue(5));
    }

    @Test
    public void testDoesNotContainValueAfterDelete(){
        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();

        TransactionOptions options = new TransactionOptions().setTransactionType(
                TransactionOptions.TransactionType.ONE_PHASE);

        TransactionContext context = hazelcastInstance.newTransactionContext(options);
        context.beginTransaction();
        TransactionalMap<Integer,Integer> map = context.getMap("mymap");
        map.put(1, 1);
        map.put(2, 2);
        map.put(3, 3);
        map.put(4, 4);
        context.commitTransaction();
        context.beginTransaction();
        map.delete(3);
        context.commitTransaction();
        context.beginTransaction();
        assertFalse(map.containsValue(5));
    }
}
