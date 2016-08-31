package com.hazelcast.spring.transaction;


import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.spring.CustomSpringJUnit4ClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionalTaskContext;

import org.junit.*;
import org.junit.Assert;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.transaction.NoTransactionException;
import org.springframework.transaction.TransactionSuspensionNotSupportedException;
import org.springframework.transaction.annotation.Transactional;

@RunWith(CustomSpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"transaction-applicationContext-hazelcast.xml"})
@Category(QuickTest.class)
public class TestSpringManagedHazelcastTransaction {

    @BeforeClass
    @AfterClass
    public static void shutdownHazecast() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Before
    public void clearHazecastData() {
        instance.getMap("dummyObjectMap").clear();
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Autowired
    ServiceBeanWithTransactionalContext service;

    @Autowired
    TransactionalTaskContext transactionalContext;

    @Autowired
    HazelcastInstance instance;


    /**
     * Tests that transactionalContext cannot be accessed when there is no transaction.
     */
    @Test
    public void noTransactionContextWhenNoTransaction() {
        // given
        expectedException.expect(NoTransactionException.class);

        // when
        transactionalContext.getMap("magic");
    }

    /**
     * Tests that transactionContext is accessible when there is a transaction.
     */
    @Test
    @Transactional
    public void noExceptionWhenTransaction() {
        // when
        TransactionalMap<Object, Object> magic = transactionalContext.getMap("magic");

        // then
        Assert.assertNotNull(magic);
    }

    /**
     * Tests that transaction will be committed if everything works fine.
     */
    @Test
    public void transactionalServiceBeanInvocation_commit() {
        // when
        service.put(new DummyObject(1L, "magic"));

        // then
        Assert.assertEquals(1L, instance.getMap("dummyObjectMap").size());
    }

    /**
     * Tests that transaction will be rollbacked if there is an exception.
     */
    @Test
    public void transactionalServiceBeanInvocation_rollback() {
        // when
        try {
            service.putWithException(new DummyObject(1L, "magic"));
        } catch (DummyException ex) {
            // then
            Assert.assertEquals(0L, instance.getMap("dummyObjectMap").size());
        }
    }

    /**
     * Tests that if propagation is set to {@link org.springframework.transaction.annotation.Propagation#REQUIRED REQUIRED},
     * then the same transaction will be used.
     */
    @Test
    public void transactionalServiceBeanInvocation_nestedWithPropagationRequired() {
        // when
        service.putUsingOtherBean_sameTransaction(new DummyObject(1L, "magic"));

        // then
        Assert.assertEquals(1L, instance.getMap("dummyObjectMap").size());
    }

    /**
     * Tests that if propagation is set to {@link org.springframework.transaction.annotation.Propagation#REQUIRED REQUIRED},
     * and exception happens in nested @Transactional code then all changes in nested and contained @Transactional blocks
     * will be rolled back.
     */
    @Test
    public void transactionalServiceBeanInvocation_nestedWithPropagationRequired_exceptionInNested() {
        // when
        try {
            service.putUsingOtherBean_withExceptionInOtherBean_sameTransaction(
                    new DummyObject(1L, "magic"),
                    new DummyObject(2L, "magic2"));
        } catch(DummyException e) {
            // then
            Assert.assertEquals(0L, instance.getMap("dummyObjectMap").size());
        }
    }

    /**
     * Tests that if propagation is set to {@link org.springframework.transaction.annotation.Propagation#REQUIRED REQUIRED},
     * and exception happens in contained @Transactional code then all changes in nested and contained @Transactional blocks
     * will be rolled back.
     */
    @Test
    public void transactionalServiceBeanInvocation_nestedWithPropagationRequired_exceptionInContaining() {
        // when
        try {
            service.putUsingOtherBean_withExceptionInThisBean_sameTransaction(
                    new DummyObject(1L, "magic"),
                    new DummyObject(2L, "magic2"));
        } catch(DummyException e) {
            // then
            Assert.assertEquals(0L, instance.getMap("dummyObjectMap").size());
        }
    }

    /**
     * Tests that if propagation is set to {@link org.springframework.transaction.annotation.Propagation#REQUIRES_NEW REQUIRES_NEW},
     * then an exception will be thrown, since Hazelcast doesn't support nested transaction, so {@link HazelcastTransactionManager}
     * doesn't support transaction suspension.
     */
    @Test
    public void transactionalServiceBeanInvocation_nestedWithPropagationRequiresNew() {
        // given
        expectedException.expect(TransactionSuspensionNotSupportedException.class);

        // when
        service.putUsingOtherBean_newTransaction(new DummyObject(1L, "magic"));
    }
}
