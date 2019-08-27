/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spring.transaction;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.transaction.TransactionalMap;
import com.hazelcast.spring.CustomSpringJUnit4ClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionalTaskContext;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.transaction.NoTransactionException;
import org.springframework.transaction.annotation.Transactional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(CustomSpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"transaction-applicationContext-hazelcast.xml"})
@Category(QuickTest.class)
public class TestSpringManagedHazelcastTransaction {

    @BeforeClass
    @AfterClass
    public static void cleanup() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Before
    public void setUp() {
        // clear all items from the dummyObjectMap used to test transactional object insertion
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
        assertNotNull(magic);
    }

    /**
     * Tests that transaction will be committed if everything works fine.
     */
    @Test
    public void transactionalServiceBeanInvocation_commit() {
        // when
        service.put(new DummyObject(1L, "magic"));

        // then
        assertEquals(1L, instance.getMap("dummyObjectMap").size());
    }

    /**
     * Tests that transaction will be rollbacked if there is an exception.
     */
    @Test
    public void transactionalServiceBeanInvocation_rollback() {
        // when
        RuntimeException expectedEx = null;

        try {
            service.putWithException(new DummyObject(1L, "magic"));
        } catch (RuntimeException ex) {
            expectedEx = ex;
        } finally {
            // then
            assertNotNull(expectedEx);
            assertEquals(0L, instance.getMap("dummyObjectMap").size());
        }
    }

    /**
     * Tests that transaction will be rollbacked when putting one object each
     * via two beans, one nested within the other,
     * if there is an exception in the nested bean, but no exception in our own bean.
     */
    @Test
    public void transactionalServiceBeanInvocation_withNestedBeanThrowingException_rollback() {
        // when
        RuntimeException expectedEx = null;

        try {
            service.putUsingSameBean_thenOtherBeanThrowingException_sameTransaction(
                    new DummyObject(1L, "magic"), new DummyObject(2L, "magic2"));
        } catch (RuntimeException ex) {
            expectedEx = ex;
        } finally {
            // then
            assertNotNull(expectedEx);
            assertEquals(0L, instance.getMap("dummyObjectMap").size());
        }
    }

    /**
     * Tests that transaction will be rollbacked when putting one object each
     * via two beans, one nested within the other,
     * if there is an exception in our own bean, but no exception in the other bean.
     */
    @Test
    public void transactionalServiceBeanInvocation_withOwnBeanThrowingException_rollback() {
        // when
        RuntimeException expectedEx = null;

        try {
            service.putUsingOtherBean_thenSameBeanThrowingException_sameTransaction(
                    new DummyObject(1L, "magic"), new DummyObject(2L, "magic2"));
        } catch (RuntimeException ex) {
            expectedEx = ex;
        } finally {
            // then
            assertNotNull(expectedEx);
            assertEquals(0L, instance.getMap("dummyObjectMap").size());
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
        assertEquals(1L, instance.getMap("dummyObjectMap").size());
    }

    /**
     * Tests that if propagation is set to {@link org.springframework.transaction.annotation.Propagation#REQUIRES_NEW REQUIRES_NEW},
     * then nested transaction will be created.
     */
    @Test
    public void transactionalServiceBeanInvocation_nestedWithPropagationRequiresNew() {
        // given
        DummyObject dummyObject1 = new DummyObject(1L, "magic1");
        DummyObject dummyObject2 = new DummyObject(2L, "magic2");
        IMap<Object, Object> dummyObjectMap = instance.getMap("dummyObjectMap");

        // when
        boolean result = service.putUsingOtherBean_newTransaction(dummyObject1, dummyObject2);

        // then
        assertTrue("No data changes within nested transaction .", result);
        assertEquals("Both transactions should have data changes.", 2L, dummyObjectMap.size());
        assertTrue("No data changes within parent transaction.", dummyObjectMap.containsKey(1L));
        assertEquals("Invalid data within parent transaction.", dummyObject1, dummyObjectMap.get(1L));
        assertTrue("No data changes within nested transaction.", dummyObjectMap.containsKey(2L));
        assertEquals("Invalid data within nested transaction.", dummyObject2, dummyObjectMap.get(2L));
    }

}
