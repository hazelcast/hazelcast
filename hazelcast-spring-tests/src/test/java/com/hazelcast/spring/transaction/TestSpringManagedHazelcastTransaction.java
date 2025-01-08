/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.spring.CustomSpringExtension;
import com.hazelcast.transaction.TransactionalMap;
import com.hazelcast.transaction.TransactionalTaskContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.transaction.NoTransactionException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionSuspensionNotSupportedException;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.support.TransactionTemplate;

import static com.hazelcast.spring.transaction.ServiceBeanWithTransactionalContext.TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.springframework.test.annotation.DirtiesContext.MethodMode.AFTER_METHOD;

@ExtendWith({SpringExtension.class, CustomSpringExtension.class})
@ContextConfiguration(locations = {"transaction-applicationContext-hazelcast.xml"})
class TestSpringManagedHazelcastTransaction {

    @BeforeAll
    @AfterAll
    public static void cleanup() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @BeforeEach
    public void setUp() {
        // clear all items from the dummyObjectMap used to test transactional object insertion
        instance.getMap("dummyObjectMap").clear();
    }


    @Autowired
    ServiceBeanWithTransactionalContext service;

    @Autowired
    TransactionalTaskContext transactionalContext;

    @Autowired
    HazelcastInstance instance;

    @Autowired
    HazelcastTransactionManager transactionManager;

    @Autowired
    PlatformTransactionManager platformTransactionManager;

    /**
     * Tests that transactionalContext cannot be accessed when there is no transaction.
     */
    @Test
    void noTransactionContextWhenNoTransaction() {
        assertThrows(NoTransactionException.class, () -> transactionalContext.getMap("magic"));
    }

    /**
     * Tests that transactionContext is accessible when there is a transaction.
     */
    @Test
    void noExceptionWhenTransaction() {
        // CustomSpringExtension runs tests in separate thread, so we cannot use
        // @Transactional test method.
        new TransactionTemplate(platformTransactionManager).execute((status) -> {
            // when
            TransactionalMap<Object, Object> magic = transactionalContext.getMap("magic");

            // then
            assertNotNull(magic);
            return null;
        });
    }

    /**
     * Tests that slow transaction will commit when no timeout value is set
     * neither for the transaction nor in the transaction manager
     */
    @Test
    void noExceptionWithoutTimeoutValue() {
        // when
        service.putWithDelay(new DummyObject(1L, "magic"), TIMEOUT + 1);

        // then
        assertEquals(1L, instance.getMap("dummyObjectMap").size());
    }

    /**
     * Tests that transaction times out when its duration exceeds the value configured for the transaction
     */
    @Test
    void transactionTimedOutExceptionWhenTimeoutValueIsSetForTransaction() {
        DummyObject dummyObject = new DummyObject(1L, "magic");
        TransactionSystemException exception = assertThrows(TransactionSystemException.class,
                () -> service.putWithDelay_transactionTimeoutValue(dummyObject, TIMEOUT + 1));
        assertThat(exception).hasRootCauseMessage("Transaction is timed-out!");
    }

    /**
     * Tests that transaction times out when its duration exceeds the default value
     * configured in the transaction manager AND no value is configured for the transaction
     */
    @Test
    @DirtiesContext(methodMode = AFTER_METHOD)
    void transactionTimedOutExceptionWhenTimeoutValueIsSetInTransactionManager() {
        // given
        transactionManager.setDefaultTimeout(TIMEOUT);
        DummyObject dummyObject = new DummyObject(1L, "magic");
        TransactionSystemException exception = assertThrows(TransactionSystemException.class,
                () -> service.putWithDelay(dummyObject, TIMEOUT + 1));
        assertThat(exception).hasRootCauseMessage("Transaction is timed-out!");
    }

    /**
     * Tests that timeout value of the transaction takes precedence over the default value in the transaction manager
     */
    @Test
    @DirtiesContext(methodMode = AFTER_METHOD)
    void transactionTimeoutTakesPrecedenceOverTransactionManagerDefaultTimeout() {
        // given
        transactionManager.setDefaultTimeout(TIMEOUT + 2);

        // when
        DummyObject dummyObject = new DummyObject(1L, "magic");
        TransactionSystemException exception = assertThrows(TransactionSystemException.class,
                () -> service.putWithDelay_transactionTimeoutValue(dummyObject, TIMEOUT + 1));

        assertThat(exception).hasRootCauseMessage("Transaction is timed-out!");
    }

    /**
     * Tests that transaction will be committed if everything works fine.
     */
    @Test
    void transactionalServiceBeanInvocation_commit() {
        // when
        service.put(new DummyObject(1L, "magic"));

        // then
        assertEquals(1L, instance.getMap("dummyObjectMap").size());
    }

    /**
     * Tests that transaction will be rolled back if there is an exception.
     */
    @Test
    void transactionalServiceBeanInvocation_rollback() {
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
     * Tests that transaction will be rolled back when putting one object each
     * via two beans, one nested within the other,
     * if there is an exception in the nested bean, but no exception in our own bean.
     */
    @Test
    void transactionalServiceBeanInvocation_withNestedBeanThrowingException_rollback() {
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
     * Tests that transaction will be rolled back when putting one object each
     * via two beans, one nested within the other,
     * if there is an exception in our own bean, but no exception in the other bean.
     */
    @Test
    void transactionalServiceBeanInvocation_withOwnBeanThrowingException_rollback() {
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
    void transactionalServiceBeanInvocation_nestedWithPropagationRequired() {
        // when
        service.putUsingOtherBean_sameTransaction(new DummyObject(1L, "magic"));

        // then
        assertEquals(1L, instance.getMap("dummyObjectMap").size());
    }

    /**
     * Tests that if propagation is set to {@link org.springframework.transaction.annotation.Propagation#REQUIRES_NEW REQUIRES_NEW},
     * then an exception will be thrown, since Hazelcast doesn't support nested transaction, so {@link HazelcastTransactionManager}
     * doesn't support transaction suspension.
     */
    @Test
    void transactionalServiceBeanInvocation_nestedWithPropagationRequiresNew() {
        DummyObject dummyObject = new DummyObject(1L, "magic");
        assertThrows(TransactionSuspensionNotSupportedException.class,
                () -> service.putUsingOtherBean_newTransaction(dummyObject));
    }
}
