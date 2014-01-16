/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.txn;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.*;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.annotation.SerialTest;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Calendar;
import java.util.Currency;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * @author ali 6/6/13
 */
@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(SerialTest.class)
public class ClientTxnTest {

    @After
    public void destroy() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testTxnRollback() throws Exception {
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance();
        final ClientConfig config = new ClientConfig();
        config.setRedoOperation(true);
        HazelcastInstance client = HazelcastClient.newHazelcastClient(config);
        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance();

        final TransactionContext context = client.newTransactionContext();

        CountDownLatch latch = new CountDownLatch(1);
        try {
            context.beginTransaction();
            assertNotNull(context.getTxnId());
            final TransactionalQueue queue = context.getQueue("testTxnRollback");
            queue.offer("item");

            instance1.getLifecycleService().shutdown();

            context.commitTransaction();
            fail("commit should throw exception!!!");
        } catch (Exception e) {
            context.rollbackTransaction();
            latch.countDown();
        }


        assertTrue(latch.await(10, TimeUnit.SECONDS));

        final IQueue<Object> q = client.getQueue("testTxnRollback");
        assertNull(q.poll());
        assertEquals(0, q.size());
    }

    @Test
    public void testDeadLockFromClientInstance() {

        HazelcastInstance hz = Hazelcast.newHazelcastInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setSmartRouting(false);
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        // / data mock object for test

        CBAuthorisation cb = new CBAuthorisation();
        cb.setEntrymode("PIN");
        cb.setCardNumber("19930923094039");
        cb.setAmount(15000);
        cb.setDate(Calendar.getInstance());
        cb.setCurrency(Currency.getInstance("EUR"));

        try {
            TransactionContext context = client.newTransactionContext();

            context.beginTransaction();

            TransactionalMap<String, Object> mapTransaction = context.getMap("mapChildTransaction");
            // init data
            mapTransaction.put("3", cb);

            cb.setAmount(12000);
            mapTransaction.set("3", cb);
            cb.setAmount(10000);
            mapTransaction.set("3", cb);
            cb.setAmount(900);
            mapTransaction.set("3", cb);
            cb.setAmount(800);
            mapTransaction.set("3", cb);
            cb.setAmount(700);
            mapTransaction.set("3", cb);
            context.commitTransaction();

        } catch (TransactionException e) {
            fail(e.getMessage());
        }

    }

    public static class CBAuthorisation implements Serializable {
        private String entrymode;
        private String cardNumber;
        private int amount;
        private Calendar date;
        private Currency currency;

        public void setEntrymode(String entrymode) {
            this.entrymode = entrymode;
        }

        public String getEntrymode() {
            return entrymode;
        }

        public void setCardNumber(String cardNumber) {
            this.cardNumber = cardNumber;
        }

        public String getCardNumber() {
            return cardNumber;
        }

        public void setAmount(int amount) {
            this.amount = amount;
        }

        public int getAmount() {
            return amount;
        }

        public void setDate(Calendar date) {
            this.date = date;
        }

        public Calendar getDate() {
            return date;
        }

        public void setCurrency(Currency currency) {
            this.currency = currency;
        }

        public Currency getCurrency() {
            return currency;
        }
    }

}
