/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.tx;

import com.hazelcast.config.Config;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder.EntryObject;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.SampleTestObjects;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalMap;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category(NightlyTest.class)
public class MapTransactionRegressionTest extends HazelcastTestSupport {

    private final TransactionOptions options = new TransactionOptions()
            .setTransactionType(TransactionOptions.TransactionType.TWO_PHASE);

    @Test
    public void test_Issue615_keySet() throws TransactionException {
        Config config = getConfig();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance h1 = factory.newHazelcastInstance(config);
        final HazelcastInstance h2 = factory.newHazelcastInstance(config);
        final IMap map = h2.getMap("default");
        map.put("1", "1");
        map.put("2", "2");

        boolean b = h1.executeTransaction(options, (context) -> {
            final TransactionalMap<Object, Object> txMap = context.getMap("default");
            txMap.put("3", "3");

            assertEquals(3, txMap.keySet().size());

            map.put("4", "4");

            assertEquals(4, txMap.keySet().size());

            txMap.remove("1");

            assertEquals(3, txMap.keySet().size());

            map.remove("2");

            assertEquals(2, txMap.keySet().size());
            assertEquals(2, txMap.size());

            return true;
        });

        assertEquals(2, map.keySet().size());

        // raise an exception and rollback changes.
        try {
            boolean b2 = h1.executeTransaction(options, (context) -> {
                final TransactionalMap<Object, Object> txMap = context.getMap("default");

                txMap.put("5", "5");

                assertEquals(3, txMap.keySet().size());
                assertEquals(2, map.keySet().size());

                throw new DummyUncheckedHazelcastTestException();
            });
        } catch (Exception e) {
            if (!(e instanceof DummyUncheckedHazelcastTestException)) {
                throw new RuntimeException(e);
            }
        }
        assertEquals(2, map.keySet().size());
        h1.shutdown();
        h2.shutdown();
    }

    @Test
    public void test_Issue615_KeysetWithPredicate() throws TransactionException {
        Config config = getConfig();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance h1 = factory.newHazelcastInstance(config);
        final HazelcastInstance h2 = factory.newHazelcastInstance(config);
        final IMap map = h2.getMap("default");
        final SampleTestObjects.Employee employee1 = new SampleTestObjects.Employee("abc-123-xvz", 34, true, 10D);
        final SampleTestObjects.Employee employee2 = new SampleTestObjects.Employee("abc-1xvz", 4, true, 7D);
        final SampleTestObjects.Employee employee3 = new SampleTestObjects.Employee("abc-1xasda...vz", 7, true, 1D);
        final SampleTestObjects.Employee employee4 = new SampleTestObjects.Employee("abc-1asdsaxvz", 2, true, 2D);

        map.put(1, employee1);


        try {
            h1.executeTransaction(options, (context) -> {
                final TransactionalMap<Object, Object> txMap = context.getMap("default");

                assertEquals(0, txMap.keySet(Predicates.sql("age <= 10")).size());
                //put
                txMap.put(2, employee2);
                Set keys = txMap.keySet(Predicates.sql("age <= 10"));
                Iterator iterator = keys.iterator();

                assertEquals(1, keys.size());

                while (iterator.hasNext()) {
                    assertEquals(2, ((Integer) iterator.next()).intValue());
                }

                txMap.put(3, employee3);
                txMap.put(4, employee4);

                keys = txMap.keySet(Predicates.sql("age <= 10"));
                assertEquals(3, keys.size());

                // force rollback.
                throw new DummyUncheckedHazelcastTestException();
            });
        } catch (Exception e) {
            if (!(e instanceof DummyUncheckedHazelcastTestException)) {
                throw new RuntimeException(e);
            }
        }
        assertEquals(1, map.size());
        assertEquals(1, map.keySet().size());
        assertEquals(0, map.keySet(Predicates.sql("age <= 10")).size());

        h1.shutdown();
        h2.shutdown();
    }

    @Test
    public void test_Issue615_KeysetPredicates() throws TransactionException {
        final String MAP_NAME = "defaultMap";
        final Config config = getConfig();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance h1 = factory.newHazelcastInstance(config);
        final HazelcastInstance h2 = factory.newHazelcastInstance(config);
        final IMap map = h2.getMap(MAP_NAME);
        final SampleTestObjects.Employee employee1 = new SampleTestObjects.Employee("abc-123-xvz", 34, true, 10D);
        final SampleTestObjects.Employee employee2 = new SampleTestObjects.Employee("abc-1xvz", 4, true, 7D);
        final SampleTestObjects.Employee employee3 = new SampleTestObjects.Employee("abc-1xasda...vz", 7, true, 1D);
        final SampleTestObjects.Employee employee4 = new SampleTestObjects.Employee("abc-1asdsaxvz", 2, true, 2D);

        map.put(employee1, employee1);

        final TransactionContext context = h1.newTransactionContext();
        context.beginTransaction();

        final TransactionalMap<Object, Object> txMap = context.getMap(MAP_NAME);

        assertNull(txMap.put(employee2, employee2));

        assertEquals(2, txMap.size());
        assertEquals(2, txMap.keySet().size());
        assertEquals(1, txMap.keySet(Predicates.sql("age = 34")).size());

        context.commitTransaction();


        assertEquals(2, map.size());

        h1.shutdown();
        h2.shutdown();
    }

    @Test
    public void test_Issue615_values() throws TransactionException {
        Config config = getConfig();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance h1 = factory.newHazelcastInstance(config);
        final HazelcastInstance h2 = factory.newHazelcastInstance(config);
        final IMap map2 = h2.getMap("default");
        map2.put("1", "1");
        map2.put("2", "2");


        boolean b = h1.executeTransaction(options, (context) -> {
                final TransactionalMap<Object, Object> txMap = context.getMap("default");
                txMap.put("3", "3");
                assertEquals(3, txMap.values().size());
                map2.put("4", "4");
                assertEquals(4, txMap.values().size());
                txMap.remove("1");
                assertEquals(3, txMap.values().size());
                map2.remove("2");
                assertEquals(2, txMap.values().size());
                assertEquals(2, txMap.size());
                txMap.put("12", "32");
                assertEquals(2, map2.values().size());
                return true;
        });

        assertEquals(3, map2.values().size());

        h1.shutdown();
        h2.shutdown();
    }

    @Test
    public void test_Issue615_ValuesWithPredicate() throws TransactionException {
        Config config = getConfig();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance h1 = factory.newHazelcastInstance(config);
        final HazelcastInstance h2 = factory.newHazelcastInstance(config);
        final IMap map2 = h2.getMap("default");
        final SampleTestObjects.Employee emp1 = new SampleTestObjects.Employee("abc-123-xvz", 34, true, 10D);
        map2.put(1, emp1);
        final SampleTestObjects.Employee emp2 = new SampleTestObjects.Employee("xvz", 4, true, 10D);

        boolean b = h1.executeTransaction(options, (context) -> {
            final TransactionalMap<Object, Object> txMap = context.getMap("default");
            assertEquals(0, txMap.values(Predicates.sql("age <= 10")).size());
            txMap.put(2, emp2);
            Collection coll = txMap.values(Predicates.sql("age <= 10"));
            Iterator<Object> iterator = coll.iterator();
            while (iterator.hasNext()) {
                final SampleTestObjects.Employee e = (SampleTestObjects.Employee) iterator.next();
                assertEquals(emp2, e);
            }
            coll = txMap.values(Predicates.sql("age > 30 "));
            iterator = coll.iterator();
            while (iterator.hasNext()) {
                final SampleTestObjects.Employee e = (SampleTestObjects.Employee) iterator.next();
                assertEquals(emp1, e);
            }
            txMap.remove(2);
            coll = txMap.values(Predicates.sql("age <= 10 "));
            assertEquals(0, coll.size());
            return true;
        });
        assertEquals(0, map2.values(Predicates.sql("age <= 10")).size());
        assertEquals(1, map2.values(Predicates.sql("age = 34")).size());
        h1.shutdown();
        h2.shutdown();
    }

    //unfortunately the bug can't be detected by a unit test since the exception is thrown in a background thread (and logged)
    @Test
    public void test_Issue1056s() throws InterruptedException {
        Config config = getConfig();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        final HazelcastInstance instance2 = factory.newHazelcastInstance(config);

        final CountDownLatch latch = new CountDownLatch(1);
        final Thread t = new Thread(() -> {
            TransactionContext ctx = instance2.newTransactionContext();
            ctx.beginTransaction();
            TransactionalMap<Integer, Integer> txnMap = ctx.getMap("test");
            latch.countDown();
            txnMap.delete(1);
            ctx.commitTransaction();
        });

        t.start();

        TransactionContext ctx = instance2.newTransactionContext();
        ctx.beginTransaction();
        TransactionalMap<Integer, Integer> txnMap = ctx.getMap("test");
        txnMap.delete(1);
        latch.await();
        ctx.commitTransaction();
        t.join();
    }


    @Test
    public void test_Issue1076() {
        Config config = getConfig();
        final HazelcastInstance inst = createHazelcastInstance(config);

        IMap map = inst.getMap("default");

        EntryListener<String, Integer> l = new EntryAdapter<String, Integer>() {
        };

        EntryObject e = Predicates.newPredicateBuilder().getEntryObject();
        Predicate<String, Integer> p = e.get("this").equal(1);
        map.addEntryListener(l, p, null, false);

        for (Integer i = 0; i < 100; i++) {
            TransactionContext context = inst.newTransactionContext();
            context.beginTransaction();
            TransactionalMap<String, Integer> txnMap = context.getMap("default");
            txnMap.remove(i.toString());
            context.commitTransaction();
        }
        assertEquals(0, map.size());

        inst.shutdown();
    }
}
