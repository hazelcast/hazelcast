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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapStoreAdapter;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.query.EntryObject;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.query.SampleObjects;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ProblematicTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionNotActiveException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalTask;
import com.hazelcast.transaction.TransactionalTaskContext;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class MapTransactionTest extends HazelcastTestSupport {

    private final TransactionOptions options = new TransactionOptions().setTransactionType(TransactionOptions.TransactionType.TWO_PHASE);

    //unfortunately the bug can't be detected by a unit test since the exception is thrown in a background thread (and logged)
    @Test
    public void issue_1056s() throws InterruptedException {
        HazelcastInstance instance = Hazelcast.newHazelcastInstance();
        final HazelcastInstance instance2 = Hazelcast.newHazelcastInstance();

        final CountDownLatch latch = new CountDownLatch(1);
        final Thread t = new Thread() {
            @Override
            public void run() {
                TransactionContext ctx = instance2.newTransactionContext();
                ctx.beginTransaction();
                TransactionalMap<Integer, Integer> txnMap = ctx.getMap("test");
                latch.countDown();
                txnMap.delete(1);
                ctx.commitTransaction();
            }
        };

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
    public void testCommitOrder() throws TransactionException {
        Config config = new Config();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(4);
        final HazelcastInstance h1 = factory.newHazelcastInstance(config);
        final HazelcastInstance h2 = factory.newHazelcastInstance(config);
        final HazelcastInstance h3 = factory.newHazelcastInstance(config);
        final HazelcastInstance h4 = factory.newHazelcastInstance(config);

        boolean b = h1.executeTransaction(options, new TransactionalTask<Boolean>() {
            public Boolean execute(TransactionalTaskContext context) throws TransactionException {
                final TransactionalMap<Object, Object> txMap = context.getMap("default");
                txMap.put("1", "value1");
                assertEquals("value1", txMap.put("1", "value2"));
                assertEquals("value2", txMap.put("1", "value3"));
                assertEquals("value3", txMap.put("1", "value4"));
                assertEquals("value4", txMap.put("1", "value5"));
                assertEquals("value5", txMap.put("1", "value6"));
                assertEquals(1, txMap.size());
                return true;
            }
        });
        assertEquals("value6", h4.getMap("default").get("1"));
    }

    @Test
    public void testTxnCommit() throws TransactionException {
        Config config = new Config();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance h1 = factory.newHazelcastInstance(config);
        final HazelcastInstance h2 = factory.newHazelcastInstance(config);
        final String map1 = "map1";
        final String map2 = "map2";
        final String key = "1";

        boolean b = h1.executeTransaction(options, new TransactionalTask<Boolean>() {
            public Boolean execute(TransactionalTaskContext context) throws TransactionException {
                final TransactionalMap<Object, Object> txMap1 = context.getMap(map1);
                final TransactionalMap<Object, Object> txMap2 = context.getMap(map2);

                txMap1.put(key, "value");
                assertEquals("value", txMap1.put(key, "value1"));
                assertEquals("value1", txMap1.get(key));

                txMap2.put(key, "value");
                assertEquals("value", txMap2.put(key, "value2"));
                assertEquals("value2", txMap2.get(key));

                assertEquals(true, txMap1.containsKey(key));
                assertEquals(true, txMap2.containsKey(key));

                assertNull(h2.getMap(map1).get(key));
                assertNull(h2.getMap(map2).get(key));
                return true;
            }
        });
        assertTrue(b);

        assertEquals("value1", h1.getMap(map1).get(key));
        assertEquals("value1", h2.getMap(map1).get(key));

        assertEquals("value2", h1.getMap(map2).get(key));
        assertEquals("value2", h2.getMap(map2).get(key));

        assertFalse(h1.getMap(map1).isLocked(key));
        assertFalse(h1.getMap(map2).isLocked(key));
    }

    @Test
    public void testTxnBackupDies() throws TransactionException, InterruptedException {
        Config config = new Config();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance h1 = factory.newHazelcastInstance(config);
        final HazelcastInstance h2 = factory.newHazelcastInstance(config);
        final IMap map1 = h1.getMap("default");
        final int size = 100;
        final CountDownLatch latch = new CountDownLatch(size + 1);
        final CountDownLatch latch2 = new CountDownLatch(1);

        Runnable runnable = new Runnable() {
            public void run() {
                try {
                    final int oneThird = size / 3;
                    final int threshold = new Random().nextInt(oneThird) + oneThird;
                    h1.executeTransaction(options, new TransactionalTask<Boolean>() {
                        public Boolean execute(TransactionalTaskContext context) throws TransactionException {
                            final TransactionalMap<Object, Object> txMap = context.getMap("default");
                            for (int i = 0; i < size; i++) {
                                if (i == threshold) {
                                    latch2.countDown();
                                }
                                txMap.put(i, i);
                                try {
                                    Thread.sleep(100);
                                } catch (InterruptedException ignored) {
                                }
                                latch.countDown();
                            }
                            return true;
                        }
                    });
                    fail();
                } catch (Exception ignored) {
                }
                latch.countDown();
            }
        };
        new Thread(runnable).start();
        assertTrue(latch2.await(20, TimeUnit.SECONDS));
        h2.shutdown();

        assertTrue(latch.await(60, TimeUnit.SECONDS));
        for (int i = 0; i < size; i++) {
            assertNull(map1.get(i));
        }
    }

    @Test
    public void testTxnOwnerDies() throws TransactionException, InterruptedException {
        Config config = new Config();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        final HazelcastInstance h1 = factory.newHazelcastInstance(config);
        final HazelcastInstance h2 = factory.newHazelcastInstance(config);
        final HazelcastInstance h3 = factory.newHazelcastInstance(config);
        final IMap map1 = h1.getMap("default");
        final int size = 50;
        final AtomicBoolean result = new AtomicBoolean(false);

        Runnable runnable = new Runnable() {
            public void run() {
                try {
                    boolean b = h1.executeTransaction(options, new TransactionalTask<Boolean>() {
                        public Boolean execute(TransactionalTaskContext context) throws TransactionException {
                            final TransactionalMap<Object, Object> txMap = context.getMap("default");
                            for (int i = 0; i < size; i++) {
                                txMap.put(i, i);
                                sleepSeconds(1);
                            }
                            return true;
                        }
                    });
                    result.set(b);
                } catch (HazelcastInstanceNotActiveException ignored) {
                } catch (TransactionException ignored) {
                }
            }
        };

        Thread thread = new Thread(runnable);
        thread.start();
        sleepSeconds(1);

        h1.shutdown();
        // wait till thread finishes.
        thread.join(30 * 1000);

        assertFalse(result.get());
        final IMap map2 = h2.getMap("default");
        for (int i = 0; i < size; i++) {
            assertNull(map2.get(i));
        }
    }


    @Test
    public void testTxnSet() throws TransactionException {
        Config config = new Config();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance h1 = factory.newHazelcastInstance(config);
        final HazelcastInstance h2 = factory.newHazelcastInstance(config);
        final IMap map2 = h2.getMap("default");

        boolean b = h1.executeTransaction(options, new TransactionalTask<Boolean>() {
            public Boolean execute(TransactionalTaskContext context) throws TransactionException {
                final TransactionalMap<Object, Object> txMap = context.getMap("default");
                txMap.set("1", "value");
                txMap.set("1", "value2");
                assertEquals("value2", txMap.get("1"));
                assertNull(map2.get("1"));
                assertNull(map2.get("2"));
                assertEquals(1, txMap.size());

                return true;
            }
        });
        assertTrue(b);

        IMap map1 = h1.getMap("default");
        assertEquals("value2", map1.get("1"));
        assertEquals("value2", map2.get("1"));
    }

    @Test
    public void testTxnPutTTL() throws TransactionException {
        Config config = new Config();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance h1 = factory.newHazelcastInstance(config);
        final HazelcastInstance h2 = factory.newHazelcastInstance(config);
        final IMap map2 = h2.getMap("putWithTTL");

        boolean b = h1.executeTransaction(options, new TransactionalTask<Boolean>() {
            public Boolean execute(TransactionalTaskContext context) throws TransactionException {
                final TransactionalMap<Object, Object> txMap = context.getMap("putWithTTL");
                txMap.put("1", "value", 5, TimeUnit.SECONDS);
                assertEquals("value", txMap.get("1"));
                assertEquals(1, txMap.size());
                return true;
            }
        });
        assertTrue(b);

        IMap map1 = h2.getMap("putWithTTL");
        assertEquals("value", map1.get("1"));
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        assertNull(map1.get("1"));
    }

    @Test
    public void testTxnGetForUpdate() throws TransactionException {
        Config config = new Config();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance h1 = factory.newHazelcastInstance(config);
        final HazelcastInstance h2 = factory.newHazelcastInstance(config);
        final IMap<String, String> map = h2.getMap("default");
        final CountDownLatch latch1 = new CountDownLatch(1);
        final CountDownLatch latch2 = new CountDownLatch(1);
        final AtomicBoolean pass = new AtomicBoolean(true);

        map.put("var", "value0");

        Runnable updater = new Runnable() {
            public void run() {
                try {
                    latch1.await(100, TimeUnit.SECONDS);
                    pass.set(map.tryPut("var", "value1", 0, TimeUnit.SECONDS) == false);
                    latch2.countDown();
                } catch (Exception e) {
                }
            }
        };
        new Thread(updater).start();
        boolean b = h1.executeTransaction(options, new TransactionalTask<Boolean>() {
            public Boolean execute(TransactionalTaskContext context) throws TransactionException {
                try {
                    final TransactionalMap<String, Integer> txMap = context.getMap("default");
                    assertEquals("value0", txMap.getForUpdate("var"));
                    latch1.countDown();
                    latch2.await(100, TimeUnit.SECONDS);
                } catch (Exception e) {
                }
                return true;
            }
        });
        assertTrue(b);
        assertTrue(pass.get());
        assertTrue(map.tryPut("var", "value2", 0, TimeUnit.SECONDS));

    }

    @Test
    public void testTxnGetForUpdateTimeout() throws TransactionException {
        Config config = new Config();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance h1 = factory.newHazelcastInstance(config);
        final HazelcastInstance h2 = factory.newHazelcastInstance(config);
        final IMap<String, String> map = h2.getMap("default");
        map.put("var", "value0");
        map.lock("var");
        TransactionOptions opts = new TransactionOptions();
        opts.setTimeout(1, TimeUnit.SECONDS);

        try {
            boolean b = h1.executeTransaction(opts, new TransactionalTask<Boolean>() {
                public Boolean execute(TransactionalTaskContext context) throws TransactionException {
                    final TransactionalMap<String, String> txMap = context.getMap("default");
                    txMap.getForUpdate("var");
                    fail();
                    return true;
                }
            });
        } catch (TransactionException e) {
        }
        assertTrue(map.isLocked("var"));

    }

    @Test
    @Category(ProblematicTest.class)
    public void testTxnGetForUpdateTxnFails() throws TransactionException {
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_OPERATION_CALL_TIMEOUT_MILLIS, "5000");
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance h1 = factory.newHazelcastInstance(config);
        final HazelcastInstance h2 = factory.newHazelcastInstance(config);
        final IMap<String, String> map = h2.getMap("default");
        map.put("var", "value");
        map.lock("varLocked");
        TransactionOptions opts = new TransactionOptions();
        opts.setTimeout(1, TimeUnit.SECONDS);

        try {
            boolean b = h1.executeTransaction(opts, new TransactionalTask<Boolean>() {
                public Boolean execute(TransactionalTaskContext context) throws TransactionException {
                    final TransactionalMap<String, String> txMap = context.getMap("default");
                    txMap.getForUpdate("var");
                    throw new TransactionException();
                }
            });
        } catch (TransactionException e) {
        }
        assertFalse(map.isLocked("var"));
        assertTrue(map.isLocked("varLocked"));

    }


    @Test
    public void testTxnGetForUpdateMultipleTimes() throws TransactionException {
        Config config = new Config();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance h1 = factory.newHazelcastInstance(config);
        final HazelcastInstance h2 = factory.newHazelcastInstance(config);
        final IMap<String, String> map = h2.getMap("default");
        map.put("var", "value0");

        boolean b = h1.executeTransaction(options, new TransactionalTask<Boolean>() {
            public Boolean execute(TransactionalTaskContext context) throws TransactionException {
                try {
                    final TransactionalMap<String, String> txMap = context.getMap("default");
                    assertEquals("value0", txMap.getForUpdate("var"));
                    assertEquals("value0", txMap.getForUpdate("var"));
                    assertEquals("value0", txMap.getForUpdate("var"));
                } catch (Exception e) {
                }
                return true;
            }
        });

    }

    @Test
    public void testTxnUpdateThenGetForUpdate() throws TransactionException {
        Config config = new Config();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance h1 = factory.newHazelcastInstance(config);
        final HazelcastInstance h2 = factory.newHazelcastInstance(config);
        final IMap<String, String> map = h2.getMap("default");
        map.put("var", "value0");

        boolean b = h1.executeTransaction(options, new TransactionalTask<Boolean>() {
            public Boolean execute(TransactionalTaskContext context) throws TransactionException {
                try {
                    final TransactionalMap<String, String> txMap = context.getMap("default");
                    assertEquals("value0", txMap.put("var", "value1"));
                    assertEquals("value1", txMap.getForUpdate("var"));
                } catch (Exception e) {
                }
                return true;
            }
        });

    }

    @Test
    public void testTxnGetForUpdateThenUpdate() throws TransactionException {
        Config config = new Config();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance h1 = factory.newHazelcastInstance(config);
        final HazelcastInstance h2 = factory.newHazelcastInstance(config);
        final IMap<String, String> map = h2.getMap("default");
        map.put("var", "value0");

        boolean b = h1.executeTransaction(options, new TransactionalTask<Boolean>() {
            public Boolean execute(TransactionalTaskContext context) throws TransactionException {
                try {
                    final TransactionalMap<String, String> txMap = context.getMap("default");
                    assertEquals("value0", txMap.getForUpdate("var"));
                    assertEquals("value0", txMap.put("var", "value1"));
                    assertEquals("value1", txMap.getForUpdate("var"));
                    assertEquals("value1", txMap.get("var"));
                } catch (Exception e) {
                }
                return true;
            }
        });
    }


    @Test
    public void testTxnRemove() throws TransactionException {
        Config config = new Config();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance h1 = factory.newHazelcastInstance(config);
        final HazelcastInstance h2 = factory.newHazelcastInstance(config);
        final IMap map2 = h2.getMap("default");
        map2.put("1", "1");
        map2.put("2", "2");

        boolean b = h1.executeTransaction(options, new TransactionalTask<Boolean>() {
            public Boolean execute(TransactionalTaskContext context) throws TransactionException {
                final TransactionalMap<Object, Object> txMap = context.getMap("default");
                txMap.put("3", "3");
                map2.put("4", "4");
                assertEquals("1", txMap.remove("1"));
                assertEquals("2", map2.remove("2"));
                assertEquals("1", map2.get("1"));
                assertEquals(null, txMap.get("1"));
                assertEquals(null, txMap.remove("2"));
                assertEquals(2, txMap.size());
                return true;
            }
        });
        assertTrue(b);

        IMap map1 = h1.getMap("default");
        assertEquals(null, map1.get("1"));
        assertEquals(null, map2.get("1"));

        assertEquals(null, map1.get("2"));
        assertEquals(null, map2.get("2"));

        assertEquals("3", map1.get("3"));
        assertEquals("3", map2.get("3"));

        assertEquals("4", map1.get("4"));
        assertEquals("4", map2.get("4"));
    }

    @Test
    public void testTxnRemoveIfSame() throws TransactionException {
        Config config = new Config();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance h1 = factory.newHazelcastInstance(config);
        final HazelcastInstance h2 = factory.newHazelcastInstance(config);
        final IMap map2 = h2.getMap("default");
        map2.put("1", "1");
        map2.put("2", "2");

        boolean b = h1.executeTransaction(options, new TransactionalTask<Boolean>() {
            public Boolean execute(TransactionalTaskContext context) throws TransactionException {
                final TransactionalMap<Object, Object> txMap = context.getMap("default");
                txMap.put("3", "3");
                map2.put("4", "4");
                assertEquals(true, txMap.remove("1", "1"));
                assertEquals(false, txMap.remove("2", "1"));
                assertEquals("1", map2.get("1"));
                assertEquals(null, txMap.get("1"));
                assertEquals(true, txMap.remove("2", "2"));
                assertEquals(false, txMap.remove("3", null));
                assertEquals(false, txMap.remove("5", "2"));
                assertEquals(2, txMap.size());
                return true;
            }
        });
        assertTrue(b);

        IMap map1 = h1.getMap("default");
        assertEquals(null, map1.get("1"));
        assertEquals(null, map2.get("1"));

        assertEquals(null, map1.get("2"));
        assertEquals(null, map2.get("2"));

        assertEquals("3", map1.get("3"));
        assertEquals("3", map2.get("3"));

        assertEquals("4", map1.get("4"));
        assertEquals("4", map2.get("4"));
    }

    @Test
    public void testTxnDelete() throws TransactionException {
        Config config = new Config();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance h1 = factory.newHazelcastInstance(config);
        final HazelcastInstance h2 = factory.newHazelcastInstance(config);
        final IMap map2 = h2.getMap("default");
        map2.put("1", "1");
        map2.put("2", "2");

        boolean b = h1.executeTransaction(options, new TransactionalTask<Boolean>() {
            public Boolean execute(TransactionalTaskContext context) throws TransactionException {
                final TransactionalMap<Object, Object> txMap = context.getMap("default");
                txMap.put("3", "3");
                map2.put("4", "4");
                txMap.delete("1");
                map2.delete("2");
                assertEquals("1", map2.get("1"));
                assertEquals(null, txMap.get("1"));
                txMap.delete("2");
                assertEquals(2, txMap.size());
                return true;
            }
        });
        assertTrue(b);

        IMap map1 = h1.getMap("default");
        assertEquals(null, map1.get("1"));
        assertEquals(null, map2.get("1"));

        assertEquals(null, map1.get("2"));
        assertEquals(null, map2.get("2"));

        assertEquals("3", map1.get("3"));
        assertEquals("3", map2.get("3"));

        assertEquals("4", map1.get("4"));
        assertEquals("4", map2.get("4"));
    }

    @Test
    public void testTxnPutIfAbsent() throws TransactionException {
        Config config = new Config();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance h1 = factory.newHazelcastInstance(config);
        final HazelcastInstance h2 = factory.newHazelcastInstance(config);
        final IMap map2 = h2.getMap("default");

        boolean b = h1.executeTransaction(options, new TransactionalTask<Boolean>() {
            public Boolean execute(TransactionalTaskContext context) throws TransactionException {
                final TransactionalMap<Object, Object> txMap = context.getMap("default");
                txMap.putIfAbsent("1", "value");
                assertEquals("value", txMap.putIfAbsent("1", "value2"));
                assertEquals("value", txMap.get("1"));
                assertNull(map2.get("1"));
                assertNull(map2.get("2"));
                return true;
            }
        });
        assertTrue(b);

        IMap map1 = h1.getMap("default");
        assertEquals("value", map1.get("1"));
        assertEquals("value", map2.get("1"));
    }

    @Test
    public void testTxnPutIfAbsentParallel() throws InterruptedException {
        final String TEST_MAP = "testMap";
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        final HazelcastInstance sharedDataService = factory.newHazelcastInstance(new Config());
        final Object[] v = new Object[1];
        Thread contender = new Thread(new Runnable() {
            @Override
            public void run() {
                TransactionContext transactionContext = sharedDataService.newTransactionContext();
                transactionContext.beginTransaction();
                TransactionalMap<String, Object> map = transactionContext.getMap(TEST_MAP);
                v[0] = map.putIfAbsent("k", "t");
                transactionContext.commitTransaction();
            }
        });

        TransactionContext transactionContext;
        TransactionalMap<String, Object> map;
        transactionContext = sharedDataService.newTransactionContext();
        transactionContext.beginTransaction();
        map = transactionContext.getMap(TEST_MAP);
        map.put("k", "v");
        transactionContext.commitTransaction();


        contender.start();
        contender.join(1000);
        assertFalse("Contender thread finished", contender.isAlive());
        assertNotNull(v[0]);

        transactionContext = sharedDataService.newTransactionContext();
        transactionContext.beginTransaction();
        map = transactionContext.getMap(TEST_MAP);
        map.delete("k");
        transactionContext.commitTransaction();

    }


    @Test
    public void testTxnReplace() throws TransactionException {
        Config config = new Config();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance h1 = factory.newHazelcastInstance(config);
        final HazelcastInstance h2 = factory.newHazelcastInstance(config);
        final IMap map2 = h2.getMap("default");

        boolean b = h1.executeTransaction(options, new TransactionalTask<Boolean>() {
            public Boolean execute(TransactionalTaskContext context) throws TransactionException {
                final TransactionalMap<Object, Object> txMap = context.getMap("default");
                assertNull(txMap.replace("1", "value"));
                txMap.put("1", "value2");
                assertEquals("value2", txMap.replace("1", "value3"));
                assertEquals("value3", txMap.get("1"));
                assertNull(map2.get("1"));
                assertNull(map2.get("2"));
                return true;
            }
        });
        assertTrue(b);

        IMap map1 = h1.getMap("default");
        assertEquals("value3", map1.get("1"));
        assertEquals("value3", map2.get("1"));
    }


    @Test
    public void testTxnReplaceIfSame() throws TransactionException {
        Config config = new Config();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance h1 = factory.newHazelcastInstance(config);
        final HazelcastInstance h2 = factory.newHazelcastInstance(config);
        final IMap map2 = h2.getMap("default");
        map2.put("1", "1");
        map2.put("2", "2");

        boolean b = h1.executeTransaction(options, new TransactionalTask<Boolean>() {
            public Boolean execute(TransactionalTaskContext context) throws TransactionException {
                final TransactionalMap<Object, Object> txMap = context.getMap("default");
                assertEquals(true, txMap.replace("1", "1", "11"));
                assertEquals(false, txMap.replace("5", "5", "55"));
                assertEquals(false, txMap.replace("2", "1", "22"));
                assertEquals("1", map2.get("1"));
                assertEquals("11", txMap.get("1"));
                assertEquals("2", map2.get("2"));
                assertEquals("2", txMap.get("2"));
                return true;
            }
        });
        assertTrue(b);

        IMap map1 = h1.getMap("default");
        assertEquals("11", map1.get("1"));
        assertEquals("11", map2.get("1"));
        assertEquals("2", map1.get("2"));
        assertEquals("2", map2.get("2"));
    }

    @Test
    public void testTxnReplace2() throws TransactionException {
        Config config = new Config();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance h1 = factory.newHazelcastInstance(config);
        final HazelcastInstance h2 = factory.newHazelcastInstance(config);
        final IMap map2 = h2.getMap("default");
        map2.put("1", "value2");
        boolean b = h1.executeTransaction(options, new TransactionalTask<Boolean>() {
            public Boolean execute(TransactionalTaskContext context) throws TransactionException {
                final TransactionalMap<Object, Object> txMap = context.getMap("default");
                assertEquals("value2", txMap.replace("1", "value3"));
                assertEquals("value3", txMap.get("1"));
                assertNull(map2.get("2"));
                return true;
            }
        });
        assertTrue(b);

        IMap map1 = h1.getMap("default");
        assertEquals("value3", map1.get("1"));
        assertEquals("value3", map2.get("1"));
    }

    @Test
    public void testIssue1076() {
        final HazelcastInstance inst = createHazelcastInstance();

        IMap map = inst.getMap("default");

        EntryListener<String, Integer> l = new EntryAdapter<String, Integer>() {
        };

        EntryObject e = new PredicateBuilder().getEntryObject();
        Predicate<String, Integer> p = e.equal(1);

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


    @Test
    @Category(ProblematicTest.class)
    // TODO: @mm - Review following case...
    public void testFailingMapStore() throws TransactionException {
        final String map = "map";
        final String anotherMap = "anotherMap";
        Config config = new Config();
        config.getMapConfig(map).setMapStoreConfig(new MapStoreConfig()
                .setEnabled(true).setImplementation(new MapStoreAdapter() {
                    public void store(Object key, Object value) {
                        throw new IllegalStateException("Map store intentionally failed :) ");
                    }
                }));

        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance h1 = factory.newHazelcastInstance(config);
        final HazelcastInstance h2 = factory.newHazelcastInstance(config);

        boolean b = h1.executeTransaction(options, new TransactionalTask<Boolean>() {
            public Boolean execute(TransactionalTaskContext context) throws TransactionException {
                assertNull(context.getMap(map).put("1", "value1"));
                assertNull(context.getMap(anotherMap).put("1", "value1"));
                return true;
            }
        });

        assertTrue(b);
        assertNull(h2.getMap(map).get("1"));
        assertEquals("value1", h2.getMap(anotherMap).get("1"));
    }

    @Test
    public void testRollbackMap() throws Throwable {
        Config config = new Config();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(4);
        final HazelcastInstance h1 = factory.newHazelcastInstance(config);

        final TransactionContext transactionContext = h1.newTransactionContext();

        transactionContext.beginTransaction();

        TransactionalMap<Integer, String> m = transactionContext.getMap("testRollbackMap");

        Integer key1 = 1;
        String value1 = "value1";

        Integer key2 = 2;
        String value2 = "value2";

        m.put(key1, value1);
        m.put(key2, value2);

        transactionContext.rollbackTransaction();

        assertNull(h1.getMap("testRollbackMap").get(key1));
        assertNull(h1.getMap("testRollbackMap").get(key2));
    }

    @Test(expected = TransactionNotActiveException.class)
    public void testTxnMapOuterTransaction() throws Throwable {
        final HazelcastInstance h1 = createHazelcastInstance();

        final TransactionContext transactionContext = h1.newTransactionContext();
        transactionContext.beginTransaction();
        TransactionalMap<Integer, Integer> m = transactionContext.getMap("testTxnMapOuterTransaction");
        m.put(1, 1);
        transactionContext.commitTransaction();
        m.put(1, 1);
    }

    @Test
    public void testIssue615keySet() throws TransactionException {
        Config config = new Config();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance h1 = factory.newHazelcastInstance(config);
        final HazelcastInstance h2 = factory.newHazelcastInstance(config);
        final IMap map = h2.getMap("default");
        map.put("1", "1");
        map.put("2", "2");

        boolean b = h1.executeTransaction(options, new TransactionalTask<Boolean>() {
            public Boolean execute(TransactionalTaskContext context) throws TransactionException {
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
            }
        });

        assertEquals(2, map.keySet().size());

        // raise an exception and rollback changes.
        try {
            boolean b2 = h1.executeTransaction(options, new TransactionalTask<Boolean>() {
                public Boolean execute(TransactionalTaskContext context) throws TransactionException {
                    final TransactionalMap<Object, Object> txMap = context.getMap("default");

                    txMap.put("5", "5");

                    assertEquals(3, txMap.keySet().size());
                    assertEquals(2, map.keySet().size());

                    throw new DummyUncheckedHazelcastTestException();
                }
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
    public void testIssue615KeysetWithPredicate() throws TransactionException {
        Config config = new Config();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance h1 = factory.newHazelcastInstance(config);
        final HazelcastInstance h2 = factory.newHazelcastInstance(config);
        final IMap map = h2.getMap("default");
        final SampleObjects.Employee employee1 = new SampleObjects.Employee("abc-123-xvz", 34, true, 10D);
        final SampleObjects.Employee employee2 = new SampleObjects.Employee("abc-1xvz", 4, true, 7D);
        final SampleObjects.Employee employee3 = new SampleObjects.Employee("abc-1xasda...vz", 7, true, 1D);
        final SampleObjects.Employee employee4 = new SampleObjects.Employee("abc-1asdsaxvz", 2, true, 2D);

        map.put(1, employee1);


        try {
            boolean b = h1.executeTransaction(options, new TransactionalTask<Boolean>() {
                public Boolean execute(TransactionalTaskContext context) throws TransactionException {
                    final TransactionalMap<Object, Object> txMap = context.getMap("default");

                    assertEquals(0, txMap.keySet(new SqlPredicate("age <= 10")).size());
                    //put
                    txMap.put(2, employee2);
                    Set keys = txMap.keySet(new SqlPredicate("age <= 10"));
                    Iterator iterator = keys.iterator();

                    assertEquals(1, keys.size());

                    while (iterator.hasNext()) {
                        assertEquals(2, ((Integer) iterator.next()).intValue());
                    }

                    txMap.put(3, employee3);
                    txMap.put(4, employee4);

                    keys = txMap.keySet(new SqlPredicate("age <= 10"));
                    assertEquals(3, keys.size());

                    // force rollback.
                    throw new DummyUncheckedHazelcastTestException();
                }
            });
        } catch (Exception e) {
            if (!(e instanceof DummyUncheckedHazelcastTestException)) {
                throw new RuntimeException(e);
            }
        }
        assertEquals(1, map.size());
        assertEquals(1, map.keySet().size());
        assertEquals(0, map.keySet(new SqlPredicate("age <= 10")).size());

        h1.shutdown();
        h2.shutdown();
    }

    @Test
    public void testIssue615KeysetPredicates() throws TransactionException {
        final String MAP_NAME = "defaultMap";
        final Config config = new Config();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance h1 = factory.newHazelcastInstance(config);
        final HazelcastInstance h2 = factory.newHazelcastInstance(config);
        final IMap map = h2.getMap(MAP_NAME);
        final SampleObjects.Employee employee1 = new SampleObjects.Employee("abc-123-xvz", 34, true, 10D);
        final SampleObjects.Employee employee2 = new SampleObjects.Employee("abc-1xvz", 4, true, 7D);
        final SampleObjects.Employee employee3 = new SampleObjects.Employee("abc-1xasda...vz", 7, true, 1D);
        final SampleObjects.Employee employee4 = new SampleObjects.Employee("abc-1asdsaxvz", 2, true, 2D);

        map.put(employee1, employee1);

        final TransactionContext context = h1.newTransactionContext();
        context.beginTransaction();

        final TransactionalMap<Object, Object> txMap = context.getMap(MAP_NAME);

        assertNull(txMap.put(employee2, employee2));

        assertEquals(2, txMap.size());
        assertEquals(2, txMap.keySet().size());
        assertEquals(1, txMap.keySet(new SqlPredicate("age = 34")).size());

        context.commitTransaction();


        assertEquals(2, map.size());

        h1.shutdown();
        h2.shutdown();
    }

    @Test
    public void testIssue615values() throws TransactionException {
        Config config = new Config();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance h1 = factory.newHazelcastInstance(config);
        final HazelcastInstance h2 = factory.newHazelcastInstance(config);
        final IMap map2 = h2.getMap("default");
        map2.put("1", "1");
        map2.put("2", "2");


        boolean b = h1.executeTransaction(options, new TransactionalTask<Boolean>() {
            public Boolean execute(TransactionalTaskContext context) throws TransactionException {
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
            }
        });

        assertEquals(3, map2.values().size());

        h1.shutdown();
        h2.shutdown();
    }

    @Test
    public void testIssue615ValuesWithPredicate() throws TransactionException {
        Config config = new Config();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance h1 = factory.newHazelcastInstance(config);
        final HazelcastInstance h2 = factory.newHazelcastInstance(config);
        final IMap map2 = h2.getMap("default");
        final SampleObjects.Employee emp1 = new SampleObjects.Employee("abc-123-xvz", 34, true, 10D);
        map2.put(1, emp1);
        final SampleObjects.Employee emp2 = new SampleObjects.Employee("xvz", 4, true, 10D);

        boolean b = h1.executeTransaction(options, new TransactionalTask<Boolean>() {
            public Boolean execute(TransactionalTaskContext context) throws TransactionException {
                final TransactionalMap<Object, Object> txMap = context.getMap("default");
                assertEquals(0, txMap.values(new SqlPredicate("age <= 10")).size());
                txMap.put(2, emp2);
                Collection coll = txMap.values(new SqlPredicate("age <= 10"));
                Iterator<Object> iterator = coll.iterator();
                while (iterator.hasNext()) {
                    final SampleObjects.Employee e = (SampleObjects.Employee) iterator.next();
                    assertEquals(emp2, e);
                }
                coll = txMap.values(new SqlPredicate("age > 30 "));
                iterator = coll.iterator();
                while (iterator.hasNext()) {
                    final SampleObjects.Employee e = (SampleObjects.Employee) iterator.next();
                    assertEquals(emp1, e);
                }
                txMap.remove(2);
                coll = txMap.values(new SqlPredicate("age <= 10 "));
                assertEquals(0, coll.size());
                return true;
            }
        });
        assertEquals(0, map2.values(new SqlPredicate("age <= 10")).size());
        assertEquals(1, map2.values(new SqlPredicate("age = 34")).size());
        h1.shutdown();
        h2.shutdown();
    }

    @Test
    public void testValuesWithPredicates_notContains_oldValues() throws TransactionException {
        Config config = new Config();
        final String mapName = "testValuesWithPredicate_notContains_oldValues";
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance h1 = factory.newHazelcastInstance(config);
        final HazelcastInstance h2 = factory.newHazelcastInstance(config);
        final IMap map = h1.getMap(mapName);
        final SampleObjects.Employee employeeAtAge22 = new SampleObjects.Employee("emin", 22, true, 10D);
        final SampleObjects.Employee employeeAtAge23 = new SampleObjects.Employee("emin", 23, true, 10D);
        map.put(1, employeeAtAge22);

        boolean b = h1.executeTransaction(options, new TransactionalTask<Boolean>() {
            public Boolean execute(TransactionalTaskContext context) throws TransactionException {
                final TransactionalMap<Object, Object> txMap = context.getMap(mapName);
                assertEquals(1, txMap.values(new SqlPredicate("age > 21")).size());
                txMap.put(1, employeeAtAge23);
                Collection coll = txMap.values(new SqlPredicate("age > 21"));
                assertEquals(1, coll.size());
                return true;
            }
        });
        h1.shutdown();
        h2.shutdown();

    }


    @Test(expected = IllegalArgumentException.class)
    public void testValuesWithPagingPredicate() throws TransactionException {
        final int nodeCount = 1;
        final String mapName = randomMapName("testValuesWithPagingPredicate");
        final Config config = new Config();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(nodeCount);
        final HazelcastInstance node = factory.newHazelcastInstance(config);
        final IMap map = node.getMap(mapName);

        final SampleObjects.Employee emp = new SampleObjects.Employee("name", 77, true, 10D);
        map.put(1, emp);

        node.executeTransaction(options, new TransactionalTask<Boolean>() {
            public Boolean execute(TransactionalTaskContext context) throws TransactionException {
                final TransactionalMap<Object, Object> txMap = context.getMap(mapName);
                PagingPredicate predicate = new PagingPredicate(5);
                txMap.values(predicate);
                return true;
            }
        });
    }

    @Test
    public void testValuesWithPredicate_removingExistentEntry() throws TransactionException {
        final int nodeCount = 1;
        final String mapName = randomMapName("_testValuesWithPredicate_removingExistentEntry_");
        final Config config = new Config();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(nodeCount);
        final HazelcastInstance node = factory.newHazelcastInstance(config);
        final IMap map = node.getMap(mapName);

        final SampleObjects.Employee emp = new SampleObjects.Employee("name", 77, true, 10D);
        map.put(1, emp);

        node.executeTransaction(options, new TransactionalTask<Boolean>() {
            public Boolean execute(TransactionalTaskContext context) throws TransactionException {
                final TransactionalMap<Object, Object> txMap = context.getMap(mapName);
                txMap.remove(1);
                Collection<Object> coll = txMap.values(new SqlPredicate("age > 70 "));
                assertEquals(0, coll.size());
                return true;
            }
        });
        node.shutdown();
    }

    @Test
    public void testUpdatesInTxnFiringUpdateEvent() {
        final String mapName = randomMapName("testUpdatesInTxnFiringUpdateEvent");
        final HazelcastInstance node = createHazelcastInstance();
        final IMap<String, String> map = node.getMap(mapName);
        final CountDownLatch expectedUpdateEventCount = new CountDownLatch(1);
        map.addEntryListener(new EntryAdapter<String, String>() {

            @Override
            public void entryUpdated(EntryEvent<String, String> event) {
                expectedUpdateEventCount.countDown();
            }

        }, true);

        map.put("foo", "one");

        TransactionContext context = node.newTransactionContext();
        context.beginTransaction();
        TransactionalMap<String, String> transactionalMap = context.getMap(mapName);
        transactionalMap.put("foo", "three");
        context.commitTransaction();

        assertOpenEventually("Not reached expected update event count", expectedUpdateEventCount);

    }
}
