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
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalTask;
import com.hazelcast.transaction.TransactionalTaskContext;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(ParallelTest.class)
public class MapTransactionTest extends HazelcastTestSupport {

    @BeforeClass
    public static void init() {
        Hazelcast.shutdownAll();
    }

    @After
    public void cleanUp() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testCommitOrder() throws TransactionException {
        Config config = new Config();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(4);
        final HazelcastInstance h1 = factory.newHazelcastInstance(config);
        final HazelcastInstance h2 = factory.newHazelcastInstance(config);
        final HazelcastInstance h3 = factory.newHazelcastInstance(config);
        final HazelcastInstance h4 = factory.newHazelcastInstance(config);

        boolean b = h1.executeTransaction(new TransactionalTask<Boolean>() {
            public Boolean execute(TransactionalTaskContext context) throws TransactionException {
                final TransactionalMap<Object, Object> txMap = context.getMap("default");
                txMap.put("1", "value1");
                assertEquals("value1", txMap.put("1", "value2"));
                assertEquals("value2", txMap.put("1", "value3"));
                assertEquals("value3", txMap.put("1", "value4"));
                assertEquals("value4", txMap.put("1", "value5"));
                assertEquals("value5", txMap.put("1", "value6"));
                return true;
            }
        });
        assertEquals("value6",h4.getMap("default").get("1"));
    }

    @Test
    public void testTxnCommit() throws TransactionException {
        Config config = new Config();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance h1 = factory.newHazelcastInstance(config);
        final HazelcastInstance h2 = factory.newHazelcastInstance(config);
        final IMap map2 = h2.getMap("default");

        boolean b = h1.executeTransaction(new TransactionalTask<Boolean>() {
            public Boolean execute(TransactionalTaskContext context) throws TransactionException {
                final TransactionalMap<Object, Object> txMap = context.getMap("default");
                txMap.put("1", "value");
                assertEquals("value", txMap.put("1", "value2"));
                assertEquals("value2", txMap.get("1"));
                assertEquals(true, txMap.containsKey("1"));
                assertNull(map2.get("1"));
                assertNull(map2.get("2"));
                return true;
            }
        });
        assertTrue(b);

        IMap map1 = h1.getMap("default");
        assertEquals("value2", map1.get("1"));
        assertEquals("value2", map2.get("1"));
    }

    @Test
    public void testTxnBackupDies() throws TransactionException {
        Config config = new Config();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance h1 = factory.newHazelcastInstance(config);
        final HazelcastInstance h2 = factory.newHazelcastInstance(config);
        final IMap map1 = h1.getMap("default");
        final int size = 50;
        final CountDownLatch latch = new CountDownLatch(size + 1);

        Runnable runnable = new Runnable() {
            public void run() {
                try {
                    boolean b = h1.executeTransaction(new TransactionOptions().setDurability(1), new TransactionalTask<Boolean>() {
                        public Boolean execute(TransactionalTaskContext context) throws TransactionException {
                            final TransactionalMap<Object, Object> txMap = context.getMap("default");
                            for (int i = 0; i < size; i++) {
                                txMap.put(i, i);
                                try {
                                    Thread.sleep(100);
                                } catch (InterruptedException e) {
                                }
                                latch.countDown();
                            }
                            return true;
                        }
                    });
                    fail();
                } catch (Exception e) {
                }
                latch.countDown();
            }
        };
        new Thread(runnable).start();
        try {
            Thread.sleep(1000);
            h2.getLifecycleService().shutdown();
            latch.await();
            for (int i = 0; i < size; i++) {
                assertNull(map1.get(i));
            }
        } catch (InterruptedException e) {
        }
    }

    @Test
    public void testTxnOwnerDies() throws TransactionException {
        Config config = new Config();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        final HazelcastInstance h1 = factory.newHazelcastInstance(config);
        final HazelcastInstance h2 = factory.newHazelcastInstance(config);
        final HazelcastInstance h3 = factory.newHazelcastInstance(config);
        final IMap map1 = h1.getMap("default");
        final int size = 50;

        Runnable runnable = new Runnable() {
            public void run() {
                try {
                    boolean b = h1.executeTransaction(new TransactionalTask<Boolean>() {
                        public Boolean execute(TransactionalTaskContext context) throws TransactionException {
                            final TransactionalMap<Object, Object> txMap = context.getMap("default");
                            for (int i = 0; i < size; i++) {
                                txMap.put(i, i);
                                try {
                                    Thread.sleep(100);
                                    System.out.println("turn:" + i);
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                            }
                            return true;
                        }
                    });
                    fail();
                } catch (TransactionException e) {
                }
            }
        };
        try {
            Thread thread = new Thread(runnable);
            thread.start();
            Thread.sleep(200);
            h1.getLifecycleService().shutdown();
            thread.join();
            final IMap map2 = h2.getMap("default");
            for (int i = 0; i < size; i++) {
                assertNull(map2.get(i));
            }
        } catch (InterruptedException e) {
        }
    }


    @Test
    public void testTxnSet() throws TransactionException {
        Config config = new Config();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance h1 = factory.newHazelcastInstance(config);
        final HazelcastInstance h2 = factory.newHazelcastInstance(config);
        final IMap map2 = h2.getMap("default");

        boolean b = h1.executeTransaction(new TransactionalTask<Boolean>() {
            public Boolean execute(TransactionalTaskContext context) throws TransactionException {
                final TransactionalMap<Object, Object> txMap = context.getMap("default");
                txMap.set("1", "value");
                txMap.set("1", "value2");
                assertEquals("value2", txMap.get("1"));
                assertNull(map2.get("1"));
                assertNull(map2.get("2"));
                return true;
            }
        });
        assertTrue(b);

        IMap map1 = h1.getMap("default");
        assertEquals("value2", map1.get("1"));
        assertEquals("value2", map2.get("1"));
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

        boolean b = h1.executeTransaction(new TransactionalTask<Boolean>() {
            public Boolean execute(TransactionalTaskContext context) throws TransactionException {
                final TransactionalMap<Object, Object> txMap = context.getMap("default");
                txMap.put("3", "3");
                map2.put("4", "4");
                assertEquals("1", txMap.remove("1"));
                assertEquals("2", map2.remove("2"));
                assertEquals("1", map2.get("1"));
                assertEquals(null, txMap.get("1"));
                assertEquals(null, txMap.remove("2"));
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

        boolean b = h1.executeTransaction(new TransactionalTask<Boolean>() {
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

        boolean b = h1.executeTransaction(new TransactionalTask<Boolean>() {
            public Boolean execute(TransactionalTaskContext context) throws TransactionException {
                final TransactionalMap<Object, Object> txMap = context.getMap("default");
                txMap.put("3", "3");
                map2.put("4", "4");
                txMap.delete("1");
                map2.delete("2");
                assertEquals("1", map2.get("1"));
                assertEquals(null, txMap.get("1"));
                txMap.delete("2");
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

        boolean b = h1.executeTransaction(new TransactionalTask<Boolean>() {
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
    public void testTxnReplace() throws TransactionException {
        Config config = new Config();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance h1 = factory.newHazelcastInstance(config);
        final HazelcastInstance h2 = factory.newHazelcastInstance(config);
        final IMap map2 = h2.getMap("default");

        boolean b = h1.executeTransaction(new TransactionOptions().setTimeout(1, TimeUnit.SECONDS), new TransactionalTask<Boolean>() {
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

        boolean b = h1.executeTransaction(new TransactionalTask<Boolean>() {
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
        boolean b = h1.executeTransaction(new TransactionalTask<Boolean>() {
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

}
