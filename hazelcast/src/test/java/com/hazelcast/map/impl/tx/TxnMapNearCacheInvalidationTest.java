/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionalTask;
import com.hazelcast.transaction.TransactionalTaskContext;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.map.impl.tx.TxnMapNearCacheInvalidationTest.InvalidatorTxnOp.CONTAINS_KEY;
import static com.hazelcast.map.impl.tx.TxnMapNearCacheInvalidationTest.InvalidatorTxnOp.DELETE;
import static com.hazelcast.map.impl.tx.TxnMapNearCacheInvalidationTest.InvalidatorTxnOp.PUT;
import static com.hazelcast.map.impl.tx.TxnMapNearCacheInvalidationTest.InvalidatorTxnOp.PUT_IF_ABSENT;
import static com.hazelcast.map.impl.tx.TxnMapNearCacheInvalidationTest.InvalidatorTxnOp.PUT_TTL;
import static com.hazelcast.map.impl.tx.TxnMapNearCacheInvalidationTest.InvalidatorTxnOp.REPLACE;
import static com.hazelcast.map.impl.tx.TxnMapNearCacheInvalidationTest.InvalidatorTxnOp.REPLACE_IF_SAME;
import static com.hazelcast.map.impl.tx.TxnMapNearCacheInvalidationTest.InvalidatorTxnOp.SET;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class TxnMapNearCacheInvalidationTest extends HazelcastTestSupport {

    @Parameters(name = "format:{0} serializeKeys:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.BINARY, true},
                {InMemoryFormat.BINARY, false},
                {InMemoryFormat.OBJECT, true},
                {InMemoryFormat.OBJECT, false},
        });
    }

    @Parameter
    public InMemoryFormat inMemoryFormat;

    @Parameter(value = 1)
    public boolean serializeKeys;

    @Test
    public void txn_map_contains_newly_put_key_even_it_is_null_cached_after_addition() {
        final String mapName = "test";
        final int key = 1;

        NearCacheConfig nearCacheConfig = createNearCacheConfig(mapName)
                .setInvalidateOnChange(true);

        Config config = smallInstanceConfig();
        config.getMapConfig(mapName).setNearCacheConfig(nearCacheConfig);

        final HazelcastInstance instance = createHazelcastInstance(config);

        final CountDownLatch waitTxnPut = new CountDownLatch(1);
        final CountDownLatch waitNullCaching = new CountDownLatch(1);

        final AtomicBoolean keyExistInTxnMap = new AtomicBoolean(false);

        Thread txnPutThread = new Thread(new Runnable() {
            @Override
            public void run() {
                Boolean result = instance.executeTransaction(new TransactionalTask<Boolean>() {
                    @Override
                    public Boolean execute(TransactionalTaskContext context) throws TransactionException {
                        TransactionalMap<Integer, Integer> map = context.getMap(mapName);
                        // 1. first put key into txn map inside txn
                        map.put(key, 1);

                        waitTxnPut.countDown();

                        assertOpenEventually(waitNullCaching);

                        // 3. after caching key as null inside Near Cache, check that key exist for txn
                        return map.containsKey(key);
                    }
                });

                keyExistInTxnMap.set(result);
            }
        });
        txnPutThread.start();

        assertOpenEventually(waitTxnPut);
        // 2. cache key as null into Near Cache (we know we didn't commit the above txn and key is null inside map)
        instance.getMap(mapName).get(key);

        waitNullCaching.countDown();

        assertJoinable(txnPutThread);

        assertTrue("Key should be exist in txn map but we didn't find it", keyExistInTxnMap.get());
    }

    @Test
    public void txn_set_invalidates_near_cache() {
        testTxnInvalidatesNearCache(SET);
    }

    @Test
    public void txn_put_invalidates_near_cache() {
        testTxnInvalidatesNearCache(PUT);
    }

    @Test
    public void txn_putIfAbsent_invalidates_near_cache() {
        testTxnInvalidatesNearCache(PUT_IF_ABSENT);
    }

    @Test
    public void txn_putTTL_invalidates_near_cache() {
        testTxnInvalidatesNearCache(PUT_TTL);
    }

    @Test
    public void txn_delete_invalidates_near_cache() {
        testTxnInvalidatesNearCache(DELETE);
    }

    @Test
    public void txn_replace_invalidates_near_cache() {
        testTxnInvalidatesNearCache(REPLACE);
    }

    @Test
    public void txn_replaceIfSame_invalidates_near_cache() {
        testTxnInvalidatesNearCache(REPLACE_IF_SAME);
    }

    @Test
    public void txn_containsKey_sees_latest_value_after_delete() {
        testTxnInvalidatesNearCache(CONTAINS_KEY);
    }

    private void testTxnInvalidatesNearCache(InvalidatorTxnOp invalidatorTxnTask) {
        final String mapName = "test";
        final int numberOfEntries = 1000;

        NearCacheConfig nearCacheConfig = createNearCacheConfig(mapName)
                .setInvalidateOnChange(true);

        Config config = smallInstanceConfig();
        config.getMapConfig(mapName).setNearCacheConfig(nearCacheConfig);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hz = factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);

        IMap<Integer, Integer> map = hz.getMap(mapName);

        // 1. populate Near Cache
        invalidatorTxnTask.doPopulateNearCache(map, numberOfEntries);

        // 2. run a txn task which causes invalidation
        hz.executeTransaction(invalidatorTxnTask.createTxnTask(mapName, numberOfEntries));

        // 3. assert
        invalidatorTxnTask.doAssertion(map, numberOfEntries);
    }

    private NearCacheConfig createNearCacheConfig(String mapName) {
        return new NearCacheConfig()
                .setName(mapName)
                .setCacheLocalEntries(true)
                .setInMemoryFormat(inMemoryFormat)
                .setSerializeKeys(serializeKeys);
    }

    enum InvalidatorTxnOp {

        SET {
            @Override
            TransactionalTask createTxnTask(final String mapName, final int numberOfEntries) {
                return new TransactionalTask<Integer>() {
                    @Override
                    public Integer execute(TransactionalTaskContext context) throws TransactionException {
                        TransactionalMap<Integer, Integer> map = context.getMap(mapName);
                        for (int i = 0; i < numberOfEntries; i++) {
                            map.set(i, i);
                        }
                        return null;
                    }
                };
            }

            @Override
            void doAssertion(IMap<Integer, Integer> map, int numberOfEntries) {
                for (int i = 0; i < numberOfEntries; i++) {
                    assertEquals(i, (int) map.get(i));
                }
            }
        },

        DELETE {
            @Override
            void doPopulateNearCache(IMap<Integer, Integer> map, int numberOfEntries) {
                for (int i = 0; i < numberOfEntries; i++) {
                    map.set(i, i);
                }
                for (int i = 0; i < numberOfEntries; i++) {
                    map.get(i);
                }
            }

            @Override
            TransactionalTask createTxnTask(final String mapName, final int numberOfEntries) {
                return new TransactionalTask<Integer>() {
                    @Override
                    public Integer execute(TransactionalTaskContext context) throws TransactionException {
                        TransactionalMap<Integer, Integer> map = context.getMap(mapName);
                        for (int i = 0; i < numberOfEntries; i++) {
                            map.delete(i);
                        }
                        return null;
                    }
                };
            }

            @Override
            void doAssertion(IMap<Integer, Integer> map, int numberOfEntries) {
                for (int i = 0; i < numberOfEntries; i++) {
                    assertNull(map.get(i));
                }
            }
        },

        PUT {
            @Override
            TransactionalTask createTxnTask(final String mapName, final int numberOfEntries) {
                return new TransactionalTask<Integer>() {
                    @Override
                    public Integer execute(TransactionalTaskContext context) throws TransactionException {
                        TransactionalMap<Integer, Integer> map = context.getMap(mapName);
                        for (int i = 0; i < numberOfEntries; i++) {
                            map.put(i, i);
                        }
                        return null;
                    }
                };
            }
        },

        REPLACE {
            @Override
            void doPopulateNearCache(IMap<Integer, Integer> map, int numberOfEntries) {
                for (int i = 0; i < numberOfEntries; i++) {
                    map.put(i, i);
                }
                for (int i = 0; i < numberOfEntries; i++) {
                    map.get(i);
                }
            }

            @Override
            TransactionalTask createTxnTask(final String mapName, final int numberOfEntries) {
                return new TransactionalTask<Integer>() {
                    @Override
                    public Integer execute(TransactionalTaskContext context) throws TransactionException {
                        TransactionalMap<Integer, Integer> map = context.getMap(mapName);
                        for (int i = 0; i < numberOfEntries; i++) {
                            map.replace(i, 2 * i);
                        }
                        return null;
                    }
                };
            }

            @Override
            void doAssertion(IMap<Integer, Integer> map, int numberOfEntries) {
                for (int i = 0; i < numberOfEntries; i++) {
                    assertEquals(2 * i, (int) map.get(i));
                }
            }
        },

        REPLACE_IF_SAME {
            @Override
            void doPopulateNearCache(IMap<Integer, Integer> map, int numberOfEntries) {
                for (int i = 0; i < numberOfEntries; i++) {
                    map.put(i, i);
                }
                for (int i = 0; i < numberOfEntries; i++) {
                    map.get(i);
                }
            }

            @Override
            TransactionalTask createTxnTask(final String mapName, final int numberOfEntries) {
                return new TransactionalTask<Integer>() {
                    @Override
                    public Integer execute(TransactionalTaskContext context) throws TransactionException {
                        TransactionalMap<Integer, Integer> map = context.getMap(mapName);
                        for (int i = 0; i < numberOfEntries; i++) {
                            map.replace(i, i, 2 * i);
                        }
                        return null;
                    }
                };
            }

            @Override
            void doAssertion(IMap<Integer, Integer> map, int numberOfEntries) {
                for (int i = 0; i < numberOfEntries; i++) {
                    assertEquals(2 * i, (int) map.get(i));
                }
            }
        },

        PUT_IF_ABSENT {
            @Override
            TransactionalTask createTxnTask(final String mapName, final int numberOfEntries) {
                return new TransactionalTask<Integer>() {
                    @Override
                    public Integer execute(TransactionalTaskContext context) throws TransactionException {
                        TransactionalMap<Integer, Integer> map = context.getMap(mapName);
                        for (int i = 0; i < numberOfEntries; i++) {
                            map.putIfAbsent(i, i);
                        }
                        return null;
                    }
                };
            }
        },

        PUT_TTL {
            @Override
            TransactionalTask createTxnTask(final String mapName, final int numberOfEntries) {
                return new TransactionalTask<Integer>() {
                    @Override
                    public Integer execute(TransactionalTaskContext context) throws TransactionException {
                        TransactionalMap<Integer, Integer> map = context.getMap(mapName);
                        for (int i = 0; i < numberOfEntries; i++) {
                            map.put(i, i, 100, TimeUnit.SECONDS);
                        }
                        return null;
                    }
                };
            }
        },

        CONTAINS_KEY {
            @Override
            void doPopulateNearCache(IMap<Integer, Integer> map, int numberOfEntries) {
                for (int i = 0; i < numberOfEntries; i++) {
                    map.put(i, i);
                }

                for (int i = 0; i < numberOfEntries; i++) {
                    map.get(i);
                }
            }

            @Override
            TransactionalTask createTxnTask(final String mapName, final int numberOfEntries) {
                return new TransactionalTask<Integer>() {
                    @Override
                    public Integer execute(TransactionalTaskContext context) throws TransactionException {
                        TransactionalMap<Integer, Integer> map = context.getMap(mapName);
                        for (int i = 0; i < numberOfEntries; i++) {
                            assertTrue(map.containsKey(i));
                        }
                        for (int i = 0; i < numberOfEntries; i++) {
                            map.delete(i);
                        }
                        for (int i = 0; i < numberOfEntries; i++) {
                            assertFalse(map.containsKey(i));
                        }
                        return null;
                    }
                };
            }

            @Override
            void doAssertion(IMap<Integer, Integer> map, int numberOfEntries) {
                // NOP
            }
        };

        // default populate implementation, override when needed
        void doPopulateNearCache(IMap<Integer, Integer> map, int numberOfEntries) {
            // populate Near Cache with nulls
            for (int i = 0; i < numberOfEntries; i++) {
                map.get(i);
            }
        }

        abstract TransactionalTask createTxnTask(String mapName, int numberOfEntries);

        // default assertion implementation, override when needed
        void doAssertion(IMap<Integer, Integer> map, int numberOfEntries) {
            // access map out-side txn
            for (int i = 0; i < numberOfEntries; i++) {
                assertEquals("map size=" + map.size(), i, (int) map.get(i));
            }
        }
    }
}
