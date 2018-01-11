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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionalTask;
import com.hazelcast.transaction.TransactionalTaskContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapTransactionLockingTest extends HazelcastTestSupport {

    String mapName;
    String key;
    String value;

    @Before
    public void setUp() throws Exception {
        mapName = randomMapName();
        key = randomString();
        value = randomString();
    }

    @Test
    public void testTxnReplace_whenReplaceIfSameFails_keyShouldRemainUnlocked() throws InterruptedException {
        HazelcastInstance hazelcastInstance = createHazelcastInstance(getConfig());
        IMap<String, Object> map = hazelcastInstance.getMap(mapName);
        map.put(key, value);

        hazelcastInstance.executeTransaction(new TransactionalTask<Object>() {
            @Override
            public Object execute(TransactionalTaskContext transactionContext) throws TransactionException {
                TransactionalMap<String, Object> transactionalMap = transactionContext.getMap(mapName);
                transactionalMap.replace(key, value + "other", value);
                return null;
            }
        });
        assertFalse("Key remains locked!", map.isLocked(key));
    }

    @Test
    public void testTxnReplace_whenReplaceIfSameFails_keyShouldRemainUnlockedDuringTransaction() throws InterruptedException {
        final HazelcastInstance hazelcastInstance = createHazelcastInstance(getConfig());
        final IMap<String, Object> map = hazelcastInstance.getMap(mapName);
        hazelcastInstance.executeTransaction(new TransactionalTask<Object>() {
            @Override
            public Object execute(TransactionalTaskContext transactionContext) throws TransactionException {
                TransactionalMap<String, Object> transactionalMap = transactionContext.getMap(mapName);
                boolean replace = transactionalMap.replace(key, value + "other", value);
                assertFalse(replace);
                assertFalse("Key remains locked!", map.isLocked(key));
                return null;
            }
        });

    }

    @Test
    public void testTxnReplace_whenReplaceIfSameFails_keyShouldRemainLocked_whenExplicitlyLocked() throws InterruptedException {
        final HazelcastInstance hazelcastInstance = createHazelcastInstance(getConfig());
        final IMap<String, Object> map = hazelcastInstance.getMap(mapName);
        hazelcastInstance.executeTransaction(new TransactionalTask<Object>() {
            @Override
            public Object execute(TransactionalTaskContext transactionContext) throws TransactionException {
                TransactionalMap<String, Object> transactionalMap = transactionContext.getMap(mapName);
                transactionalMap.getForUpdate(key);
                boolean replace = transactionalMap.replace(key, value + "other", value);
                assertFalse(replace);
                assertTrue("Key remains unlocked!", map.isLocked(key));
                return null;
            }
        });
    }

    @Test
    public void testTxnReplace_whenReplaceFails_keyShouldRemainUnlocked() throws InterruptedException {
        HazelcastInstance hazelcastInstance = createHazelcastInstance(getConfig());
        IMap<String, Object> map = hazelcastInstance.getMap(mapName);

        hazelcastInstance.executeTransaction(new TransactionalTask<Object>() {
            @Override
            public Object execute(TransactionalTaskContext transactionContext) throws TransactionException {
                TransactionalMap<String, Object> transactionalMap = transactionContext.getMap(mapName);
                transactionalMap.replace(key, value);
                return null;
            }
        });
        assertFalse("Key remains locked!", map.isLocked(key));
    }

    @Test
    public void testTxnReplace_whenReplaceFails_keyShouldRemainUnlockedDuringTransaction() throws InterruptedException {
        final HazelcastInstance hazelcastInstance = createHazelcastInstance(getConfig());
        final IMap<String, Object> map = hazelcastInstance.getMap(mapName);

        hazelcastInstance.executeTransaction(new TransactionalTask<Object>() {
            @Override
            public Object execute(TransactionalTaskContext transactionContext) throws TransactionException {
                TransactionalMap<String, Object> transactionalMap = transactionContext.getMap(mapName);
                transactionalMap.replace(key, value);
                assertFalse("Key remains locked!", map.isLocked(key));
                return null;
            }
        });
    }

    @Test
    public void testTxnReplace_whenReplaceFails_keyShouldRemainLocked_whenExplicitlyLocked() throws InterruptedException {
        final HazelcastInstance hazelcastInstance = createHazelcastInstance(getConfig());
        final IMap<String, Object> map = hazelcastInstance.getMap(mapName);

        hazelcastInstance.executeTransaction(new TransactionalTask<Object>() {
            @Override
            public Object execute(TransactionalTaskContext transactionContext) throws TransactionException {
                TransactionalMap<String, Object> transactionalMap = transactionContext.getMap(mapName);
                transactionalMap.getForUpdate(key);
                transactionalMap.replace(key, value);
                assertTrue("Key remains unlocked!", map.isLocked(key));
                return null;
            }
        });
    }

    @Test
    public void testTxnPutIfAbsent_whenPutFails_keyShouldRemainUnlocked() throws InterruptedException {
        HazelcastInstance hazelcastInstance = createHazelcastInstance(getConfig());
        IMap<String, Object> map = hazelcastInstance.getMap(mapName);
        map.put(key, value);

        hazelcastInstance.executeTransaction(new TransactionalTask<Object>() {
            @Override
            public Object execute(TransactionalTaskContext transactionContext) throws TransactionException {
                TransactionalMap<String, Object> transactionalMap = transactionContext.getMap(mapName);
                transactionalMap.putIfAbsent(key, "t");
                return null;
            }
        });
        assertFalse("Key remains locked!", map.isLocked(key));
    }

    @Test
    public void testTxnPutIfAbsent_whenPutFails_keyShouldRemainUnlockedDuringTransaction() throws InterruptedException {
        final HazelcastInstance hazelcastInstance = createHazelcastInstance(getConfig());
        final IMap<String, Object> map = hazelcastInstance.getMap(mapName);
        map.put(key, value);

        hazelcastInstance.executeTransaction(new TransactionalTask<Object>() {
            @Override
            public Object execute(TransactionalTaskContext transactionContext) throws TransactionException {
                TransactionalMap<String, Object> transactionalMap = transactionContext.getMap(mapName);
                transactionalMap.putIfAbsent(key, "t");
                assertFalse("Key remains locked!", map.isLocked(key));
                return null;
            }
        });
    }

    @Test
    public void testTxnPutIfAbsent_whenPutFails_keyShouldRemainLocked_whenExplicitlyLocked() throws InterruptedException {
        final HazelcastInstance hazelcastInstance = createHazelcastInstance(getConfig());
        final IMap<String, Object> map = hazelcastInstance.getMap(mapName);
        map.put(key, value);

        hazelcastInstance.executeTransaction(new TransactionalTask<Object>() {
            @Override
            public Object execute(TransactionalTaskContext transactionContext) throws TransactionException {
                TransactionalMap<String, Object> transactionalMap = transactionContext.getMap(mapName);
                transactionalMap.getForUpdate(key);
                transactionalMap.putIfAbsent(key, "t");
                assertTrue("Key remains unlocked!", map.isLocked(key));
                return null;
            }
        });
    }

    @Test
    public void testTxnRemoveIfSame_whenRemoveFails_keyShouldRemainUnlocked() throws InterruptedException {
        HazelcastInstance hazelcastInstance = createHazelcastInstance(getConfig());
        IMap<String, Object> map = hazelcastInstance.getMap(mapName);
        map.put(key, value);

        hazelcastInstance.executeTransaction(new TransactionalTask<Object>() {
            @Override
            public Object execute(TransactionalTaskContext transactionContext) throws TransactionException {
                TransactionalMap<String, Object> transactionalMap = transactionContext.getMap(mapName);
                transactionalMap.remove(key, value + "other");
                return null;
            }
        });
        assertFalse("Key remains locked!", map.isLocked(key));
    }


    @Test
    public void testTxnRemoveIfSame_whenRemoveFails_keyShouldRemainUnlockedDuringTransaction() throws InterruptedException {
        final HazelcastInstance hazelcastInstance = createHazelcastInstance(getConfig());
        final IMap<String, Object> map = hazelcastInstance.getMap(mapName);
        map.put(key, value);

        hazelcastInstance.executeTransaction(new TransactionalTask<Object>() {
            @Override
            public Object execute(TransactionalTaskContext transactionContext) throws TransactionException {
                TransactionalMap<String, Object> transactionalMap = transactionContext.getMap(mapName);
                transactionalMap.remove(key, "");
                assertFalse("Key remains locked!", map.isLocked(key));
                return null;
            }
        });
    }

    @Test
    public void testTxnRemoveIfSame_whenRemoveFails_keyShouldRemainLocked_whenExplicitlyLocked() throws InterruptedException {
        final HazelcastInstance hazelcastInstance = createHazelcastInstance(getConfig());
        final IMap<String, Object> map = hazelcastInstance.getMap(mapName);
        map.put(key, value);

        hazelcastInstance.executeTransaction(new TransactionalTask<Object>() {
            @Override
            public Object execute(TransactionalTaskContext transactionContext) throws TransactionException {
                TransactionalMap<String, Object> transactionalMap = transactionContext.getMap(mapName);
                transactionalMap.getForUpdate(key);
                transactionalMap.remove(key, "");
                assertTrue("Key remains unlocked!", map.isLocked(key));
                return null;
            }
        });
    }

}
