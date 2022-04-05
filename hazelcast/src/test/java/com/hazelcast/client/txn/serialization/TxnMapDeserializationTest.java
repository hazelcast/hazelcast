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

package com.hazelcast.client.txn.serialization;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.transaction.TransactionalMap;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TxnMapDeserializationTest extends HazelcastTestSupport {

    private static final Object EXISTING_KEY = new SampleIdentified(1);
    private static final Object EXISTING_VALUE = new SampleIdentified(2);
    private static final Object NEW_KEY = new SampleIdentified(3);
    private static final Object NEW_VALUE = new SampleIdentified(4);
    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();
    private TransactionalMap<Object, Object> map;
    private TransactionContext context;

    @Before
    public void setup() {
        //Identified factory is only set to client, to make sure there is no deserialization on server.
        hazelcastFactory.newHazelcastInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getSerializationConfig().
                addDataSerializableFactory(SampleIdentified.FACTORY_ID, new DataSerializableFactory() {
                    @Override
                    public IdentifiedDataSerializable create(int typeId) {
                        return new SampleIdentified();
                    }
                });
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        client.getMap("test").put(EXISTING_KEY, EXISTING_VALUE);
        client.getMap("test").get(EXISTING_KEY);

        context = client.newTransactionContext();
        context.beginTransaction();
        map = context.getMap("test");
    }

    @After
    public void tearDown() {
        if (context != null) {
            context.commitTransaction();
        }
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testMapUpdate() {
        assertEquals(EXISTING_VALUE, map.put(EXISTING_KEY, NEW_VALUE));
    }

    @Test
    public void testMapPut() {
        assertNull(map.put(NEW_KEY, NEW_VALUE));
    }

    @Test
    public void testMapPutTwiceToSameKey() {
        assertNull(map.put(NEW_KEY, NEW_VALUE));
        assertEquals(NEW_VALUE, map.put(NEW_KEY, NEW_VALUE));
    }

    @Test
    public void testMapPutGet() {
        assertEquals(EXISTING_VALUE, map.put(EXISTING_KEY, NEW_VALUE));
        assertEquals(NEW_VALUE, map.get(EXISTING_KEY));
    }

    @Test
    public void testMapGet() {
        assertEquals(EXISTING_VALUE, map.get(EXISTING_KEY));
    }

    @Test
    public void testMapValues() {
        assertContains(map.values(), EXISTING_VALUE);
    }

    @Test
    public void testMapKeySet() {
        assertContains(map.keySet(), EXISTING_KEY);
    }

    @Test
    public void testMapContainsKey() {
        assertTrue(map.containsKey(EXISTING_KEY));
    }

    @Test
    public void testMapRemove() {
        assertEquals(EXISTING_VALUE, map.remove(EXISTING_KEY));
    }

    @Test
    public void testMapRemoveIfEqual() {
        assertTrue(map.remove(EXISTING_KEY, EXISTING_VALUE));
    }

    @Test
    public void testMapReplace() {
        assertTrue(map.replace(EXISTING_KEY, EXISTING_VALUE, NEW_VALUE));
    }

    @Test
    public void testMapReplaceAndGet() {
        assertEquals(EXISTING_VALUE, map.replace(EXISTING_KEY, NEW_VALUE));
    }

    @Test
    public void testGetForUpdate() {
        assertEquals(EXISTING_VALUE, map.getForUpdate(EXISTING_KEY));
    }

}
