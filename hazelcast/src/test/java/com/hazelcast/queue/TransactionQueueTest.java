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

package com.hazelcast.queue;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.TransactionalQueue;
import com.hazelcast.instance.StaticNodeFactory;
import com.hazelcast.transaction.TransactionalTaskContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionalTask;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Random;

import static org.junit.Assert.*;

/**
 * @ali 3/11/13
 */
@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class TransactionQueueTest {

    @BeforeClass
    public static void init() {
//        System.setProperty("hazelcast.test.use.network","true");
    }

    @Before
    @After
    public void cleanup() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testTransactionalOfferPoll() throws Exception {
        Config config = new Config();
        final int count = 100;
        final int insCount = 4;
        final String name = "defQueue";
        final HazelcastInstance[] instances = StaticNodeFactory.newInstances(config, insCount);

        boolean b = instances[0].executeTransaction(new TransactionalTask<Boolean>() {
            public Boolean execute(TransactionalTaskContext context) throws TransactionException {
                TransactionalQueue<String> q = context.getQueue(name);
                assertTrue(q.offer("ali"));
                String s = q.poll();
                assertEquals("ali", s);
                return true;
            }
        });
        assertTrue(b);
        assertEquals(0, getQueue(instances, name).size());
    }

    private IQueue getQueue(HazelcastInstance[] instances, String name) {
        final Random rnd = new Random(System.currentTimeMillis());
        return instances[rnd.nextInt(instances.length)].getQueue(name);
    }
}
