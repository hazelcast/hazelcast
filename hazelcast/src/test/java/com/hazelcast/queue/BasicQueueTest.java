/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.StaticNodeFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @ali 2/12/13
 */
@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class BasicQueueTest {

    @Before
    @After
    public void cleanup() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testSimpleUsage() throws Exception {
        final int k = 8;
        final Config config = new Config();
        config.setProperty(GroupProperties.PROP_GRACEFUL_SHUTDOWN_MAX_WAIT, "0");
        final HazelcastInstance[] instances = StaticNodeFactory.newInstances(config, k);
        final IQueue[] queues = new IQueue[30];
        for (int i=0; i<queues.length; i++){
            queues[i] = instances[0].getQueue("queue_"+i);
        }
        final AtomicBoolean hasNull = new AtomicBoolean(false);
        final AtomicBoolean offerDone = new AtomicBoolean(false);
        final AtomicBoolean pollDone = new AtomicBoolean(false);

        new Thread() {
            public void run() {
                for (int i = 0; i < 10000; i++) {
                    for (IQueue queue: queues) {
                        queue.offer("value" + i);
                    }
                }
                offerDone.set(true);
            }
        }.start();

        new Thread() {
            public void run() {
                for (IQueue queue: queues) {
                    for (int i = 0; i < 10000; i++) {
                        try {
                            Object o = queue.poll(30, TimeUnit.SECONDS);
                            hasNull.compareAndSet(false, o == null);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
                pollDone.set(true);
            }
        }.start();

        Thread.sleep(3*1000);

        instances[1].getLifecycleService().shutdown();
        instances[2].getLifecycleService().shutdown();

        Thread.sleep(3*1000);
        instances[4].getLifecycleService().shutdown();

        while (!offerDone.get() || !pollDone.get()){
            Thread.sleep(1*1000);
        }

        Assert.assertTrue(!hasNull.get());
        for (IQueue queue: queues) {
            Assert.assertTrue(queue.size() == 0);
        }


    }
}
