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

package com.hazelcast.map.test;

import com.hazelcast.core.IMap;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class DynamicClusterTest extends BaseTest {


    @Test
    public void testMapSizeWhileRandomDeaths() throws InterruptedException {
        IMap map = getInstance(0).getMap("testMapSizeWhileRandomDeaths");
        int size = 1000;
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }

        Random rand = new Random(System.currentTimeMillis());

        for (int i = 0; i < 30; i++) {
            map = getInstance(0).getMap("testMapSizeWhileRandomDeaths");
            assertEquals(map.size(), size);
            System.out.println("trial:" + i + " instance count:" + instanceCount);
            if ((rand.nextInt(10) % 2 == 0 && instanceCount > 2) || instanceCount > 6) {
                System.out.println("remove...");
                System.out.println("remove...");
                System.out.println("remove...");
                removeInstance();
                System.out.println("removed!!!");
                System.out.println("removed!!!");
                System.out.println("removed!!!");
            } else {
                System.out.println("new instance...");
                System.out.println("new instance...");
                System.out.println("new instance...");
                newInstance();
                System.out.println("instance is up!!!");
                System.out.println("instance is up!!!");
                System.out.println("instance is up!!!");
            }
            Thread.sleep(10000);
            int realSize = getInstance(0).getCluster().getMembers().size();
            System.out.println("Instance count Real:" + realSize + " Expected:" + instanceCount);
            System.out.println("Instance count Real:" + realSize + " Expected:" + instanceCount);
            System.out.println("Instance count Real:" + realSize + " Expected:" + instanceCount);
            instanceCount = realSize;
        }


    }

    //TODO ali, recurrent
    @Test
    public void testMapSize() throws Exception {

        log("starting");
        final IMap map = getInstance(0).getMap("testMapSize");
        final int putSize = 50000;
        final CountDownLatch latch = new CountDownLatch(2);

        new Thread() {

            public void run() {
                for (int i = 0; i < putSize; i++) {
                    map.put("key" + i, "value" + i);
                }
                latch.countDown();
            }

        }.start();

        final AtomicInteger removed = new AtomicInteger();

        new Thread() {

            public void run() {
                for (int i = 0; i < putSize*2; i++) {
                    Random ran = new Random(System.currentTimeMillis());
                    Object o = map.remove("key" + ran.nextInt(putSize));
                    if (o != null) {
                        removed.incrementAndGet();
                    }
                }
                latch.countDown();
            }

        }.start();

        Thread.sleep(5000);
        log("remove instance");
        removeInstance(1);

        Thread.sleep(5000);
        log("new instance");
        newInstance();

        Thread.sleep(5000);
        log("remove instance");
        removeInstance(2);

        assertTrue(latch.await(100, TimeUnit.SECONDS));
        assertEquals(map.size(), putSize - removed.get());

        log("size: " + (putSize - removed.get()));

    }

    private void log(String s){
        for (int k=0; k<8; k++){
            System.out.println("---------------------------- "+s+" ----------------------------");
        }
    }


}
