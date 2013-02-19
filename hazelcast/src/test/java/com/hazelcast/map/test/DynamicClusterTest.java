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
import com.hazelcast.core.IQueue;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;


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
            }
            Thread.sleep(10000);
            int realSize = getInstance(0).getCluster().getMembers().size();
            System.out.println("Instance count Real:" + realSize + " Expected:" + instanceCount);
            System.out.println("Instance count Real:" + realSize + " Expected:" + instanceCount);
            System.out.println("Instance count Real:" + realSize + " Expected:" + instanceCount);

            instanceCount = realSize;
        }


    }

    @Test
    public void testMapSize() throws Exception {

        log("starting");
        final IMap map = getInstance(0).getMap("testMapSize");
        final int putSize = 80*1000;
        final int removeSize = putSize * 25;
        final AtomicInteger putCount = new AtomicInteger(putSize);
        final AtomicBoolean putException = new AtomicBoolean(false);
        final AtomicBoolean removeException = new AtomicBoolean(false);

        new Thread() {

            public void run() {
                try {
                    for (int i = 0; i < putSize; i++) {
                        map.put("key" + i, "value" + i);
                        putCount.decrementAndGet();
                    }
                }
                catch (Exception e){
                    putException.set(true);
                    log("exexex");
                    log(e.getMessage());
                }
            }

        }.start();

        final AtomicInteger removed = new AtomicInteger();
        final AtomicInteger removeCount = new AtomicInteger(removeSize);
        new Thread() {

            public void run() {
                try {
                    for (int i = 0; i < removeSize; i++) {
                        Random ran = new Random(System.currentTimeMillis());
                        Object o = map.remove("key" + ran.nextInt(putSize));
                        if (o != null) {
                            removed.incrementAndGet();
                        }
                        removeCount.decrementAndGet();
                    }
                }
                catch (Exception e){
                    removeException.set(true);
                    log("exexex");
                    log(e.getMessage());
                }
            }

        }.start();

        Thread.sleep(500);

        for (int i=0; i<3; i++){
            log("remove instance");
            removeInstance(2);
            Thread.sleep(4000);

            log("new instance");
            newInstance();
            Thread.sleep(2000);
            log("putCount: " + putCount.get() + "  removeCount: " + removeCount.get());
            Thread.sleep(2000);

        }

        while (putCount.get() != 0 || removeCount.get() != 0){
            Thread.sleep(1000);
            log("putCount: " + putCount.get() + "  removeCount: " + removeCount.get());
            assertFalse(putException.get());
            assertFalse(removeException.get());
        }

        assertEquals(putSize - removed.get(), map.size());

        log("size: " + (putSize - removed.get()));

    }


    @Test
    public void testQueueSize() throws Exception {
        final IQueue queue = getInstance(0).getQueue("testQueueSize"+rand.nextInt(100));
        final int pollSize = 150*1000;
        final int offerSize = 220*1000;

        final AtomicInteger offerCount = new AtomicInteger(offerSize);
        final AtomicInteger pollCount = new AtomicInteger(pollSize);
        final AtomicInteger polled = new AtomicInteger();
        final AtomicInteger offered = new AtomicInteger();

        new Thread(){
            public void run() {
                for (int i=0; i<offerSize; i++){
                    try {
                        if(queue.offer("item"+i)){
                            offered.incrementAndGet();
                        }
                    }
                    catch (Exception e){
                        log("exexex");
                        log(e.getMessage());
                    }
                    offerCount.decrementAndGet();
                }
            }
        }.start();

        new Thread(){
            public void run() {
                for (int i=0; i<pollSize; i++){
                    try {
                        Object o = queue.poll();
                        if (o != null){
                            polled.incrementAndGet();
                        }
                    }
                    catch (Exception e){
                        log("exexex");
                        log(e.getMessage());
                    }
                    pollCount.decrementAndGet();
                }
            }
        }.start();

        Thread.sleep(500);

        for (int i=0; i<4; i++){
            log("remove instance");
            removeInstance(1);
            Thread.sleep(2000);

            log("new instance");
            newInstance();
            Thread.sleep(1000);
            log("offerCount: " + offerCount.get() + "  pollCount: " + pollCount.get());
            Thread.sleep(1000);
        }

        while (offerCount.get() != 0 || pollCount.get() != 0){
            log("offerCount: " + offerCount.get() + "  pollCount: " + pollCount.get());
            Thread.sleep(1000);
        }
        assertEquals(offered.get()-polled.get(), queue.size());
        log("offered: " + offered.get() + "  polled: " + polled.get());

    }

    private void log(String s){
        for (int k=0; k<8; k++){
            System.out.println("---------------------------- " + s + " ----------------------------");
        }
    }


}
